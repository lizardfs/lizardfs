/*
   Copyright 2017 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"

#ifdef __APPLE__

#include "mount/osx_acl_converter.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <membership.h>
#include <array>

using namespace osxAclConverter;

static std::array<std::pair<uint32_t, acl_perm_t>, 17> kPermLookup = {{
	{RichACL::Ace::kReadData, ACL_READ_DATA},
	{RichACL::Ace::kWriteData, ACL_WRITE_DATA},
	{RichACL::Ace::kAddFile, ACL_ADD_FILE},
	{RichACL::Ace::kAppendData, ACL_WRITE_DATA},
	{RichACL::Ace::kReadNamedAttrs, ACL_READ_ATTRIBUTES},
	{RichACL::Ace::kWriteNamedAttrs, ACL_WRITE_ATTRIBUTES},
	{RichACL::Ace::kExecute, ACL_EXECUTE},
	{RichACL::Ace::kDeleteChild, ACL_DELETE_CHILD},
	{RichACL::Ace::kReadAttributes, ACL_READ_ATTRIBUTES},
	{RichACL::Ace::kWriteAttributes, ACL_WRITE_ATTRIBUTES},
	{RichACL::Ace::kWriteRetention, ACL_WRITE_ATTRIBUTES},
	{RichACL::Ace::kWriteRetentionHold, ACL_WRITE_ATTRIBUTES},
	{RichACL::Ace::kDelete, ACL_DELETE},
	{RichACL::Ace::kReadAcl, ACL_READ_SECURITY},
	{RichACL::Ace::kWriteAcl, ACL_WRITE_SECURITY},
	{RichACL::Ace::kWriteOwner, ACL_CHANGE_OWNER},
	{RichACL::Ace::kSynchronize, ACL_SYNCHRONIZE}
}};

static uint32_t osxPermsetToRichACLMask(acl_permset_t permset) {
	uint32_t mask = 0;
	for (const auto &perm_entry : kPermLookup) {
		if (acl_get_perm_np(permset, perm_entry.second)) {
			mask |= perm_entry.first;
		}
	}

	return mask;
}

static RichACL::Ace convertOsxEntryToRichACL(acl_entry_t entry) {
	RichACL::Ace ace;
	ace.id = RichACL::Ace::kInvalidId;

	acl_tag_t tag;
	acl_permset_t permset;
	int ret;

	uuid_t *uu;
	ret = acl_get_tag_type(entry, &tag);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to get tag type from ACL entry");
		return ace;
	}
	uu = (uuid_t *)acl_get_qualifier(entry);

	if (tag == ACL_EXTENDED_ALLOW) {
		ace.type = RichACL::Ace::kAccessAllowedAceType;
	} else if (tag == ACL_EXTENDED_DENY) {
		ace.type = RichACL::Ace::kAccessDeniedAceType;
	} else {
		lzfs_pretty_syslog(LOG_WARNING, "Only allow and deny entries are supported");
		goto free_acl_entry;
	}

	id_t uid_or_gid;
	int id_type;
	ret = mbr_uuid_to_id(*uu, &uid_or_gid, &id_type);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to translate uuid to id");
		goto free_acl_entry;
	}

	ret = acl_get_permset(entry, &permset);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to get permset from ACL entry");
		goto free_acl_entry;
	}
	ace.mask = osxPermsetToRichACLMask(permset);

	if (id_type == ID_TYPE_GID) {
		ace.flags |= RichACL::Ace::kIdentifierGroup;
	} else if (id_type != ID_TYPE_UID) {
		lzfs_pretty_syslog(LOG_WARNING, "Unsupported id type for RichACL: %u", id_type);
		goto free_acl_entry;
	}

	ace.id = uid_or_gid;

free_acl_entry:
	acl_free(uu);
	return ace;
}

static RichACL convertOsxToRichACL(acl_t osx_acl) {
	RichACL out;

	int ret = 0;
	int entry_id = ACL_FIRST_ENTRY;

	while (ret == 0) {
		acl_entry_t entry;
		ret = acl_get_entry(osx_acl, entry_id, &entry);
		if (ret != 0) {
			break;
		}
		RichACL::Ace ace = convertOsxEntryToRichACL(entry);
		if (ace.id != RichACL::Ace::kInvalidId) {
			out.insert(ace);
		}
		entry_id = ACL_NEXT_ENTRY;
	}

	out.setFlags(out.getFlags() | RichACL::kAutoSetMode);
	return out;
}

RichACL osxAclConverter::extractAclObject(const void *data, size_t /*size*/) {
	RichACL out;
	acl_t acl = acl_copy_int(data);
	ssize_t osx_acl_size = acl_size(acl);
	if (acl) {
		out = convertOsxToRichACL(acl);
	}
	acl_free(acl);
	if (osx_acl_size > 0 && out.size() == 0) {
		throw AclConversionException("Extracting RichACL from OSX xattr failed");
	}
	return out;
}

static int createOsxEntry(acl_t osx_acl, const RichACL::Ace &ace) {
	static const char kNfsOwner[] = "OWNER@";
	static const char kNfsGroup[] = "GROUP@";
	static const char kNfsEveryone[] = "EVERYONE@";

	acl_entry_t osx_entry;
	acl_permset_t permset;
	uuid_t uu;
	int ret = 0;

	ret = acl_create_entry(&osx_acl, &osx_entry);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to create ACL entry");
		return ret;
	}
	ret = acl_get_permset(osx_entry, &permset);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to get ACL permset");
		acl_delete_entry(osx_acl, osx_entry);
		return ret;
	}
	for (const auto &perm_entry : kPermLookup) {
		if (ace.mask & perm_entry.first) {
			ret = acl_add_perm(permset, perm_entry.second);
			if (ret < 0) {
				lzfs_pretty_syslog(LOG_WARNING, "Failed to add permission to permset");
				acl_delete_entry(osx_acl, osx_entry);
				return ret;
			}
		}
	}
	acl_tag_t tag = ace.isAllow() ? ACL_EXTENDED_ALLOW : ACL_EXTENDED_DENY;
	ret = acl_set_tag_type(osx_entry, tag);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to set tag type for ACL");
		acl_delete_entry(osx_acl, osx_entry);
		return ret;
	}
	if (ace.isOwner()) {
		ret = mbr_identifier_to_uuid(ID_TYPE_USER_NFS, kNfsOwner, sizeof(kNfsOwner), uu);
	} else if (ace.isGroup()) {
		ret = mbr_identifier_to_uuid(ID_TYPE_GROUP_NFS, kNfsGroup, sizeof(kNfsGroup), uu);
	} else if (ace.isEveryone()) {
		ret = mbr_identifier_to_uuid(ID_TYPE_GROUP_NFS, kNfsEveryone, sizeof(kNfsEveryone), uu);
	} else if (ace.isUnixGroup()) {
		ret = mbr_gid_to_uuid(ace.id, uu);
	} else if (ace.isUnixUser()) {
		ret = mbr_uid_to_uuid(ace.id, uu);
	} else {
		ret = -1;
	}

	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to translate uid/gid to uuid");
		acl_delete_entry(osx_acl, osx_entry);
		return ret;
	}
	ret = acl_set_qualifier(osx_entry, (void *)uu);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to set ACL qualifier");
		acl_delete_entry(osx_acl, osx_entry);
		return ret;
	}
	acl_flagset_t flagset;
	ret = acl_get_flagset_np(osx_entry, &flagset);
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to get ACL flagset");
		acl_delete_entry(osx_acl, osx_entry);
		return ret;
	}
	if (ace.flags & RichACL::Ace::kInheritOnlyAce) {
		ret = acl_add_flag_np(flagset, ACL_ENTRY_ONLY_INHERIT);
	}
	if (ret == 0 && ace.flags & RichACL::Ace::kFileInheritAce) {
		ret = acl_add_flag_np(flagset, ACL_ENTRY_FILE_INHERIT);
	}
	if (ret == 0 && ace.flags & RichACL::Ace::kDirectoryInheritAce) {
		ret = acl_add_flag_np(flagset, ACL_ENTRY_DIRECTORY_INHERIT);
	}
	if (ret == 0 && ace.flags & RichACL::Ace::kInheritedAce) {
		ret = acl_add_flag_np(flagset, ACL_ENTRY_INHERITED);
	}
	if (ret == 0 && ace.flags & RichACL::Ace::kNoPropagateInheritAce) {
		ret = acl_add_flag_np(flagset, ACL_FLAG_DEFER_INHERIT);
	}
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to add flags to ACL flagset");
		acl_delete_entry(osx_acl, osx_entry);
		return ret;
	}
	return ret;
}

static acl_t convertRichACLToOsx(const RichACL &acl) {
	acl_t osx_acl = acl_init(acl.size());

	int ret = 0;
	for (const RichACL::Ace &ace : acl) {
		ret = createOsxEntry(osx_acl, ace);
		if (ret < 0) {
			lzfs_pretty_syslog(LOG_ERR, "Failed to create OSX entry");
			acl_free(osx_acl);
			return nullptr;
		}
	}
	return osx_acl;
}

std::vector<uint8_t> osxAclConverter::objectToOsxXattr(const RichACL &acl) {
	int ret;
	std::vector<uint8_t> out;
	acl_t osx_acl = convertRichACLToOsx(acl);
	if (osx_acl == nullptr) {
		return std::vector<uint8_t>();
	}
	ssize_t size = acl_size(osx_acl);
	if (size <= 0) {
		acl_free(osx_acl);
		return std::vector<uint8_t>();
	}
	out.resize(size);
	ret = acl_copy_ext(out.data(), osx_acl, out.size());
	if (ret < 0) {
		lzfs_pretty_syslog(LOG_WARNING, "Failed to serialize ACL to OSX xattr");
		acl_free(osx_acl);
		throw AclConversionException("Serializing RichACL to OSX xattr failed");
	}
	acl_free(osx_acl);
	return out;
}

#endif // __APPLE__
