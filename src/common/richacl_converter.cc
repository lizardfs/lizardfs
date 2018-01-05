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
#include "common/richacl_converter.h"
#include "common/portable_endian.h"
#include "common/datapack.h"
#ifndef _WIN32
#include <pwd.h>
#include <grp.h>
#endif

#include <bitset>

using namespace richAclConverter;

static const uint32_t kInvalidId = 0xFFFFFFFF;

struct RichAceWrapper {
	uint16_t e_type;
	uint16_t e_flags;
	uint32_t e_mask;
	uint32_t e_id;
};

struct RichACLWrapper {
	uint8_t a_version;
	uint8_t a_flags;
	uint16_t a_count;
	uint32_t a_owner_mask;
	uint32_t a_group_mask;
	uint32_t a_other_mask;
};

static uint32_t padLength(size_t length, size_t pad) {
	return (length + pad - 1) / pad * pad;
}

static uint32_t nameToUid(const std::string &name) {
#ifndef _WIN32
	struct passwd *pwd = getpwnam(name.c_str());
	if (pwd) {
		return pwd->pw_uid;
	} else {
		if (name.size() > 2 && name[0] == 'u' && name[1] == ':') {
			try {
				return std::stoull(name.substr(2));
			} catch (...) {
				return kInvalidId;
			}
		}
	}
	return kInvalidId;
#else
	if (name.size() > 2 && name[0] == 'u' && name[1] == ':') {
		try {
			return std::stoull(name.substr(2));
		} catch (...) {
		}
	}
	return kInvalidId;
#endif
}

static uint32_t nameToGid(const std::string &name) {
#ifndef _WIN32
	struct group *grp = getgrnam(name.c_str());
	if (grp) {
		return grp->gr_gid;
	} else {
		if (name.size() > 2 && name[0] == 'g' && name[1] == ':') {
			try {
				return std::stoull(name.substr(2));
			} catch (...) {
				return kInvalidId;
			}
		}
	}
	return kInvalidId;
#else
	if (name.size() > 2 && name[0] == 'g' && name[1] == ':') {
		try {
			return std::stoull(name.substr(2));
		} catch (...) {
		}
	}
	return kInvalidId;
#endif
}

static std::string idToName(const RichACL::Ace &ace) {
	if (ace.flags & RichACL::Ace::kSpecialWho) {
		switch (ace.id) {
		case RichACL::Ace::kOwnerSpecialId:
			return "OWNER@";
		case RichACL::Ace::kGroupSpecialId:
			return "GROUP@";
		case RichACL::Ace::kEveryoneSpecialId:
			return "EVERYONE@";
		default:
			throw ConversionException("Incorrect special id: " + std::to_string(ace.id));
		}
	}
#ifndef _WIN32
	if (ace.flags & RichACL::Ace::kIdentifierGroup) {
		struct group *grp = getgrgid(ace.id);
		if (grp) {
			return grp->gr_name;
		} else {
			return "g:" + std::to_string(ace.id);
		}
	} else {
		struct passwd *pwd = getpwuid(ace.id);
		if (pwd) {
			return pwd->pw_name;
		} else {
			return "u:" + std::to_string(ace.id);
		}
	}
#else
	if (ace.flags & RichACL::Ace::kIdentifierGroup) {
		return "g:" + std::to_string(ace.id);
	} else {
		return "u:" + std::to_string(ace.id);
	}
#endif
}

static RichACL::Ace extractAceFromNFS(const uint8_t *&buffer, uint32_t &bytes_left) {
	RichACL::Ace ace;
	if (bytes_left < 4 * sizeof(uint32_t)) {
		throw ExtractionException("Buffer too short for ACE header");
	}

	uint32_t type = get32bit(&buffer);
	uint32_t flag = get32bit(&buffer);
	uint32_t access_mask = get32bit(&buffer);
	uint32_t owner_length = get32bit(&buffer);
	bytes_left -= 4 * sizeof(uint32_t);

	if (bytes_left < owner_length) {
		throw ExtractionException("Buffer too short for ACE owner");
	}

	std::string owner((char *)buffer, owner_length);
	uint32_t padded_length = padLength(owner_length, sizeof(uint32_t));
	buffer += padded_length;
	bytes_left -= padded_length;

	ace.type = type;
	ace.flags = flag;
	ace.mask = access_mask;
	if (owner == "OWNER@") {
		ace.flags |= RichACL::Ace::kSpecialWho;
		ace.id = RichACL::Ace::kOwnerSpecialId;
	} else if (owner == "GROUP@") {
		ace.flags |= RichACL::Ace::kSpecialWho;
		ace.id = RichACL::Ace::kGroupSpecialId;
	} else if (owner == "EVERYONE@") {
		ace.flags |= RichACL::Ace::kSpecialWho;
		ace.id = RichACL::Ace::kEveryoneSpecialId;
	} else if (ace.flags & RichACL::Ace::kIdentifierGroup) {
		ace.id = nameToGid(owner);
	} else {
		ace.id = nameToUid(owner);
	}
	return ace;
}

RichACL richAclConverter::extractObjectFromNFS(const uint8_t *buffer, uint32_t buffer_size) {
	RichACL acl;
	if (buffer_size < sizeof(uint32_t)) {
		throw ExtractionException("Buffer too short for ACL header");
	}

	uint32_t bytes_left = buffer_size;
	uint32_t ace_count = get32bit(&buffer);
	bytes_left -= sizeof(uint32_t);

	for (uint32_t i = 0; i < ace_count; ++i) {
		RichACL::Ace ace = extractAceFromNFS(buffer, bytes_left);
		acl.insert(ace);
	}

	// Manually recompute masks used for extracting POSIX rwx mode.
	uint32_t owner_mask = acl.allowedToWho(
		RichACL::Ace(0, RichACL::Ace::kSpecialWho, 0, RichACL::Ace::kOwnerSpecialId));
	uint32_t group_mask = acl.allowedToWho(
		RichACL::Ace(0, RichACL::Ace::kSpecialWho, 0, RichACL::Ace::kGroupSpecialId));
	uint32_t other_mask = acl.allowedToWho(
		RichACL::Ace(0, RichACL::Ace::kSpecialWho, 0, RichACL::Ace::kEveryoneSpecialId));

	acl.setOwnerMask(owner_mask);
	acl.setGroupMask(group_mask);
	acl.setOtherMask(other_mask);

	return acl;
}

static RichACL::Ace extractAceFromRichACL(const uint8_t *&buffer, uint32_t &bytes_left) {
	RichACL::Ace ace;
	if (bytes_left < sizeof(RichAceWrapper)) {
		throw ExtractionException("Buffer too short for ACE header");
	}
	RichAceWrapper *wrapper = (RichAceWrapper *)buffer;

	uint16_t type = le16toh(wrapper->e_type);
	uint16_t flag = le16toh(wrapper->e_flags);
	uint32_t access_mask = le32toh(wrapper->e_mask);
	uint32_t owner_id = le32toh(wrapper->e_id);
	bytes_left -= sizeof(RichAceWrapper);
	buffer += sizeof(RichAceWrapper);

	const uint16_t incompatible_special_who = 0x4000;
	if (flag & incompatible_special_who) {
		flag &= ~incompatible_special_who;
		flag |= RichACL::Ace::kSpecialWho;
	}

	ace.type = type;
	ace.flags = flag;
	ace.mask = access_mask;
	ace.id = owner_id;
	return ace;
}

RichACL richAclConverter::extractObjectFromRichACL(const uint8_t *buffer, uint32_t buffer_size) {
	RichACL acl;
	if (buffer_size < sizeof(RichACLWrapper)) {
		throw ExtractionException("Buffer too short for ACL header");
	}
	RichACLWrapper *wrapper = (RichACLWrapper *)buffer;
	uint32_t bytes_left = buffer_size;
	uint16_t ace_count = le16toh(wrapper->a_count);
	acl.setFlags(wrapper->a_flags);
	acl.setOwnerMask(le32toh(wrapper->a_owner_mask));
	acl.setGroupMask(le32toh(wrapper->a_group_mask));
	acl.setOtherMask(le32toh(wrapper->a_other_mask));

	buffer += sizeof(RichACLWrapper);
	bytes_left -= sizeof(RichACLWrapper);

	for (uint32_t i = 0; i < ace_count; ++i) {
		RichACL::Ace ace = extractAceFromRichACL(buffer, bytes_left);
		acl.insert(ace);
	}
	return acl;
}

std::vector<uint8_t> richAclConverter::objectToRichACLXattr(const RichACL& acl) {
	std::vector<uint8_t> buffer;

	buffer.resize(sizeof(RichACLWrapper) + sizeof(RichAceWrapper) * acl.size());

	RichACLWrapper *wrapper = (RichACLWrapper *)buffer.data();

	wrapper->a_flags = acl.getFlags();
	wrapper->a_count = htole16(acl.size());
	wrapper->a_owner_mask = htole32(acl.getOwnerMask());
	wrapper->a_group_mask = htole32(acl.getGroupMask());
	wrapper->a_other_mask = htole32(acl.getOtherMask());

	RichAceWrapper *ace_wrapper = (RichAceWrapper *)(buffer.data() + sizeof(RichACLWrapper));
	for (const RichACL::Ace &ace : acl) {
		ace_wrapper->e_type = htole16(ace.type);
		ace_wrapper->e_id = htole32(ace.id);
		ace_wrapper->e_mask = htole32(ace.mask);
		uint16_t flags = ace.flags;
		const uint16_t incompatible_special_who = 0x4000;
		if (flags & RichACL::Ace::kSpecialWho) {
			flags &= ~RichACL::Ace::kSpecialWho;
			flags |= incompatible_special_who;
		}
		ace_wrapper->e_flags = htole16(flags);
		ace_wrapper++;
	}

	return buffer;
}

std::vector<uint8_t> richAclConverter::objectToNFSXattr(const RichACL& acl, uint32_t owner_id) {
	std::vector<uint8_t> buffer;
	RichACL acl_with_applied_masks = acl;
	acl_with_applied_masks.applyMasks(owner_id);

	buffer.resize(sizeof(uint32_t));
	uint8_t *current = buffer.data();
	put32bit(&current, acl_with_applied_masks.size());

	for (const RichACL::Ace &ace : acl_with_applied_masks) {
		std::string owner_name = idToName(ace);
		size_t prev_size = buffer.size();
		size_t padded_length = padLength(owner_name.size(), sizeof(uint32_t));
		buffer.resize(prev_size + 4 * sizeof(uint32_t) + padded_length);
		current = buffer.data() + prev_size;
		put32bit(&current, ace.type);
		uint16_t flags = ace.flags;
		const uint16_t incompatible_special_who = 0x4000;
		if (flags & RichACL::Ace::kSpecialWho) {
			flags &= ~RichACL::Ace::kSpecialWho;
			flags |= incompatible_special_who;
		}
		put32bit(&current, flags);
		put32bit(&current, ace.mask);
		put32bit(&current, padded_length);
		owner_name.copy((char *)current, owner_name.size());
		current += padded_length; // redundant, but makes it safe to write code below it
	}

	return buffer;
}
