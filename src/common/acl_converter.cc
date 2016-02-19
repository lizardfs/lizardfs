/*
   Copyright 2016 Skytechnology sp. z o.o.

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
#include "common/acl_converter.h"
#include "common/portable_endian.h"

#include <bitset>

#define ACL_UNDEFINED_ID    (-1U)

/* e_tag entry in struct posix_acl_entry */
#define ACL_USER_OBJ        (0x01)
#define ACL_USER            (0x02)
#define ACL_GROUP_OBJ       (0x04)
#define ACL_GROUP           (0x08)
#define ACL_MASK            (0x10)
#define ACL_OTHER           (0x20)

/* permissions in the e_perm field */
#define ACL_READ            (0x04)
#define ACL_WRITE           (0x02)
#define ACL_EXECUTE         (0x01)

#define POSIX_ACL_XATTR_VERSION 0x0002

static uint16_t convertTag(uint16_t tag) {
	switch(tag) {
		case ACL_USER_OBJ : return AccessControlList::kUser;
		case ACL_USER : return AccessControlList::kNamedUser;
		case ACL_GROUP_OBJ : return AccessControlList::kGroup;
		case ACL_GROUP : return AccessControlList::kNamedGroup;
		case ACL_MASK : return AccessControlList::kMask;
		case ACL_OTHER : return AccessControlList::kOther;
	}
	return AccessControlList::kInvalid;
}

static AccessControlList::Entry extractEntry(const uint8_t* buffer, uint32_t buffer_size) {
	if (buffer_size < 8) {
		return AccessControlList::Entry(AccessControlList::kInvalid, 0, 0);
	}

	AccessControlList::Entry entry;

	entry.type = convertTag(le16toh(*(const uint16_t*)buffer));
	entry.access_rights = le16toh(*(const uint16_t*)(buffer + 2));
	entry.id = le32toh(*(const uint32_t*)(buffer + 4));

	if ((entry.access_rights & ~(ACL_READ | ACL_WRITE | ACL_EXECUTE)) != 0) {
		throw aclConverter::AclConversionException("Invalid permissions mask");
	}

	if (entry.type >= AccessControlList::kUser && entry.type < AccessControlList::kInvalid &&
	    entry.id != ACL_UNDEFINED_ID) {
		throw aclConverter::AclConversionException("Entry with invalid ID");
	}

	return entry;
}

static void storeEntry(std::vector<uint8_t> &buffer, const AccessControlList::Entry &entry) {
	if (entry.type == AccessControlList::kInvalid) {
		return;
	}

	uint16_t tag[] = {ACL_USER, ACL_GROUP, ACL_USER_OBJ, ACL_GROUP_OBJ, ACL_OTHER, ACL_MASK};

	buffer.resize(buffer.size() + 8);
	uint8_t *buf = buffer.data() + (buffer.size() - 8);

	*(uint16_t*)buf = htole16(tag[entry.type]);
	*(uint16_t*)(buf + 2) = htole16(entry.access_rights);
	*(uint32_t*)(buf + 4) = htole32(entry.id);
}


AccessControlList aclConverter::extractAclObject(const uint8_t* buffer, uint32_t buffer_size) {
	if (buffer_size<4) {
		throw AclConversionException("Incorrect POSIX ACL xattr version");
	}

	uint32_t version = le32toh(*(uint32_t*)buffer);
	if (version != POSIX_ACL_XATTR_VERSION) {
		throw AclConversionException("Incorrect POSIX ACL xattr version: " +
				std::to_string(version));
	}

	AccessControlList acl;
	std::bitset<16> available_tags;

	for (uint32_t scan_position = 4; scan_position < buffer_size; scan_position += 8) {
		auto entry = extractEntry(buffer + scan_position, buffer_size - scan_position);
		if (entry.type == AccessControlList::kInvalid) {
			throw aclConverter::AclConversionException("Invalid entry");
		}
		acl.setEntry(entry.type, entry.id, entry.access_rights);

		if (entry.type >= AccessControlList::kUser && available_tags[entry.type]) {
			throw aclConverter::AclConversionException("Tag occurred more than once");
		}
		available_tags.set(entry.type);
	}

	if (!available_tags[AccessControlList::kUser] ||
	   !available_tags[AccessControlList::kGroup] ||
	   !available_tags[AccessControlList::kOther]) {
		throw AclConversionException("ACL xattr without all minimal ACL entries");
	}

	return acl;
}

std::vector<uint8_t> aclConverter::aclObjectToXattr(const AccessControlList& acl) {
	std::vector<uint8_t> buffer;

	buffer.reserve(4 + 8 * (4 + std::distance(acl.begin(), acl.end())));

	buffer.resize(4);
	*(uint32_t*)buffer.data() = htole32(POSIX_ACL_XATTR_VERSION);

	storeEntry(buffer, acl.getEntry(AccessControlList::kUser, 0));
	for (const auto &entry : acl) {
		if (entry.type != AccessControlList::kNamedUser) {
			continue;
		}
		storeEntry(buffer, entry);
	}

	storeEntry(buffer, acl.getEntry(AccessControlList::kGroup, 0));
	for (const auto &entry : acl) {
		if (entry.type != AccessControlList::kNamedGroup) {
			continue;
		}
		storeEntry(buffer, entry);
	}

	storeEntry(buffer, acl.getEntry(AccessControlList::kMask, 0));
	storeEntry(buffer, acl.getEntry(AccessControlList::kOther, 0));

	return buffer;
}
