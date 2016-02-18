/*
   Copyright 2013-2014 EditShare, 2013-2016 Skytechnology sp. z o.o.

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
#include "common/access_control_list.h"

#include <cstdlib>
#include <cstring>
#include <sstream>

static char accessMaskToChar(AccessControlList::AccessMask mask) {
	if (mask > 7) {
		return '?';
	} else {
		return '0' + mask;
	}
}

static AccessControlList::AccessMask accessMaskFromChar(char chr) {
	if (chr >= '0' && chr <= '7') {
		return chr - '0';
	} else {
		throw AccessControlList::IncorrectStringRepresentationException(
		    std::string("wrong mask '") + chr + "'");
	}
}

static uint8_t entryTypeFromChar(char chr) {
	switch (chr) {
	case 'u': return AccessControlList::kNamedUser;
	case 'g': return AccessControlList::kNamedGroup;
	case 'm': return AccessControlList::kMask;
	}

	return AccessControlList::kInvalid;
}

// Removes given prefix from a C-string
static void eat(const char *&str, const std::string &prefix) {
	if (strncmp(str, prefix.c_str(), prefix.size()) != 0) {
		throw AccessControlList::IncorrectStringRepresentationException(
		    "Expected prefix '" + prefix + "' in string '" + str + "'");
	}
	str += prefix.size();
}

AccessControlList AccessControlList::fromString(const std::string &str) {
	if (str.size() < 4) {
		throw IncorrectStringRepresentationException("ACL string '" + str + "': too short");
	}
	AccessControlList acl;
	const char *rptr = str.c_str();
	eat(rptr, "A");

	for (auto type : {kUser, kGroup, kOther}) {
		AccessMask mask = accessMaskFromChar(*rptr++);
		acl.setEntry(type, 0, mask);
	}
	if (*rptr == '\0') {
		return acl;
	}
	while (*rptr != '\0') {
		uint8_t entry_type = kInvalid;

		eat(rptr, "/");
		if (*rptr != '\0') {
			entry_type = entryTypeFromChar(*rptr++);  // should be 'g' or 'u' -- will be verified in a moment
		}
		eat(rptr, ":");
		uint32_t id = 0xFFFFFFFFU;
		if (*rptr != ':') {
			char *end_of_id;
			id = strtol(rptr, &end_of_id, 10);
			if (end_of_id == rptr) {
				throw IncorrectStringRepresentationException("ACL string '" + str +
				                                             "': unknown id");
			} else {
				rptr = end_of_id;
			}
		}
		eat(rptr, ":");
		AccessMask mask = accessMaskFromChar(*rptr++);

		if (entry_type == kInvalid) {
			throw IncorrectStringRepresentationException("ACL string '" + str +
			                                             "': unknown entry type");
		}
		if (acl.getEntry(entry_type, id).type != kInvalid) {
			throw IncorrectStringRepresentationException("ACL string '" + str +
			                                             "': repeated entry");
		}
		if (id == 0xFFFFFFFFU && (entry_type == kNamedUser || entry_type == kNamedGroup)) {
			throw IncorrectStringRepresentationException("ACL string '" + str +
			                                             "': missing id for entry");
		}

		acl.setEntry(entry_type, id, mask);
	}
	return acl;
}

std::string AccessControlList::toString() const {
	std::stringstream ss;
	ss << 'A';
	for (auto type : {kUser, kGroup, kOther}) {
		ss << accessMaskToChar(getEntry(type, 0).access_rights);
	}

	auto mask = getEntry(kMask, 0);
	for (const auto &entry : list_) {
		if (entry.type == kNamedUser) {
			ss << "/u:" << entry.id << ":" << accessMaskToChar(entry.access_rights);
		}
		if (entry.type == kNamedGroup) {
			ss << "/g:" << entry.id << ":" << accessMaskToChar(entry.access_rights);
		}
	}
	if (mask.type == kMask) {
		ss << "/m::" << accessMaskToChar(mask.access_rights);
	}
	return ss.str();
}
