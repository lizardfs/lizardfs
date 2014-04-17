#include "common/access_control_list.h"

#include <cstdlib>
#include <cstring>
#include <sstream>

static char accessMaskToChar(ExtendedAcl::AccessMask mask) {
	if (mask > 7) {
		return '?';
	} else {
		return '0' + mask;
	}
}

static ExtendedAcl::AccessMask accessMaskFromChar(char chr) {
	if (chr >= '0' && chr <= '7') {
		return chr - '0';
	} else {
		throw AccessControlList::IncorrectStringRepresentationException(
				std::string("wrong mask '") + chr + "'");
	}
}

// Removes given prefix from a C-string
static void eat(const char*& str, const std::string& prefix) {
	if (strncmp(str, prefix.c_str(), prefix.size()) != 0) {
		throw AccessControlList::IncorrectStringRepresentationException(
				"Expected prefix '" + prefix + "' in string '" + str + "'");
	}
	str += prefix.size();
}

AccessControlList AccessControlList::fromString(const std::string& str) {
	if (str.size() < 4) {
		throw IncorrectStringRepresentationException("ACL string '" + str + "': too short");
	}
	AccessControlList acl;
	const char* rptr = str.c_str();
	eat(rptr, "A");
	acl.mode = 0;
	for (int i = 0; i < 3; ++i) {
		acl.mode <<= 3;
		acl.mode |= accessMaskFromChar(*rptr++);
	}
	if (*rptr == '\0') {
		return acl;
	}
	eat(rptr, "/g::");
	acl.extendedAcl.reset(new ExtendedAcl(accessMaskFromChar(*rptr++)));
	while (*rptr != '\0') {
		char entryType;
		eat(rptr, "/");
		if (*rptr != '\0') {
			entryType = *rptr++; // should be 'g' or 'u' -- will be verified in a moment
		}
		eat(rptr, ":");
		char* endOfId;
		uint32_t id = strtol(rptr, &endOfId, 10);
		if (endOfId == rptr) {
			throw IncorrectStringRepresentationException("ACL string '" + str + "': unknown id");
		} else {
			rptr = endOfId;
		}
		eat(rptr, ":");
		if (entryType == 'u') {
			if (acl.extendedAcl->hasNamedUser(id)) {
				throw IncorrectStringRepresentationException(
						"ACL string '" + str + "': repeated uid");
			}
			acl.extendedAcl->addNamedUser(id, accessMaskFromChar(*rptr++));
		} else if (entryType == 'g') {
			if (acl.extendedAcl->hasNamedGroup(id)) {
				throw IncorrectStringRepresentationException(
						"ACL string '" + str + "': repeated gid");
			}
			acl.extendedAcl->addNamedGroup(id, accessMaskFromChar(*rptr++));
		} else {
			throw IncorrectStringRepresentationException(
					"ACL string '" + str + "': unknown entry type " + entryType);
		}
	}
	return acl;
}

std::string AccessControlList::toString() const {
	std::stringstream ss;
	ss << 'A';
	for (int i = 0; i < 3; ++i) {
		ss << accessMaskToChar((mode >> (6 - 3 * i)) & 0007);
	}
	if (extendedAcl) {
		ss << "/g::" << accessMaskToChar(extendedAcl->owningGroupMask());
		for (const auto& entry : extendedAcl->list()) {
			if (entry.type == ExtendedAcl::EntryType::kNamedUser) {
				ss << "/u:" << entry.id << ":" << accessMaskToChar(entry.mask);
			}
		}
		for (const auto& entry : extendedAcl->list()) {
			if (entry.type == ExtendedAcl::EntryType::kNamedGroup) {
				ss << "/g:" << entry.id << ":" << accessMaskToChar(entry.mask);
			}
		}

	}
	return ss.str();
}
