#pragma once

#include <memory>

#include "common/exception.h"
#include "common/extended_acl.h"
#include "common/serialization_macros.h"

class AccessControlList {
public:
	LIZARDFS_CREATE_EXCEPTION_CLASS(IncorrectStringRepresentationException, Exception);

	/*
	 * Default constructor just to make life (eg. deserialization,
	 * using std::map<foo, AccessControlList>) easier.
	 * Creates an uninitialized object which can be deserialized or assigned to.
	 */
	AccessControlList() {}

	/*
	 * Constructs a minimal ACL
	 */
	explicit AccessControlList(uint16_t mode) : mode(mode) {}

	AccessControlList(const AccessControlList& acl) {
		*this = acl;
	}

	/*
	 * Move constructor restoration
	 */
	AccessControlList(AccessControlList&&) = default;

	AccessControlList& operator=(const AccessControlList& acl) {
		mode = acl.mode;
		if (acl.extendedAcl) {
			extendedAcl.reset(new ExtendedAcl(*acl.extendedAcl));
		}
		return *this;
	}

	/*
	 * Move assignment operator restoration
	 */
	AccessControlList& operator=(AccessControlList&&) = default;

	/*
	 * ACL <-> human-readable-string conversions
	 *
	 * The format is eg:
	 * A760 -- minimal ACL - just 'A' + mode in octal
	 * A770/g::6 -- extended acl with only the owning group's mask rw- (6 in octal)
	 * A770/g::6/u:123:7 -- the same as before, but with uid 123 having rwx rights
	 * A770/g::6/u:123:7/g:166:4 -- the same as before, but with gid 166 having r-- rights
	 *
	 */
	std::string toString() const;
	static AccessControlList fromString(const std::string& string);

	LIZARDFS_DEFINE_SERIALIZE_METHODS(
			AccessControlList,
			uint16_t, mode,
			std::unique_ptr<ExtendedAcl>, extendedAcl);

	/*
	 * If acl.extendedAcl is null, then acl represents a minimal ACL
	 * If acl.extendedAcl is not null, then acl represents an extended ACL
	 */
	std::unique_ptr<ExtendedAcl> extendedAcl;

	/*
	 * Semantics of mode are POSIX-like, ie:
	 * - mode = [user::rwx group::rwx other::rwx] for minimal ACL
	 * - mode = [user::rwx mask::rwx other::rwx] for extended ACL
	 */
	uint16_t mode;
};
