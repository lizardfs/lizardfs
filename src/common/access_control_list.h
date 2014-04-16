#pragma once

#include <memory>

#include "common/extended_acl.h"
#include "common/serializable_class.h"

struct AccessControlList {
public:
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

	LIZARDFS_DEFINE_SERIALIZE_METHODS(
			AccessControlList,
			uint16_t, mode,
			std::unique_ptr<ExtendedAcl>, extendedAcl);
};
