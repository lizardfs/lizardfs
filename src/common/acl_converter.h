#include "config.h"

#include "common/access_control_list.h"
#include "common/posix_acl_xattr.h"

namespace aclConverter {

	/*
	 * An exception of failed xattr/AccessControlList generation
	 */
	LIZARDFS_CREATE_EXCEPTION_CLASS(AclConversionException, Exception);

	/*
	 * An exception of failure during extraction xattr to POSIX object
	 */
	LIZARDFS_CREATE_EXCEPTION_CLASS(PosixExtractionException, Exception);

	/*
	 * Get POSIX ACL object from xattr value
	 */
	PosixAclXattr extractPosixObject(const uint8_t* buffer, uint32_t bufferSize);

	/*
	 * Generate AccessControlList object from POSIX ACL xattr object
	 */
	AccessControlList posixToAclObject(const PosixAclXattr& posix);

	/*
	 * Generate xattr value from AccessControlList object
	 */
	std::vector<uint8_t> aclObjectToXattr(const AccessControlList& acl);
}
