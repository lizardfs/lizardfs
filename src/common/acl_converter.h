#include "common/access_control_list.h"

namespace aclConverter {

	/*
	 * An exception of failed xattr/AccessControlList generation
	 */
	LIZARDFS_CREATE_EXCEPTION_CLASS(ConversionErrorException, Exception);

	/*
	 * Generate AccessControlList object from xattr value
	 */
	AccessControlList xattrToAclObject(const uint8_t* buffer, uint32_t bufferSize);

	/*
	 * Generate xattr value from AccessControlList object
	 */
	std::vector<uint8_t> aclObjectToXattr(const AccessControlList& acl);
}
