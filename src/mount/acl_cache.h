#pragma once

#include "common/platform.h"

#include <memory>

#include "common/exception.h"
#include "mount/mastercomm.h"

LIZARDFS_CREATE_EXCEPTION_CLASS_MSG(AclAcquisitionException, Exception, "ACL acquiring");

typedef std::shared_ptr<AccessControlList> AclCacheEntry;

typedef TreeLruCacheMt<AclCacheEntry, uint32_t, uint32_t, uint32_t, AclType> AclCache;

inline AclCacheEntry getAcl(uint32_t inode, uint32_t uid, uint32_t gid, AclType type) {
	AclCacheEntry acl(new AccessControlList());
	uint8_t status = fs_getacl(inode, uid, gid, type, *acl);
	if (status == STATUS_OK) {
		return acl;
	} else if (status == ERROR_ENOATTR) {
		return AclCacheEntry();
	} else {
		throw AclAcquisitionException(status);
	}
}
