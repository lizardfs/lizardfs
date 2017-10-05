/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#pragma once

#include "common/platform.h"

#include "common/access_control_list.h"

namespace aclConverter {

	/*
	 * An exception of failed xattr/AccessControlList generation
	 */
	LIZARDFS_CREATE_EXCEPTION_CLASS(AclConversionException, Exception);

	/*
	 * An exception of failure during extraction xattr to ACL object
	 */
	LIZARDFS_CREATE_EXCEPTION_CLASS(PosixExtractionException, Exception);

	/*
	 * Get POSIX ACL object from xattr value
	 */
	AccessControlList extractAclObject(const uint8_t* buffer, uint32_t bufferSize);

	/*
	 * Generate xattr value from AccessControlList object
	 */
	std::vector<uint8_t> aclObjectToXattr(const AccessControlList& acl);
}
