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

#include "common/platform.h"
#include "common/posix_acl_xattr.h"

#include <gtest/gtest.h>

TEST(PosixAclXattrTests, WriteReadFailures) {
	PosixAclXattr xattrValueIn, xattrValueOut;
	xattrValueIn.version = POSIX_ACL_XATTR_VERSION;
	xattrValueIn.entries.push_back({ACL_USER, ACL_WRITE, 0});
	xattrValueIn.entries.push_back({ACL_GROUP, ACL_READ, 0});
	xattrValueIn.entries.push_back({ACL_OTHER, ACL_EXECUTE, 0});
	xattrValueIn.entries.push_back({ACL_MASK, ACL_WRITE | ACL_READ, 0});
	xattrValueIn.entries.push_back({ACL_USER_OBJ, ACL_WRITE | ACL_EXECUTE, 1001});
	xattrValueIn.entries.push_back({ACL_GROUP_OBJ, ACL_READ | ACL_EXECUTE, 1010});

	ASSERT_EQ(
			sizeof(xattrValueIn.version) + xattrValueIn.entries.size() * sizeof(PosixAclXattrEntry),
			xattrValueIn.rawSize());
	std::vector<uint8_t> buffer(xattrValueIn.rawSize());
	size_t writtenSize;
	ASSERT_NO_THROW(writtenSize = xattrValueIn.write(buffer.data()));
	ASSERT_EQ(xattrValueIn.rawSize(), writtenSize);
	ASSERT_NO_THROW(xattrValueOut.read(buffer.data(), buffer.size()));
	ASSERT_EQ(xattrValueIn, xattrValueOut);

	// Failed buffers
	xattrValueOut.reset();
	ASSERT_ANY_THROW(xattrValueOut.read(buffer.data(), buffer.size() - 1));
	buffer.pop_back();
	xattrValueOut.reset();
	ASSERT_ANY_THROW(xattrValueOut.read(buffer.data(), buffer.size()));
	buffer.push_back(5);
	buffer.push_back(5);
	xattrValueOut.reset();
	ASSERT_ANY_THROW(xattrValueOut.read(buffer.data(), buffer.size()));
}
