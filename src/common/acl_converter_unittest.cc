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
#include "common/acl_converter.h"

#include <gtest/gtest.h>

/* xattr data of following minimal ACL:
	user::rwx
	group::rw-
	other::r--
*/
static const std::vector<uint8_t> kMinimalXattr {
		2,0,0,0,
		1,0,7,0,255,255,255,255,
		4,0,6,0,255,255,255,255,
		32,0,4,0,255,255,255,255
};

/* xattr data of following extended ACL:
	user::rw-
	user:<id=1000>:rwx
	user:<id=1002>:r--
	group::r-x
	group:<id=27>:-w-
	group:<id=124>:rw-
	mask::rwx
	other::r--
*/
static const std::vector<uint8_t> kExtendedXattr {
		2,0,0,0,
		1,0,6,0,255,255,255,255,
		2,0,7,0,232,3,0,0,
		2,0,4,0,234,3,0,0,
		4,0,5,0,255,255,255,255,
		8,0,2,0,27,0,0,0,
		8,0,6,0,124,0,0,0,
		16,0,7,0,255,255,255,255,
		32,0,4,0,255,255,255,255
};

/* FIXME
static bool hasEntry(const std::vector<ExtendedAcl::Entry>& list, ExtendedAcl::EntryType type,
		uint16_t id, ExtendedAcl::AccessMask expectedMask) {
	for (const ExtendedAcl::Entry& entry : list) {
		if (entry.type == type && entry.id == id) {
			if (entry.mask == expectedMask) {
				return true;
			} else {
				return false;
			}
		}
	}
	return false;
}

TEST(AclConverterTests, MinimalAcl) {
	// xattr -> acl
	PosixAclXattr posix;
	ASSERT_NO_THROW(posix =
			aclConverter::extractPosixObject(kMinimalXattr.data(), kMinimalXattr.size()));
	AccessControlList acl;
	ASSERT_NO_THROW(acl = aclConverter::posixToAclObject(posix));
	EXPECT_EQ(0764, acl.mode);
	EXPECT_TRUE(!acl.extendedAcl);

	// acl -> xattr
	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(buffer = aclConverter::aclObjectToXattr(acl));
	EXPECT_EQ(kMinimalXattr, buffer);
}

TEST(AclConverterTests, ExtendedAcl) {
	// xattr -> acl
	PosixAclXattr posix;
	ASSERT_NO_THROW(posix =
			aclConverter::extractPosixObject(kExtendedXattr.data(), kExtendedXattr.size()));
	AccessControlList acl;
	ASSERT_NO_THROW(acl = aclConverter::posixToAclObject(posix));
	EXPECT_EQ(0674, acl.mode);

	const ExtendedAcl* eacl = acl.extendedAcl.get();
	ASSERT_TRUE(eacl);
	EXPECT_EQ(uint8_t(5), eacl->owningGroupMask());
	EXPECT_EQ(4U, eacl->list().size());
	EXPECT_TRUE(hasEntry(eacl->list(), ExtendedAcl::EntryType::kNamedUser, 1000, 7));
	EXPECT_TRUE(hasEntry(eacl->list(), ExtendedAcl::EntryType::kNamedUser, 1002, 4));
	EXPECT_TRUE(hasEntry(eacl->list(), ExtendedAcl::EntryType::kNamedGroup, 27, 2));
	EXPECT_TRUE(hasEntry(eacl->list(), ExtendedAcl::EntryType::kNamedGroup, 124, 6));

	// acl -> xattr
	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(buffer = aclConverter::aclObjectToXattr(acl));
	EXPECT_EQ(kExtendedXattr, buffer);
}

static void checkError(std::vector<uint8_t>& buffer, bool checkPosix) {
	PosixAclXattr posix;
	if (checkPosix) {
		ASSERT_ANY_THROW(posix = aclConverter::extractPosixObject(buffer.data(), buffer.size()));
	} else {
		ASSERT_NO_THROW(posix = aclConverter::extractPosixObject(buffer.data(), buffer.size()));
	}
	AccessControlList acl;
	ASSERT_ANY_THROW(acl = aclConverter::posixToAclObject(posix));
	buffer = kExtendedXattr;
}

TEST(AclConverterTests, FailedAcl) {
	std::vector<uint8_t> buffer = kExtendedXattr;

	buffer[1] = 2; // failed POSIX ACL xattr version
	checkError(buffer, false);
	buffer[5] = 1; // failed tag
	checkError(buffer, false);
	buffer[9] = 1; // non-UNDEFINED_ID
	checkError(buffer, false);
	buffer[14] = 9; // failed permissions
	checkError(buffer, false);
	buffer.insert(buffer.begin() + 2, 1); // A byte offset
	checkError(buffer, true);
}
*/
