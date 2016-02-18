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
#include "common/access_control_list.h"

#include <gtest/gtest.h>

#define EXPECT_GOOD_ACL(str) \
		ASSERT_NO_THROW(AccessControlList::fromString(str)); \
		EXPECT_EQ(str, AccessControlList::fromString(str).toString())

#define EXPECT_WRONG_ACL(str) \
		EXPECT_THROW(AccessControlList::fromString(str), \
				AccessControlList::IncorrectStringRepresentationException)

TEST(AccessControlListTests, ToStringMimial) {
	EXPECT_GOOD_ACL("A000");
	EXPECT_GOOD_ACL("A700");
	EXPECT_GOOD_ACL("A740");
	EXPECT_GOOD_ACL("A777");
	EXPECT_GOOD_ACL("A660");
	EXPECT_GOOD_ACL("A222");
	EXPECT_GOOD_ACL("A234");
	EXPECT_GOOD_ACL("A567");
}

TEST(AccessControlListTests, ToStringExtended) {
	EXPECT_GOOD_ACL("A700/m::7");
	EXPECT_GOOD_ACL("A700/u:12345:5/m::7");
	EXPECT_GOOD_ACL("A755/g:12345:5/m::4");
	EXPECT_GOOD_ACL("A755/u:12345:5/g:12345:5/m::4");
}

TEST(AccessControlListTests, ToStringErrors) {
	EXPECT_WRONG_ACL(""); // malformed
	EXPECT_WRONG_ACL("A1"); // malformed
	EXPECT_WRONG_ACL("A12"); // malformed
	EXPECT_WRONG_ACL("A1234"); // malformed
	EXPECT_WRONG_ACL("123"); // malformed
	EXPECT_WRONG_ACL("Ab123"); // malformed
	EXPECT_WRONG_ACL("750/m::3"); // malformed
	EXPECT_WRONG_ACL("A800"); // wrong access mask
	EXPECT_WRONG_ACL("A700/g::4"); // named group without id
	EXPECT_WRONG_ACL("A700/m::8"); // wrong access mask
	EXPECT_WRONG_ACL("A700/m:8"); // malformed
	EXPECT_WRONG_ACL("A700/m::7:u:123:8"); // wrong access mask
	EXPECT_WRONG_ACL("A700/m::7/u:123:5/u::5"); // missing uid
	EXPECT_WRONG_ACL("A700/m::7/u:123:5/g::5"); // missing gid
	EXPECT_WRONG_ACL("A700/m::7/u:123:5/u:123:5"); // repeated entry
	EXPECT_WRONG_ACL("A700/m::7/g:123:5/g:123:5"); // repeated entry
	EXPECT_WRONG_ACL("A700/m::7/g:123:5/g:125:"); // malformed
	EXPECT_WRONG_ACL("A700/m::7/g:123:5/g:125"); // malformed
	EXPECT_WRONG_ACL("A700/m::7/g:123:5/g:"); // malformed
	EXPECT_WRONG_ACL("A700/m::7/g:123:5/g"); // malformed
	EXPECT_WRONG_ACL("A700/m::7/g:123:5/"); // malformed
	EXPECT_WRONG_ACL("A755/g/m:4/g:12345:5");
	EXPECT_WRONG_ACL("A755/m::4i/g:12345:5");
	EXPECT_WRONG_ACL("A755/m::4/g:1o2345:5");
	EXPECT_WRONG_ACL("A755/m::4/g:12345:5a");
	EXPECT_WRONG_ACL("A755g/m::4/g:12345:5");
	EXPECT_WRONG_ACL("A755/m::4/g:i12345:5");
	EXPECT_WRONG_ACL("A755/m::4/g:12345p:5");
	EXPECT_WRONG_ACL("A755:i::4/g:12345:5");
	EXPECT_WRONG_ACL("A755/m::4/m:12345:5");
	EXPECT_WRONG_ACL("A755:m::4/g:12345:5:u/12345:5");
	EXPECT_WRONG_ACL("A755/m::4:g:12345:5:u/12345:5");
	EXPECT_WRONG_ACL("A755/m::4/g:12345:5:u:12345:5");
}
