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
#include "mount/io_limit_group.h"

#include <sstream>
#include <gtest/gtest.h>

TEST(IoLimitGroupTests, Empty) {
	std::stringstream input("");
	EXPECT_THROW(getIoLimitGroupId(input, "blkio"), GetIoLimitGroupIdException);
}

TEST(IoLimitGroupTests, NoMatching) {
	std::stringstream input("123:foo,bar:/test\n234:baz:/rmrfHOME\n");
	EXPECT_THROW(getIoLimitGroupId(input, "blkio"), GetIoLimitGroupIdException);
}

TEST(IoLimitGroupTests, SubsystemSuffix) {
	std::stringstream input("123:blkioo:/test");
	EXPECT_THROW(getIoLimitGroupId(input, "blkio"), GetIoLimitGroupIdException);
}

TEST(IoLimitGroupTests, SubsystemPrefix) {
	std::stringstream input("123:bblkio:/test");
	EXPECT_THROW(getIoLimitGroupId(input, "blkio"), GetIoLimitGroupIdException);
}

TEST(IoLimitGroupTests, Minimal) {
	std::stringstream input(":blkio:/test\n");
	EXPECT_EQ(getIoLimitGroupId(input, "blkio"), "/test");
}

TEST(IoLimitGroupTests, Commas) {
	std::stringstream input("123:cpuset,blkio,memory:/test\n");
	EXPECT_EQ(getIoLimitGroupId(input, "blkio"), "/test");
}

TEST(IoLimitGroupTests, SecondLine) {
	std::stringstream input("1:blah:/wrong\n:blkio:/test\n");
	EXPECT_EQ(getIoLimitGroupId(input, "blkio"), "/test");
}
