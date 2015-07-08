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
#include "common/moosefs_string.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"

TEST(MooseFsStringTests, Serialization8Bit) {
	std::vector<uint8_t> buffer;
	LIZARDFS_DEFINE_INOUT_PAIR(MooseFsString<uint8_t>,   string8, "", "");
	LIZARDFS_DEFINE_INOUT_PAIR(MooseFsString<uint16_t>, string16, "", "");
	LIZARDFS_DEFINE_INOUT_PAIR(MooseFsString<uint32_t>, string32, "", "");

	std::stringstream ss;
	for (int i = 0; i < 1000; ++i) {
		ss << "1=-09;'{}[]\\|/.,<>?!@#$%^&*()qwertsdfag d1426asdfghjklmn~!@#$%^&*() 63245634345347";
	}

	string8In = ss.str().substr(0, 200);
	buffer.clear();
	ASSERT_NO_THROW(serialize(buffer, string8In));
	EXPECT_EQ(string8In.length() + 1, buffer.size());
	ASSERT_NO_THROW(deserialize(buffer, string8Out));
	LIZARDFS_VERIFY_INOUT_PAIR(string8);

	string16In = ss.str().substr(0, 20000);
	buffer.clear();
	ASSERT_NO_THROW(serialize(buffer, string16In));
	EXPECT_EQ(string16In.length() + 2, buffer.size());
	ASSERT_NO_THROW(deserialize(buffer, string16Out));
	LIZARDFS_VERIFY_INOUT_PAIR(string16);

	string32In = ss.str().substr(0, 70000);
	buffer.clear();
	ASSERT_NO_THROW(serialize(buffer, string32In));
	EXPECT_EQ(string32In.length() + 4, buffer.size());
	ASSERT_NO_THROW(deserialize(buffer, string32Out));
	LIZARDFS_VERIFY_INOUT_PAIR(string32);
}

TEST(MooseFsStringTests, MaxLength) {
	std::vector<uint8_t> buffer;
	uint32_t maxLength8 = MooseFsString<uint8_t>::maxLength();
	ASSERT_ANY_THROW(serialize(buffer, MooseFsString<uint8_t>(maxLength8 + 1, 'x')));
	ASSERT_TRUE(buffer.empty());
	ASSERT_NO_THROW(serialize(buffer, MooseFsString<uint8_t>(maxLength8, 'x')));
	MooseFsString<uint8_t> out;
	deserialize(buffer, out);
	EXPECT_EQ(MooseFsString<uint8_t>(maxLength8, 'x'), out);
}
