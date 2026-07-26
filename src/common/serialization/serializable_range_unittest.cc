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
#include "common/serializable_range.h"

#include <cstdint>
#include <vector>
#include <gtest/gtest.h>

TEST(SerializableRangeTests, MakeSerializableRange) {
	std::string a = "lubie placuszki";
	std::string b = "ala ma kota";
	std::string c = "";
	std::string d = "feniks fs";
	typedef std::vector<std::string> Vector;
	Vector numbers = {a, b, c, d};
	std::vector<uint8_t> actualBuffer, expectedBuffer;

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{a, b, c, d});
	serialize(actualBuffer, makeSerializableRange(numbers));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{a, b, c, d});
	serialize(actualBuffer, makeSerializableRange(numbers.begin(), numbers.end()));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{a, b});
	serialize(actualBuffer, makeSerializableRange(numbers.begin(), numbers.begin() + 2));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{b});
	serialize(actualBuffer, makeSerializableRange(numbers.begin() + 1, numbers.begin() + 2));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{c, d});
	serialize(actualBuffer, makeSerializableRange(numbers.begin() + 2, numbers.end()));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{});
	serialize(actualBuffer, makeSerializableRange(numbers.end(), numbers.end()));
	EXPECT_EQ(expectedBuffer, actualBuffer);
}
