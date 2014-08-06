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
