#include "common/serializable_range.h"

#include <cstdint>
#include <vector>
#include <gtest/gtest.h>

TEST(SerializableRangeTests, MakeSerializableRange) {
	typedef std::vector<uint16_t> Vector;
	Vector numbers = {1, 2, 3, 4};
	std::vector<uint8_t> actualBuffer, expectedBuffer;

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{1, 2, 3, 4});
	serialize(actualBuffer, makeSerializableRange(numbers));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{1, 2, 3, 4});
	serialize(actualBuffer, makeSerializableRange(numbers.begin(), numbers.end()));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{1, 2});
	serialize(actualBuffer, makeSerializableRange(numbers.begin(), numbers.begin() + 2));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{2});
	serialize(actualBuffer, makeSerializableRange(numbers.begin() + 1, numbers.begin() + 2));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{3, 4});
	serialize(actualBuffer, makeSerializableRange(numbers.begin() + 2, numbers.end()));
	EXPECT_EQ(expectedBuffer, actualBuffer);

	expectedBuffer = actualBuffer = {};
	serialize(expectedBuffer, Vector{});
	serialize(actualBuffer, makeSerializableRange(numbers.end(), numbers.end()));
	EXPECT_EQ(expectedBuffer, actualBuffer);
}
