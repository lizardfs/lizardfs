#include "common/string_8bit.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"

TEST(MooseFsStringTests, Serialization) {
	std::vector<uint8_t> buffer;
	LIZARDFS_DEFINE_INOUT_PAIR(String8Bit, string, "1234567/90poiuh aj89/';[p/1821", "");
	ASSERT_NO_THROW(serialize(buffer, stringIn));
	EXPECT_EQ(stringIn.length() + 1, buffer.size());
	ASSERT_NO_THROW(deserialize(buffer, stringOut));
	LIZARDFS_VERIFY_INOUT_PAIR(string);
}

TEST(MooseFsStringTests, MaxLength) {
	std::vector<uint8_t> buffer;
	ASSERT_ANY_THROW(serialize(buffer, String8Bit(String8Bit::kMaxLength + 2, 'x')));
	ASSERT_TRUE(buffer.empty());
	ASSERT_NO_THROW(serialize(buffer, String8Bit(String8Bit::kMaxLength, 'x')));
	String8Bit out;
	deserialize(buffer, out);
	EXPECT_EQ(String8Bit(String8Bit::kMaxLength, 'x'), out);
}
