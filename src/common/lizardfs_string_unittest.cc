#include "common/platform.h"
#include "common/lizardfs_string.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"

TEST(LizardFsStringTests, Serialization8Bit) {
	std::vector<uint8_t> buffer;
	LIZARDFS_DEFINE_INOUT_PAIR(LizardFsString<uint8_t>,   string8, "", "");
	LIZARDFS_DEFINE_INOUT_PAIR(LizardFsString<uint16_t>, string16, "", "");
	LIZARDFS_DEFINE_INOUT_PAIR(LizardFsString<uint32_t>, string32, "", "");

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

TEST(LizardFsStringTests, MaxLength) {
	std::vector<uint8_t> buffer;
	uint32_t maxLength8 = LizardFsString<uint8_t>::maxLength();
	ASSERT_ANY_THROW(serialize(buffer, LizardFsString<uint8_t>(maxLength8 + 1, 'x')));
	ASSERT_TRUE(buffer.empty());
	ASSERT_NO_THROW(serialize(buffer, LizardFsString<uint8_t>(maxLength8, 'x')));
	LizardFsString<uint8_t> out;
	deserialize(buffer, out);
	EXPECT_EQ(LizardFsString<uint8_t>(maxLength8, 'x'), out);
}
