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
