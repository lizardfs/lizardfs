#include "config.h"
#include "common/lizardfs_version.h"

#include <gtest/gtest.h>

TEST(LizardFsVersionTests, LizardFsVersion) {
	EXPECT_EQ(0x01061BU, lizardfsVersion(1, 6, 27));
	EXPECT_EQ(0x01061CU, lizardfsVersion(1, 6, 28));
	EXPECT_EQ(0x01071BU, lizardfsVersion(1, 7, 27));
	EXPECT_EQ(0x02061BU, lizardfsVersion(2, 6, 27));
}
