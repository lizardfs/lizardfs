#include "config.h"
#include "common/network_address.h"

#include <gtest/gtest.h>

TEST(NetworkAddressTests, ToString) {
	EXPECT_EQ("10.0.255.16:9425", NetworkAddress(0x0A00FF10, 9425).toString());
}
