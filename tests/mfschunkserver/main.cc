#include <gtest/gtest.h>

#include "common/crc.h"
#include "common/strerr.h"

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	mycrc32_init();
	strerr_init();
	return RUN_ALL_TESTS();
}
