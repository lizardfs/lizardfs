#include <gtest/gtest.h>

#include "crc.h"
#include "strerr.h"

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	mycrc32_init();
	strerr_init();
	return RUN_ALL_TESTS();
}
