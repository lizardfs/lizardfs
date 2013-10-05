#include <gtest/gtest.h>

#include "mfscommon/crc.h"
#include "mfscommon/strerr.h"

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	mycrc32_init();
	strerr_init();
	return RUN_ALL_TESTS();
}
