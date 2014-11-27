#include "common/platform.h"

#include <gtest/gtest.h>

#include "common/crc.h"
#include "common/lfserr.h"
#include "common/random.h"

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	rnd_init();
	mycrc32_init();
	strerr_init();
	return RUN_ALL_TESTS();
}
