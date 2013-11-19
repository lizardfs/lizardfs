#include "mount/block_xor.h"

#include <gtest/gtest.h>

TEST(BlockXorTests, BlockXor) {
	std::vector<uint8_t> v1(7000);
	std::vector<uint8_t> v2(v1.size());

	for (size_t i = 0; i < 32; ++i) {
		for (size_t j = 0; j < 32; ++j) {
			ASSERT_NO_THROW(blockXor(v1.data() + i, v2.data() + i, 6000));
			ASSERT_NO_THROW(blockXor(v1.data() + i, v2.data() + i, 32));
			ASSERT_NO_THROW(blockXor(v1.data() + i, v2.data() + i, 5));
		}
	}
}
