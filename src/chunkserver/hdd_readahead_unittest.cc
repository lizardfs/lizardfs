#include "chunkserver/hdd_readahead.h"

#include <gtest/gtest.h>

void testHDDReadAhead(uint32_t actual_kB, uint16_t expected_blocks) {
	SCOPED_TRACE("Testing actual_kB = " + std::to_string(actual_kB));
	{
		HDDReadAhead d;
		d.setMaxReadBehind_kB(actual_kB);
		EXPECT_EQ(expected_blocks, d.maxBlocksToBeReadBehind());
	}
	{
		HDDReadAhead d;
		d.setReadAhead_kB(actual_kB);
		EXPECT_EQ(expected_blocks, d.blocksToBeReadAhead());
	}
}

TEST(HDDReadAheadTests, HDDReadAhead) {
	testHDDReadAhead(0, 0);
	testHDDReadAhead(MFSBLOCKSIZE / 1024 - 1, 0);
	testHDDReadAhead(MFSBLOCKSIZE / 1024, 1);
	testHDDReadAhead(2*(MFSBLOCKSIZE / 1024) - 1, 1);
	testHDDReadAhead(2*(MFSBLOCKSIZE / 1024), 2);
	testHDDReadAhead(17*(MFSBLOCKSIZE / 1024), 17);
}
