/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
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
