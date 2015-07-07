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
#include "common/block_xor.h"

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
