/*
   Copyright 2016 Skytechnology sp. z o.o.

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

#include <gtest/gtest.h>

#include "common/ring_buffer.h"

TEST(RingBuffer, Basic) {
	RingBuffer<int, 7> ring;
	ASSERT_TRUE(ring.empty());
	for (int i = 0; i < 7; ++i) {
		ASSERT_TRUE(!ring.full());
		ring.push_back(i);
		ASSERT_EQ(ring.back(), i);
		ASSERT_EQ(ring.size(), (unsigned)i + 1);
	}
	ASSERT_TRUE(ring.full());
	for (int i = 0; i < 7; ++i) {
		int v = ring.front();
		ring.pop_front();
		ASSERT_EQ(v, i);
		ASSERT_TRUE(!ring.full());
	}
	ASSERT_TRUE(ring.empty());
}
