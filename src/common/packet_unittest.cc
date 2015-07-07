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
#include "common/packet.h"

#include <gtest/gtest.h>

TEST(PacketTests, PacketHeaderSize) {
	// workaround: ASSERT_EQ(x, y) requires &x and &y to be valid expressions,
	// which is not true for constants defined in header files
	uint32_t packetHeaderSize = PacketHeader::kSize;
	PacketHeader header(1000, 1);
	ASSERT_EQ(serializedSize(header), packetHeaderSize) <<
			"The constant PacketHeader::kSize defined in hacket.h has wrong value";
}
