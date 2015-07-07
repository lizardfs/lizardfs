/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#pragma once

#include "common/platform.h"

#include <cstdint>
#include <vector>
#include <gtest/gtest.h>

#include "common/packet.h"

inline void removeHeaderInPlace(std::vector<uint8_t>& packet) {
	sassert(packet.size() >= PacketHeader::kSize);
	packet.erase(packet.begin(), packet.begin() + PacketHeader::kSize);
}

inline void verifyHeader(const std::vector<uint8_t>& messageWithHeader,
		PacketHeader::Type expectedType) {
	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(messageWithHeader, header));
	EXPECT_EQ(expectedType, header.type);
	EXPECT_EQ(messageWithHeader.size() - PacketHeader::kSize, header.length);
}

inline void verifyHeaderInPrefix(const std::vector<uint8_t>& messagePrefixWithHeader,
		PacketHeader::Type type, uint32_t extraDataSize) {
	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(messagePrefixWithHeader, header));
	EXPECT_EQ(type, header.type);
	EXPECT_EQ(messagePrefixWithHeader.size() + extraDataSize - PacketHeader::kSize, header.length);
}

inline void verifyVersion(const std::vector<uint8_t>& messageWithoutHeader,
		PacketVersion expectedVersion) {
	PacketVersion version = !expectedVersion;
	ASSERT_NO_THROW(deserializePacketVersionNoHeader(messageWithoutHeader, version));
	EXPECT_EQ(expectedVersion, version);
}
