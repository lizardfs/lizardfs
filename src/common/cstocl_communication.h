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

#pragma once

#include "common/platform.h"

#include "common/chunk_type.h"
#include "common/packet.h"
#include "common/serialization_macros.h"

namespace cstocl {

namespace readData {

inline void serializePrefix(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t readOffset, uint32_t readSize) {
	// This prefix requires CRC (uint32_t) and data (readSize * uint8_t) to be appended
	uint32_t extraSpace = serializedSize(uint32_t()) + readSize;
	serializePacketPrefix(destination, extraSpace,
			LIZ_CSTOCL_READ_DATA, 0, chunkId, readOffset, readSize);
}

inline void deserializePrefix(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& readOffset, uint32_t& readSize, uint32_t& crc) {
	verifyPacketVersionNoHeader(source, 0);
	deserializePacketDataNoHeader(source, chunkId, readOffset, readSize, crc);
}

// kPrefixSize - version:u32, chunkId:u64, readOffset:u32, readSize:u32, crc:u32
static const uint32_t kPrefixSize = 4 + 8 + 4 + 4 + 4;

} // namespace readData

namespace readStatus {

inline void serialize(std::vector<uint8_t>& destination, uint64_t chunkId, uint8_t status) {
	serializePacket(destination, LIZ_CSTOCL_READ_STATUS, 0, chunkId, status);
}

inline void deserialize(const std::vector<uint8_t>& source, uint64_t& chunkId, uint8_t& status) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, status);
}

} // namespace readStatus

namespace writeStatus {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t writeId, uint8_t status) {
	serializePacket(destination, LIZ_CSTOCL_WRITE_STATUS, 0, chunkId, writeId, status);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& writeId, uint8_t& status) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, writeId, status);
}

} // namespace writeStatus

} // namespace cstocl
