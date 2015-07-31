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
#include "common/network_address.h"
#include "protocol/packet.h"
#include "common/serialization_macros.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltocs, prefetch, LIZ_CLTOCS_PREFETCH, 0,
		uint64_t, chunkId, uint32_t, chunkVersion, ChunkType, chunkType,
		uint32_t, readOffset, uint32_t, readSize)

namespace cltocs {

namespace read {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
		uint32_t readOffset, uint32_t readSize) {
	serializePacket(destination, LIZ_CLTOCS_READ, 0,
			chunkId, chunkVersion, chunkType, readOffset, readSize);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType,
		uint32_t& readOffset, uint32_t& readSize) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize,
			chunkId, chunkVersion, chunkType, readOffset, readSize);
}

} // namespace read

namespace writeInit {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
		const std::vector<NetworkAddress>& chain) {
	serializePacket(destination, LIZ_CLTOCS_WRITE_INIT, 0,
			chunkId, chunkVersion, chunkType, chain);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType,
		std::vector<NetworkAddress>& chain) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize,
			chunkId, chunkVersion, chunkType, chain);
}

} // namespace writeInit

namespace writeData {

inline void serializePrefix(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t writeId,
		uint16_t blockNumber, uint32_t offset, uint32_t size, uint32_t crc) {
	serializePacketPrefix(destination, size, LIZ_CLTOCS_WRITE_DATA, 0,
			chunkId, writeId, blockNumber, offset, size, crc);
}

inline void deserializePrefix(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& writeId,
		uint16_t& blockNumber, uint32_t& offset, uint32_t& size, uint32_t& crc) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializePacketDataNoHeader(source, sourceSize,
			chunkId, writeId, blockNumber, offset, size, crc);
}

// kPrefixSize is equal to:
// version:u32 chunkId:u64 writeId:32 block:16 offset:32 size:32 crc:32
static const uint32_t kPrefixSize = 4 + 8 + 4 + 2 + 4 + 4 + 4;

} // namespace writeData

namespace writeEnd {

inline void serialize(std::vector<uint8_t>& destination, uint64_t chunkId) {
	serializePacket(destination, LIZ_CLTOCS_WRITE_END, 0, chunkId);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint64_t& chunkId) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, chunkId);
}

} // namespace writeEnd

namespace testChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType) {
	serializePacket(destination, LIZ_CLTOCS_TEST_CHUNK, 0, chunkId, chunkVersion, chunkType);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, chunkId, chunkVersion, chunkType);
}

} // namespace testChunk

} // namespace cltocs
