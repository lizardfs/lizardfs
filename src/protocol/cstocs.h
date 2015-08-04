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

#include "common/chunk_part_type.h"
#include "protocol/MFSCommunication.h"
#include "protocol/packet.h"

namespace cstocs {

namespace getChunkBlocks {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, const ChunkPartType& chunkType) {
	serializePacket(destination, LIZ_CSTOCS_GET_CHUNK_BLOCKS, 0, chunkId, chunkVersion, chunkType);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkPartType& chunkType) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, chunkId, chunkVersion, chunkType);
}

} // namespace getChunkBlocks

namespace getChunkBlocksStatus {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, const ChunkPartType& chunkType,
		uint16_t blocks, uint8_t status) {
	serializePacket(destination, LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS, 0, chunkId, chunkVersion,
			chunkType, blocks, status);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkPartType& chunkType,
		uint16_t& blocks, uint8_t& status) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType, blocks, status);
}

} // namespace getChunkBlocksStatus

} // namespace cstocs
