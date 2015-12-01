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

#include <iostream>

#include "common/chunk_part_type.h"
#include "common/chunk_with_version.h"
#include "common/chunk_with_version_and_type.h"
#include "common/serialization_macros.h"
#include "protocol/chunks_with_type.h"
#include "protocol/packet.h"

namespace cstoma {
inline void overwriteStatusField(std::vector<uint8_t>& destination, uint8_t status) {
	// 9 - sizeof chunkId + chunkType, 1 - sizeof status
	uint32_t statusOffset = PacketHeader::kSize + serializedSize(PacketVersion()) + 9;
	sassert(destination.size() >= statusOffset + 1);
	destination[PacketHeader::kSize + sizeof(PacketVersion) + 9] = status;
}
} // namespace cstoma

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, chunkNew, LIZ_CSTOMA_CHUNK_NEW, 0,
		std::vector<ChunkWithVersionAndType>, chunks)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerHost, LIZ_CSTOMA_REGISTER_HOST, 0,
		uint32_t, ip,
		uint16_t, port,
		uint32_t, timeout,
		uint32_t, csVersion)

LIZARDFS_DEFINE_PACKET_VERSION(cstoma, registerChunks, kStandardAndXorChunks, 0)
LIZARDFS_DEFINE_PACKET_VERSION(cstoma, registerChunks, kStandardChunksOnly, 1)
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerChunks, LIZ_CSTOMA_REGISTER_CHUNKS, 0,
		std::vector<ChunkWithVersionAndType>, chunks)
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerChunks, LIZ_CSTOMA_REGISTER_CHUNKS, 1,
		std::vector<ChunkWithVersion>, chunks)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerSpace, LIZ_CSTOMA_REGISTER_SPACE, 0,
		uint64_t, usedSpace,
		uint64_t, totalSpace,
		uint32_t, chunkCount,
		uint64_t, tdUsedSpace,
		uint64_t, toDeleteTotalSpace,
		uint32_t, toDeleteChunksNumber)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerLabel, LIZ_CSTOMA_REGISTER_LABEL, 0,
		std::string, label)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, setVersion, LIZ_CSTOMA_SET_VERSION, 0,
		uint64_t,  chunkId,
		ChunkPartType, chunkType,
		uint8_t,   status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, deleteChunk, LIZ_CSTOMA_DELETE_CHUNK, 0,
		uint64_t,  chunkId,
		ChunkPartType, chunkType,
		uint8_t,   status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, createChunk, LIZ_CSTOMA_CREATE_CHUNK, 0,
		uint64_t,  chunkId,
		ChunkPartType, chunkType,
		uint8_t,   status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, truncate, LIZ_CSTOMA_TRUNCATE, 0,
		uint64_t,  chunkId,
		ChunkPartType, chunkType,
		uint8_t,   status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, duplicateChunk, LIZ_CSTOMA_DUPLICATE_CHUNK, 0,
		uint64_t,  chunkId,
		ChunkPartType, chunkType,
		uint8_t,   status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, duptruncChunk, LIZ_CSTOMA_DUPTRUNC_CHUNK, 0,
		uint64_t,  chunkId,
		ChunkPartType, chunkType,
		uint8_t,   status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, replicateChunk, LIZ_CSTOMA_REPLICATE_CHUNK, 0,
		uint64_t,  chunkId,
		ChunkPartType, chunkType,
		uint8_t,   status, // status has to be third field to make overwriteStatusField work!!!
		uint32_t,  chunkVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, chunkDamaged, LIZ_CSTOMA_CHUNK_DAMAGED, 0,
		std::vector<ChunkWithType>, chunks)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, chunkLost, LIZ_CSTOMA_CHUNK_LOST, 0,
		std::vector<ChunkWithType>, chunks)
