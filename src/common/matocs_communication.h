#pragma once

#include "config.h"

#include "common/chunk_type_with_address.h"
#include "common/packet.h"
#include "common/serialization_macros.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocs, setVersion, LIZ_MATOCS_SET_VERSION, 0,
		uint64_t,  chunkId,
		ChunkType, chunkType,
		uint32_t,  chunkVersion,
		uint32_t,  newVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocs, deleteChunk, LIZ_MATOCS_DELETE_CHUNK, 0,
		uint64_t,  chunkId,
		ChunkType, chunkType,
		uint32_t,  chunkVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocs, createChunk, LIZ_MATOCS_CREATE_CHUNK, 0,
		uint64_t,  chunkId,
		ChunkType, chunkType,
		uint32_t,  chunkVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocs, truncateChunk, LIZ_MATOCS_TRUNCATE, 0,
		uint64_t,  chunkId,
		ChunkType, chunkType,
		uint32_t,  length, // if xor chunk - length of chunk part
		uint32_t,  newVersion,
		uint32_t,  oldVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocs, duplicateChunk, LIZ_MATOCS_DUPLICATE_CHUNK, 0,
		uint64_t, newChunkId,
		uint32_t, newchunkVersion,
		ChunkType, chunkType,
		uint64_t, oldChunkId,
		uint32_t, oldChunkVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocs, duptruncChunk, LIZ_MATOCS_DUPTRUNC_CHUNK, 0,
		uint64_t, newChunkId,
		uint32_t, newchunkVersion,
		ChunkType, chunkType,
		uint64_t, oldChunkId,
		uint32_t, oldChunkVersion,
		uint32_t, length) // if xor chunk - length of chunk part

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocs, replicateChunk, LIZ_MATOCS_REPLICATE_CHUNK, 0,
		uint64_t,  chunkId,
		uint32_t,  chunkVersion,
		ChunkType, chunkType,
		std::vector<ChunkTypeWithAddress>, sources)

namespace matocs {
namespace replicateChunk {
inline void deserializePartial(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType, const uint8_t*& sources) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType, sources);
}
} // namespace replicate
} // namespace matocs
