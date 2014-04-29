#pragma once

#include "config.h"

#include "common/chunk_type.h"
#include "common/MFSCommunication.h"
#include "common/packet.h"

namespace cstocs {

namespace getChunkBlocks {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, const ChunkType& chunkType) {
	serializePacket(destination, LIZ_CSTOCS_GET_CHUNK_BLOCKS, 0, chunkId, chunkVersion, chunkType);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, chunkId, chunkVersion, chunkType);
}

} // namespace getChunkBlocks

namespace getChunkBlocksStatus {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, const ChunkType& chunkType,
		uint16_t blocks, uint8_t status) {
	serializePacket(destination, LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS, 0, chunkId, chunkVersion,
			chunkType, blocks, status);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType,
		uint16_t& blocks, uint8_t& status) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType, blocks, status);
}

} // namespace getChunkBlocksStatus

} // namespace cstocs
