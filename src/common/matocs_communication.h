#pragma once

#include "common/chunk_type_with_address.h"
#include "common/packet.h"

namespace matocs {

namespace setVersion {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, ChunkType chunkType, uint32_t chunkVersion, uint32_t newVersion) {
	serializePacket(destination, LIZ_MATOCS_SET_VERSION, 0,
			chunkId, chunkVersion, chunkType, newVersion);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, ChunkType& chunkType, uint32_t& chunkVersion, uint32_t& newVersion) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType, newVersion);
}

} // namespace setVersion

namespace deleteChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, ChunkType chunkType, uint32_t chunkVersion) {
	serializePacket(destination, LIZ_MATOCS_DELETE_CHUNK, 0, chunkId, chunkVersion, chunkType);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, ChunkType& chunkType, uint32_t& chunkVersion) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType);
}

} // namespace deleteChunk

namespace createChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, ChunkType chunkType, uint32_t chunkVersion) {
	serializePacket(destination, LIZ_MATOCS_CREATE_CHUNK, 0, chunkId, chunkType, chunkVersion);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, ChunkType& chunkType, uint32_t& chunkVersion) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkType, chunkVersion);
}

} // namespace createChunk

namespace truncateChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, ChunkType chunkType, uint32_t length, uint32_t newVersion,
		uint32_t oldVersion) {
	serializePacket(destination, LIZ_MATOCS_TRUNCATE, 0, chunkId, chunkType, length, newVersion,
			oldVersion);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, ChunkType& chunkType, uint32_t& length, uint32_t& newVersion,
		uint32_t& oldVersion) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkType, length, newVersion, oldVersion);
}

} // namespace truncateChunk

namespace replicate {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
		const std::vector<ChunkTypeWithAddress> sources) {
	serializePacket(destination, LIZ_MATOCS_REPLICATE, 0, chunkId, chunkVersion, chunkType,
			sources);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType,
		std::vector<ChunkTypeWithAddress>& sources) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType, sources);
}

inline void deserializePartial(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType, const uint8_t*& sources) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType, sources);
}

} // namespace replicate

} // namespace matocs
