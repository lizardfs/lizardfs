#ifndef LIZARDFS_MFSCOMMON_MATOCS_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_MATOCS_COMMUNICATION_H_

#include "common/chunk_type.h"
#include "common/packet.h"

namespace matocs {

namespace setVersion {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType, uint32_t newVersion) {
	serializePacket(destination, LIZ_MATOCS_SET_VERSION, 0,
			chunkId, chunkVersion, chunkType, newVersion);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType, uint32_t& newVersion) {
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType, newVersion);
}

} // namespace setVersion

namespace deleteChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType) {
	serializePacket(destination, LIZ_MATOCS_DELETE_CHUNK, 0, chunkId, chunkVersion, chunkType);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType) {
	deserializeAllPacketDataNoHeader(source, chunkId, chunkVersion, chunkType);
}

} // namespace deleteChunk

} // namespace matocs

#endif // LIZARDFS_MFSCOMMON_MATOCS_COMMUNICATION_H_
