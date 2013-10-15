#ifndef LIZARDFS_MFSCOMMON_CLTOCS_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_CLTOCS_COMMUNICATION_H_

#include "mfscommon/chunk_type.h"
#include "mfscommon/packet.h"

namespace cltocs {

namespace read {

inline void serialize(std::vector<uint8_t>& buffer,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
		uint32_t readOffset, uint32_t readSize) {
	serializePacket(buffer, LIZ_CLTOCS_READ, 0,
			chunkId, chunkVersion, chunkType, readOffset, readSize);
}

inline void deserialize(const uint8_t* buffer, uint32_t bufferSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType,
		uint32_t& readOffset, uint32_t& readSize) {
	deserializeAllPacketDataNoHeader(buffer, bufferSize,
			chunkId, chunkVersion, chunkType, readOffset, readSize);
}

} // namespace read

} // namespace cltocs

#endif // LIZARDFS_MFSCOMMON_CLTOCS_COMMUNICATION_H_
