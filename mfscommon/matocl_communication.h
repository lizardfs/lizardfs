#ifndef LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_

#include "mfscommon/chunk_type_with_address.h"
#include "mfscommon/MFSCommunication.h"
#include "mfscommon/packet.h"

namespace matocl {

namespace fuseReadChunk {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK, 0, messageId, status);
}

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint64_t fileLength,
		uint64_t chunkId, uint32_t chunkVersion,
		const std::vector<ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK, 1, messageId, fileLength, chunkId,
			chunkVersion, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source, uint8_t& status) {
	uint32_t dummyMessageId;
	deserializePacketDataSkipHeader(source, dummyMessageId, status);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion,
		std::vector<ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	deserializePacketDataSkipHeader(source, dummyMessageId, fileLength, chunkId, chunkVersion,
			serversList);
}

} // namespace fuseReadChunk

} // namespace matocl

#endif // LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_
