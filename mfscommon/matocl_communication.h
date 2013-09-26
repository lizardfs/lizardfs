#ifndef LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_

#include "mfscommon/chunkserver_holding_part_of_chunk.h"
#include "mfscommon/MFSCommunication.h"
#include "mfscommon/packet.h"

namespace matocl {

namespace fuseReadChunkData {
void serialize(std::vector<uint8_t>& destination, uint64_t chunkId, uint32_t chunkVersion,
		uint64_t fileLength, std::vector<ChunkserverHoldingPartOfChunk>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK_DATA, 0, chunkId, chunkVersion,
			fileLength, serversList);
}
void deserialize(const std::vector<uint8_t>& source, uint64_t& chunkId, uint32_t& chunkVersion,
		uint64_t& fileLength, std::vector<ChunkserverHoldingPartOfChunk>& serversList) {
	deserializePacketDataNoHeader(source, chunkId, chunkVersion, fileLength, serversList);
}
} // namespace fuseReadChunkData

namespace fuseReadChunkStatus {
void serialize(std::vector<uint8_t>& destination, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK_STATUS, 0, status);
}
void deserialize(const std::vector<uint8_t>& source, uint8_t& status) {
	deserializePacketDataNoHeader(source, status);
}
} // namespace fuseReadChunkStatus

} // namespace matocl

#endif // LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_
