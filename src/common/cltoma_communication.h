#ifndef LIZARDFS_MFSCOMMON_CLTOMA_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_CLTOMA_COMMUNICATION_H_

#include "common/MFSCommunication.h"
#include "common/packet.h"

namespace cltoma {

namespace fuseReadChunk {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint32_t inode,
		uint32_t chunkIndex) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_READ_CHUNK, 0, messageId, inode,
			chunkIndex);
}

inline void deserialize(const std::vector<uint8_t>& source, uint32_t& messageId, uint32_t& inode,
		uint32_t& chunkIndex) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, messageId, inode, chunkIndex);
}

} // namespace fuseReadChunk

} // namespace cltoma

#endif // LIZARDFS_MFSCOMMON_CLTOMA_COMMUNICATION_H_
