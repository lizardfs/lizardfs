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

namespace fuseWriteChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint32_t inode, uint32_t chunkIndex) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_WRITE_CHUNK, 0, messageId, inode, chunkIndex);
}

inline void deserialize(const std::vector<uint8_t>& source, uint32_t& messageId, uint32_t& inode,
		uint32_t& chunkIndex) {
	deserializeAllPacketDataNoHeader(source, messageId, inode, chunkIndex);
}

} // namespace fuseWriteChunk

namespace fuseWriteChunkEnd {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint64_t chunkId,
		uint32_t inode, uint64_t fileLength) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_WRITE_CHUNK_END, 0,
			messageId, chunkId, inode, fileLength);
}

inline void deserialize(const std::vector<uint8_t>& source, uint32_t& messageId, uint64_t& chunkId,
		uint32_t& inode, uint64_t& fileLength) {
	deserializeAllPacketDataNoHeader(source, messageId, chunkId, inode, fileLength);
}

} // namespace fuseWriteChunkEnd

} // namespace cltoma

#endif // LIZARDFS_MFSCOMMON_CLTOMA_COMMUNICATION_H_
