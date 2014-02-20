#pragma once

#include "common/MFSCommunication.h"
#include "common/packet.h"

namespace cltoma {

namespace fuseReadChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint32_t inode, uint32_t chunkIndex) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_READ_CHUNK, 0, messageId, inode,
			chunkIndex);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, messageId, inode, chunkIndex);
}

} // namespace fuseReadChunk

namespace fuseWriteChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint32_t inode, uint32_t chunkIndex, uint32_t lockId) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_WRITE_CHUNK, 0,
			messageId, inode, chunkIndex, lockId);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex, uint32_t& lockId) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, messageId, inode, chunkIndex, lockId);
}

} // namespace fuseWriteChunk

namespace fuseWriteChunkEnd {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint64_t chunkId, uint32_t lockId,
		uint32_t inode, uint64_t fileLength) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_WRITE_CHUNK_END, 0,
			messageId, chunkId, lockId, inode, fileLength);
}


inline void deserialize(const std::vector<uint8_t>& source,
		uint32_t& messageId, uint64_t& chunkId, uint32_t& lockId,
		uint32_t& inode, uint64_t& fileLength) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, messageId, chunkId, lockId, inode, fileLength);
}

} // namespace fuseWriteChunkEnd

namespace xorChunksHealth {

inline void serialize(std::vector<uint8_t>& destination, bool regularChunksOnly) {
	serializePacket(destination, LIZ_CLTOMA_CHUNKS_HEALTH, 0, regularChunksOnly);
}

inline void deserialize(const std::vector<uint8_t>& source, bool& regularChunksOnly) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, regularChunksOnly);
}

} // xorChunksHealth

} // namespace cltoma
