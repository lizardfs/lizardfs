#ifndef LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_

#include "common/chunk_type_with_address.h"
#include "common/MFSCommunication.h"
#include "common/packet.h"

namespace matocl {

namespace fuseReadChunk {

const PacketVersion kStatusPacketVersion = 0;
const PacketVersion kResponsePacketVersion = 1;

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK, kStatusPacketVersion,
			messageId, status);
}

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
		const std::vector<ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK, kResponsePacketVersion,
			messageId, fileLength, chunkId, chunkVersion, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion,
		std::vector<ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId,
			fileLength, chunkId, chunkVersion, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kStatusPacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId, status);
}

} // namespace fuseReadChunk

namespace fuseWriteChunk {

const PacketVersion kStatusPacketVersion = 0;
const PacketVersion kResponsePacketVersion = 1;

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_WRITE_CHUNK, kStatusPacketVersion,
			messageId, status);
}

inline void deserialize(const std::vector<uint8_t>& source, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kStatusPacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId, status);
}

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId,
		uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
		const std::vector<ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_WRITE_CHUNK, kResponsePacketVersion,
			messageId, fileLength, chunkId, chunkVersion, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion,
		std::vector<ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId,
			fileLength, chunkId, chunkVersion, serversList);
}

} //namespace fuseWriteChunk

namespace fuseWriteChunkEnd {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, MATOCL_FUSE_WRITE_CHUNK_END, 0, messageId, status);
}

inline void deserialize(const std::vector<uint8_t>& source, uint8_t& status) {
	uint32_t dummyMessageId;
	deserializeAllPacketDataNoHeader(source, dummyMessageId, status);
}

} //namespace fuseWriteChunkEnd

} // namespace matocl

#endif // LIZARDFS_MFSCOMMON_MATOCL_COMMUNICATION_H_
