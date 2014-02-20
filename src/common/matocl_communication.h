#pragma once

#include "common/chunk_type_with_address.h"
#include "common/chunks_availability_state.h"
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

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint64_t fileLength,
		uint64_t chunkId, uint32_t chunkVersion, uint32_t lockId,
		const std::vector<ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_WRITE_CHUNK, kResponsePacketVersion,
			messageId, fileLength, chunkId, chunkVersion, lockId, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion, uint32_t& lockId,
		std::vector<ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId,
			fileLength, chunkId, chunkVersion, lockId, serversList);
}

} //namespace fuseWriteChunk

namespace fuseWriteChunkEnd {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_WRITE_CHUNK_END, 0, messageId, status);
}

inline void deserialize(const std::vector<uint8_t>& source, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, dummyMessageId, status);
}

} //namespace fuseWriteChunkEnd

namespace xorChunksHealth {

inline void serialize(std::vector<uint8_t>& destination, bool regularChunksOnly,
		const ChunksAvailabilityState& availability, const ChunksReplicationState& replication) {
	serializePacket(destination, LIZ_MATOCL_CHUNKS_HEALTH, 0,
			regularChunksOnly, availability, replication);
}

inline void deserialize(const std::vector<uint8_t>& source, bool& regularChunksOnly,
		ChunksAvailabilityState& availability, ChunksReplicationState& replication) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, regularChunksOnly, availability, replication);
}

} // xorChunksHealth

} // namespace matocl
