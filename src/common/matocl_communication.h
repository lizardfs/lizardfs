#pragma once

#include "common/access_control_list.h"
#include "common/attributes.h"
#include "common/chunk_type_with_address.h"
#include "common/chunks_availability_state.h"
#include "common/MFSCommunication.h"
#include "common/packet.h"
#include "common/serialization_macros.h"
#include "common/string_8bit.h"

// LIZ_MATOCL_FUSE_MKNOD
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMknod, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMknod, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMknod, LIZ_MATOCL_FUSE_MKNOD, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMknod, LIZ_MATOCL_FUSE_MKNOD, kResponsePacketVersion,
		uint32_t, messageId,
		uint32_t, inode,
		Attributes, attributes)

// LIZ_MATOCL_FUSE_MKDIR
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMkdir, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMkdir, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMkdir, LIZ_MATOCL_FUSE_MKDIR, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMkdir, LIZ_MATOCL_FUSE_MKDIR, kResponsePacketVersion,
		uint32_t, messageId,
		uint32_t, inode,
		Attributes, attributes)

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

namespace fuseDeleteAcl {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_DELETE_ACL, 0, messageId, status);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, status);
}

} // fuseDeleteAcl

namespace fuseGetAcl {

const PacketVersion kStatusPacketVersion = 0;
const PacketVersion kResponsePacketVersion = 1;

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_GET_ACL, kStatusPacketVersion, messageId, status);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, kStatusPacketVersion);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, status);
}

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, const AccessControlList& acl) {
	serializePacket(destination, LIZ_MATOCL_FUSE_GET_ACL, kResponsePacketVersion, messageId, acl);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, AccessControlList& acl) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, kResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, acl);
}

} // fuseGetAcl

namespace fuseSetAcl {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_SET_ACL, 0, messageId, status);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, status);
}

} // fuseSetAcl

namespace iolimits_config {

inline void serialize(std::vector<uint8_t>& destination, const std::string& subsystem,
		const std::vector<std::string>& groups, uint32_t renewFrequency_us) {
	serializePacket(destination, LIZ_MATOCL_IOLIMITS_CONFIG, 0, subsystem, groups,
			renewFrequency_us);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, std::string& subsystem,
		std::vector<std::string>& groups, uint32_t& renewFrequency_us) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, subsystem, groups, renewFrequency_us);
}

} // namespace iolimits_config

namespace iolimit {

inline void serialize(std::vector<uint8_t>& destination, const std::string& group,
		uint64_t limit_Bps) {
	serializePacket(destination, LIZ_MATOCL_IOLIMIT, 0, group, limit_Bps);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, std::string& group,
		uint64_t& limit_Bps) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, group, limit_Bps);
}

} // namespace iolimit

} // namespace matocl
