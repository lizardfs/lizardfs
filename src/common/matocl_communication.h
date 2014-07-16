#pragma once

#include "common/platform.h"

#include "common/access_control_list.h"
#include "common/attributes.h"
#include "common/chunk_type_with_address.h"
#include "common/chunks_availability_state.h"
#include "common/io_limits_database.h"
#include "common/MFSCommunication.h"
#include "common/moosefs_string.h"
#include "common/packet.h"
#include "common/quota.h"
#include "common/serialization_macros.h"

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

// LIZ_MATOCL_FUSE_DELETE_ACL
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseDeleteAcl, LIZ_MATOCL_FUSE_DELETE_ACL, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_FUSE_GET_ACL
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetAcl, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetAcl, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kResponsePacketVersion,
		uint32_t, messageId,
		AccessControlList, acl)

// LIZ_MATOCL_FUSE_SET_ACL
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseSetAcl, LIZ_MATOCL_FUSE_SET_ACL, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_IOLIMITS_CONFIG
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, iolimitsConfig, LIZ_MATOCL_IOLIMITS_CONFIG, 0,
		uint32_t, configVersion,
		uint32_t, period_us,
		std::string, subsystem,
		std::vector<std::string>, groups)

// LIZ_MATOCL_IOLIMIT
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, iolimit, LIZ_MATOCL_IOLIMIT, 0,
		uint32_t, msgid,
		uint32_t, configVersion,
		std::string, group,
		uint64_t, grantedBytes)

// LIZ_MATOCL_FUSE_SET_QUOTA
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseSetQuota, LIZ_MATOCL_FUSE_SET_QUOTA, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_FUSE_DELETE_QUOTA
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseDeleteQuota, LIZ_MATOCL_FUSE_DELETE_QUOTA, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_FUSE_GET_QUOTA
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetQuota, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetQuota, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetQuota, LIZ_MATOCL_FUSE_GET_QUOTA, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetQuota, LIZ_MATOCL_FUSE_GET_QUOTA, kResponsePacketVersion,
		uint32_t, messageId,
		std::vector<QuotaOwnerAndLimits>, ownersAndLimits)

// LIZ_MATOCL_IOLIMITS_STATUS
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, iolimitsStatus, LIZ_MATOCL_IOLIMITS_STATUS, 0,
		uint32_t, messageId,
		uint32_t, configId,
		uint32_t, period_us,
		uint32_t, accumulate_ms,
		std::string, subsystem,
		std::vector<IoGroupAndLimit>, groupsAndLimits)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, metadataserverStatus, LIZ_MATOCL_METADATASERVER_STATUS, 0,
		uint32_t, messageId,
		uint8_t, status,
		uint64_t, metadataVersion)

// LIZ_MATOCL_FUSE_TRUNCATE
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseTruncate, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseTruncate, kFinishedPacketVersion, 1)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseTruncate, kInProgressPacketVersion, 2)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseTruncate, LIZ_MATOCL_FUSE_TRUNCATE, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseTruncate, LIZ_MATOCL_FUSE_TRUNCATE, kFinishedPacketVersion,
		uint32_t, messageId,
		Attributes, attributes)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseTruncate, LIZ_MATOCL_FUSE_TRUNCATE, kInProgressPacketVersion,
		uint32_t, messageId,
		uint64_t, oldLength,
		uint32_t, lockId)

// LIZ_MATOCL_FUSE_TRUNCATE_END
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseTruncateEnd, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseTruncateEnd, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseTruncateEnd, LIZ_MATOCL_FUSE_TRUNCATE_END, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseTruncateEnd, LIZ_MATOCL_FUSE_TRUNCATE_END, kResponsePacketVersion,
		uint32_t, messageId,
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

} // namespace matocl
