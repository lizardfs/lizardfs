#pragma once

#include "config.h"

#include "common/access_control_list.h"
#include "common/acl_type.h"
#include "common/MFSCommunication.h"
#include "common/moosefs_string.h"
#include "common/packet.h"
#include "common/quota.h"
#include "common/serialization_macros.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(cltoma, fuseMknod, LIZ_CLTOMA_FUSE_MKNOD, 0,
		uint32_t, messageId,
		uint32_t, inode,
		MooseFsString<uint8_t>, name,
		uint8_t, nodeType,
		uint16_t, mode,
		uint16_t, umask,
		uint32_t, uid,
		uint32_t, gid,
		uint32_t, rdev)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(cltoma, fuseMkdir, LIZ_CLTOMA_FUSE_MKDIR, 0,
		uint32_t, messageId,
		uint32_t, inode,
		MooseFsString<uint8_t>, name,
		uint16_t, mode,
		uint16_t, umask,
		uint32_t, uid,
		uint32_t, gid,
		bool, copySgid)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseDeleteAcl, LIZ_CLTOMA_FUSE_DELETE_ACL, 0,
		uint32_t, messageId,
		uint32_t, inode,
		uint32_t, uid,
		uint32_t, gid,
		AclType, type)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseGetAcl, LIZ_CLTOMA_FUSE_GET_ACL, 0,
		uint32_t, messageId,
		uint32_t, inode,
		uint32_t, uid,
		uint32_t, gid,
		AclType, type)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseSetAcl, LIZ_CLTOMA_FUSE_SET_ACL, 0,
		uint32_t, messageId,
		uint32_t, inode,
		uint32_t, uid,
		uint32_t, gid,
		AclType, type,
		AccessControlList, acl)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, iolimit, LIZ_CLTOMA_IOLIMIT, 0,
		uint32_t, msgid,
		uint32_t, configVersion,
		std::string, group,
		uint64_t, requestedBytes)

// LIZ_CLTOMA_FUSE_SET_QUOTA
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseSetQuota, LIZ_CLTOMA_FUSE_SET_QUOTA, 0,
		uint32_t, messageId,
		uint32_t, uid,
		uint32_t, gid,
		std::vector<QuotaEntry>, quotaEntries)

// LIZ_CLTOMA_FUSE_DELETE_QUOTA
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseDeleteQuota, LIZ_CLTOMA_FUSE_DELETE_QUOTA, 0,
		uint32_t, messageId,
		uint32_t, uid,
		uint32_t, gid,
		std::vector<QuotaEntryKey>, quotaEntriesKeys)

// LIZ_CLTOMA_FUSE_GET_QUOTA
LIZARDFS_DEFINE_PACKET_VERSION(cltoma, fuseGetQuota, kAllLimits, 0)
LIZARDFS_DEFINE_PACKET_VERSION(cltoma, fuseGetQuota, kSelectedLimits, 1)
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseGetQuota, LIZ_CLTOMA_FUSE_GET_QUOTA, kAllLimits,
		uint32_t, messageId,
		uint32_t, uid,
		uint32_t, gid)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseGetQuota, LIZ_CLTOMA_FUSE_GET_QUOTA, kSelectedLimits,
		uint32_t, messageId,
		uint32_t, uid,
		uint32_t, gid,
		std::vector<QuotaOwner>, owners)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseTruncate, LIZ_CLTOMA_FUSE_TRUNCATE, 0,
		uint32_t, messageId,
		uint32_t, inode,
		bool, isOpened,
		uint32_t, uid,
		uint32_t, gid,
		uint64_t, length)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseTruncateEnd, LIZ_CLTOMA_FUSE_TRUNCATE_END, 0,
		uint32_t, messageId,
		uint32_t, inode,
		uint32_t, uid,
		uint32_t, gid,
		uint64_t, length,
		uint32_t, lockid)

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
