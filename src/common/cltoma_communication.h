#pragma once

#include "common/access_control_list.h"
#include "common/acl_type.h"
#include "common/packet.h"
#include "common/serialization_macros.h"
#include "common/string_8bit.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(cltoma, fuseMknod, LIZ_CLTOMA_FUSE_MKNOD, 0,
		uint32_t, messageId,
		uint32_t, inode,
		String8Bit, name,
		uint8_t, nodeType,
		uint16_t, mode,
		uint16_t, umask,
		uint32_t, uid,
		uint32_t, gid,
		uint32_t, rdev)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(cltoma, fuseMkdir, LIZ_CLTOMA_FUSE_MKDIR, 0,
		uint32_t, messageId,
		uint32_t, inode,
		String8Bit, name,
		uint16_t, mode,
		uint16_t, umask,
		uint32_t, uid,
		uint32_t, gid,
		bool, copySgid)

namespace cltoma {

namespace fuseDeleteAcl {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint32_t inode, uint32_t uid, uint32_t gid, AclType type) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_DELETE_ACL, 0, messageId, inode, uid, gid, type);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint32_t& messageId, uint32_t& inode, uint32_t& uid, uint32_t& gid, AclType& type) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, messageId, inode, uid, gid, type);
}

} // fuseDeleteAcl

namespace fuseGetAcl {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint32_t inode, uint32_t uid, uint32_t gid, AclType type) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_GET_ACL, 0, messageId, inode, uid, gid, type);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint32_t& messageId, uint32_t& inode, uint32_t& uid, uint32_t& gid, AclType& type) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, messageId, inode, uid, gid, type);
}

} // fuseGetAcl

namespace fuseSetAcl {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint32_t inode, uint32_t uid, uint32_t gid,
		AclType type, const AccessControlList& acl) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_SET_ACL, 0, messageId, inode, uid, gid, type, acl);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint32_t& messageId, uint32_t& inode, uint32_t& uid, uint32_t& gid,
		AclType& type, AccessControlList& acl) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, messageId, inode, uid, gid, type, acl);
}

} // fuseSetAcl

namespace iolimit {

inline void serialize(std::vector<uint8_t>& destination, const std::string& group, bool wantMore,
		uint64_t currentLimit_Bps, uint64_t recentUsage_Bps) {
	serializePacket(destination, LIZ_CLTOMA_IOLIMIT, 0, group, wantMore,
			currentLimit_Bps, recentUsage_Bps);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, std::string& group,
		bool& wantMore, uint64_t& currentLimit_Bps, uint64_t& recentUsage_Bps)
{
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, group, wantMore,
			currentLimit_Bps, recentUsage_Bps);
}

} // namespace iolimit

} // namespace cltoma
