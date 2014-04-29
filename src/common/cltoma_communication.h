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
		std::string, group,
		bool, wantMore,
		uint64_t, currentLimit_Bps,
		uint64_t, recentUsage_Bps)
