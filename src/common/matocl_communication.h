#pragma once

#include "common/platform.h"

#include "common/access_control_list.h"
#include "common/attributes.h"
#include "common/io_limits_database.h"
#include "common/moosefs_vector.h"
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

// LIZ_MATOCL_FUSE_GETGOAL
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetGoal, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetGoal, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetGoal, LIZ_MATOCL_FUSE_GETGOAL, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(FuseGetGoalStats,
		std::string, goalName,
		uint32_t, directories,
		uint32_t, files);

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetGoal, LIZ_MATOCL_FUSE_GETGOAL, kResponsePacketVersion,
		uint32_t, messageId,
		std::vector<FuseGetGoalStats>, goalsStats)

// LIZ_MATOCL_FUSE_SETGOAL
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseSetGoal, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseSetGoal, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseSetGoal, LIZ_MATOCL_FUSE_SETGOAL, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseSetGoal, LIZ_MATOCL_FUSE_SETGOAL, kResponsePacketVersion,
		uint32_t, messageId,
		uint32_t, changed,
		uint32_t, notChanged,
		uint32_t, notPermitted)
