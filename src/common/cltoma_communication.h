#pragma once

#include "common/platform.h"

#include <array>

#include "common/access_control_list.h"
#include "common/acl_type.h"
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

// LIZ_CLTOMA_IOLIMITS_STATUS
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, iolimitsStatus, LIZ_CLTOMA_IOLIMITS_STATUS, 0,
		uint32_t, messageId)

// LIZ_CLTOMA_METADATASERVER_STATUS
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, metadataserverStatus, LIZ_CLTOMA_METADATASERVER_STATUS, 0,
		uint32_t, messageId)

// LIZ_CLTOMA_METADATASERVERS_LIST
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, metadataserversList, LIZ_CLTOMA_METADATASERVERS_LIST, 0)

// LIZ_CLTOMA_FUSE_GETGOAL
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseGetGoal, LIZ_CLTOMA_FUSE_GETGOAL, 0,
		uint32_t, messageId,
		uint32_t, inode,
		uint8_t, gmode)

// LIZ_CLTOMA_FUSE_SETGOAL
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, fuseSetGoal, LIZ_CLTOMA_FUSE_SETGOAL, 0,
		uint32_t, messageId,
		uint32_t, inode,
		uint32_t, uid,
		std::string, goalName,
		uint8_t, smode)

// LIZ_CLTOMA_LIST_GOALS
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, listGoals, LIZ_CLTOMA_LIST_GOALS, 0,
		bool, dummy)

// LIZ_CLTOMA_CHUNKS_HEALTH
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, chunksHealth, LIZ_CLTOMA_CHUNKS_HEALTH, 0,
		bool, regularChunksOnly)

// LIZ_CLTOMA_CSERV_LIST
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, cservList, LIZ_CLTOMA_CSERV_LIST, 0,
		bool, dummy)

// LIZ_CLTOMA_CHUNK_INFO
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, chunkInfo, LIZ_CLTOMA_CHUNK_INFO, 0,
		uint32_t, messageId,
		uint32_t, inode,
		uint32_t, chunkIndex)

// LIZ_CLTOMA_HOSTNAME
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, hostname, LIZ_CLTOMA_HOSTNAME, 0)

// LIZ_CLTOMA_ADMIN_REGISTER
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, adminRegister, LIZ_CLTOMA_ADMIN_REGISTER_CHALLENGE, 0)

// LIZ_CLTOMA_ADMIN_REGISTER_RESPONSE
typedef std::array<uint8_t, 16> LizCltomaAdminRegisterResponseData;
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, adminRegisterResponse, LIZ_CLTOMA_ADMIN_REGISTER_RESPONSE, 0,
		LizCltomaAdminRegisterResponseData, data)

// LIZ_CLTOMA_ADMIN_BECOME_MASTER
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, adminBecomeMaster, LIZ_CLTOMA_ADMIN_BECOME_MASTER, 0)

// LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, adminStopWithoutMetadataDump, LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP, 0)

// LIZ_CLTOMA_ADMIN_RELOAD
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, adminReload, LIZ_CLTOMA_ADMIN_RELOAD, 0)

// LIZ_CLTOMA_ADMIN_SAVE_METADATA
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, adminSaveMetadata, LIZ_CLTOMA_ADMIN_SAVE_METADATA, 0,
		bool, asynchronous)

// LIZ_CLTOMA_ADMIN_RECALCULATE_METADATA_CHECKSUM
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, adminRecalculateMetadataChecksum, LIZ_CLTOMA_ADMIN_RECALCULATE_METADATA_CHECKSUM, 0,
		bool, asynchronous)

// LIZ_CLTOMA_TAPE_INFO
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cltoma, tapeInfo, LIZ_CLTOMA_TAPE_INFO, 0,
		uint32_t, messageId,
		uint32_t, inode)
