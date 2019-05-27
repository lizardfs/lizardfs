/*
   Copyright 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include "common/access_control_list.h"
#include "common/attributes.h"
#include "common/chunk_type_with_address.h"
#include "common/chunk_with_address_and_label.h"
#include "common/chunks_availability_state.h"
#include "common/defective_file_info.h"
#include "common/io_limits_database.h"
#include "common/job_info.h"
#include "common/legacy_acl.h"
#include "common/metadataserver_list_entry.h"
#include "common/moosefs_string.h"
#include "common/moosefs_vector.h"
#include "common/richacl.h"
#include "common/serialization_macros.h"
#include "common/serialized_goal.h"
#include "common/tape_copy_location_info.h"
#include "protocol/chunkserver_list_entry.h"
#include "protocol/directory_entry.h"
#include "protocol/lock_info.h"
#include "protocol/named_inode_entry.h"
#include "protocol/MFSCommunication.h"
#include "protocol/packet.h"
#include "protocol/quota.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(matocl, updateCredentials, LIZ_MATOCL_UPDATE_CREDENTIALS, 0,
		uint32_t, messageId,
		uint8_t, status)

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
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetAcl, kLegacyResponsePacketVersion, 1)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetAcl, kResponsePacketVersion, 2)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetAcl, kRichACLResponsePacketVersion, 3)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kLegacyResponsePacketVersion,
		uint32_t, messageId,
		legacy::AccessControlList, acl)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kResponsePacketVersion,
		uint32_t, messageId,
		AccessControlList, acl)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kRichACLResponsePacketVersion,
		uint32_t, messageId,
		uint32_t, owner,
		RichACL, acl)

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
		std::vector<QuotaEntry>, quotaEntries,
		std::vector<std::string>, quotaInfo)

// LIZ_MATOCL_IOLIMITS_STATUS
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, iolimitsStatus, LIZ_MATOCL_IOLIMITS_STATUS, 0,
		uint32_t, messageId,
		uint32_t, configId,
		uint32_t, period_us,
		uint32_t, accumulate_ms,
		std::string, subsystem,
		std::vector<IoGroupAndLimit>, groupsAndLimits)

// LIZ_MATOCL_METADATASERVER_STATUS
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
		uint32_t, files,
		uint32_t, directories);

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

// LIZ_MATOCL_LIST_GOALS
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, listGoals, LIZ_MATOCL_LIST_GOALS, 0,
		std::vector<SerializedGoal>, serializedGoals)

// LIZ_MATOCL_CHUNKS_HEALTH
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, chunksHealth, LIZ_MATOCL_CHUNKS_HEALTH, 0,
		bool, regularChunksOnly,
		ChunksAvailabilityState, availability,
		ChunksReplicationState, replication)

// LIZ_MATOCL_CSERV_LIST
LIZARDFS_DEFINE_PACKET_VERSION(matocl, cservList, kStandard, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, cservList, kWithMessageId, 1)
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, cservList, LIZ_MATOCL_CSERV_LIST, kStandard,
		std::vector<ChunkserverListEntry>, cservList)
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, cservList, LIZ_MATOCL_CSERV_LIST, kWithMessageId,
		uint32_t, message_id,
		std::vector<ChunkserverListEntry>, cservList)

// LIZ_MATOCL_METADATASERVERS_LIST
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, metadataserversList, LIZ_MATOCL_METADATASERVERS_LIST, 0,
		uint32_t, masterVersion,
		std::vector<MetadataserverListEntry>, shadowList)

// LIZ_MATOCL_CHUNKS_INFO
namespace matocl {
namespace chunksInfo {
	static constexpr uint32_t kMaxNumberOfResultEntries = 4096;
}
}

LIZARDFS_DEFINE_PACKET_VERSION(matocl, chunksInfo, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, chunksInfo, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, chunksInfo, LIZ_MATOCL_CHUNKS_INFO, kStatusPacketVersion,
		uint32_t, message_id,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, chunksInfo, LIZ_MATOCL_CHUNKS_INFO, kResponsePacketVersion,
		uint32_t, message_id,
		std::vector<ChunkWithAddressAndLabel>, chunks)

// LIZ_MATOCL_HOSTNAME
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, hostname, LIZ_MATOCL_HOSTNAME, 0,
		std::string, hostname)

// LIZ_MATOCL_ADMIN_REGISTER_CHALLENGE
typedef std::array<uint8_t, 32> LizMatoclAdminRegisterChallengeData;
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, adminRegisterChallenge, LIZ_MATOCL_ADMIN_REGISTER_CHALLENGE, 0,
		LizMatoclAdminRegisterChallengeData, data)

// LIZ_MATOCL_ADMIN_REGISTER_RESPONSE
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, adminRegisterResponse, LIZ_MATOCL_ADMIN_REGISTER_RESPONSE, 0,
		uint8_t, status)

// LIZ_MATOCL_ADMIN_BECOME_MASTER
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, adminBecomeMaster, LIZ_MATOCL_ADMIN_BECOME_MASTER, 0,
		uint8_t, status)

// LIZ_MATOCL_ADMIN_STOP_WITHOUT_METADATA_DUMP
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, adminStopWithoutMetadataDump, LIZ_MATOCL_ADMIN_STOP_WITHOUT_METADATA_DUMP, 0,
		uint8_t, status)

// LIZ_MATOCL_ADMIN_RELOAD
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, adminReload, LIZ_MATOCL_ADMIN_RELOAD, 0,
		uint8_t, status)

// LIZ_MATOCL_ADMIN_SAVE_METADATA
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, adminSaveMetadata, LIZ_MATOCL_ADMIN_SAVE_METADATA, 0,
		uint8_t, status)

// LIZ_MATOCL_ADMIN_RECALCULATE_METADATA_CHECKSUM
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, adminRecalculateMetadataChecksum, LIZ_MATOCL_ADMIN_RECALCULATE_METADATA_CHECKSUM, 0,
		uint8_t, status)

// LIZ_MATOCL_TAPE_INFO
LIZARDFS_DEFINE_PACKET_VERSION(matocl, tapeInfo, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, tapeInfo, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, tapeInfo, LIZ_MATOCL_TAPE_INFO, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, tapeInfo, LIZ_MATOCL_TAPE_INFO, kResponsePacketVersion,
		uint32_t, messageId,
		std::vector<TapeCopyLocationInfo>, chunks)

// LIZ_MATOCL_TAPESERVERS_LIST
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, listTapeservers, LIZ_MATOCL_LIST_TAPESERVERS, 0,
		std::vector<TapeserverListEntry>, tapeservers)

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

// LIZ_MATOCL_FUSE_FLOCK
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseFlock, LIZ_MATOCL_FUSE_FLOCK, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_FUSE_GETLK
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetlk, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetlk, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetlk, LIZ_MATOCL_FUSE_GETLK, kStatusPacketVersion,
		uint32_t, message_id,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetlk, LIZ_MATOCL_FUSE_GETLK, kResponsePacketVersion,
		uint32_t, message_id,
		lzfs_locks::FlockWrapper, lock)

// LIZ_MATOCL_FUSE_SETLK
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseSetlk, LIZ_MATOCL_FUSE_SETLK, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_MANAGE_LOCKS
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, manageLocksList, LIZ_MATOCL_MANAGE_LOCKS_LIST, 0,
		std::vector<lzfs_locks::Info>, locks
		)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, manageLocksUnlock, LIZ_MATOCL_MANAGE_LOCKS_UNLOCK, 0,
		uint8_t, status)

// LIZ_MATOCL_WHOLE_PATH_LOOKUP
LIZARDFS_DEFINE_PACKET_VERSION(matocl, wholePathLookup, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, wholePathLookup, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, wholePathLookup, LIZ_MATOCL_WHOLE_PATH_LOOKUP, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, wholePathLookup, LIZ_MATOCL_WHOLE_PATH_LOOKUP, kResponsePacketVersion,
		uint32_t, messageId,
		uint32_t, inode,
		Attributes, attr)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, recursiveRemove, LIZ_MATOCL_RECURSIVE_REMOVE, 0,
		uint32_t, msgid,
		uint8_t, status)

// LIZ_MATOCL_FUSE_GETDIR
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetDir, kStatus, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetDir, kResponse, 1)

namespace matocl {
namespace fuseGetDir {
	const uint64_t kMaxNumberOfDirectoryEntries = 1 << 13;
}
}

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetDir, LIZ_MATOCL_FUSE_GETDIR, kStatus,
		uint32_t, messageId,
		uint8_t, status)


LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetDir, LIZ_MATOCL_FUSE_GETDIR, kResponse,
		uint32_t, message_id,
		uint64_t, first_entry_index,
		std::vector<DirectoryEntry>, dir_entry)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetReserved, LIZ_MATOCL_FUSE_GETRESERVED, 0,
		uint32_t, msgid,
		std::vector<NamedInodeEntry>, entries)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetTrash, LIZ_MATOCL_FUSE_GETTRASH, 0,
		uint32_t, msgid,
		std::vector<NamedInodeEntry>, entries)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, listTasks, LIZ_MATOCL_LIST_TASKS, 0,
		std::vector<JobInfo>, jobs_info)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, stopTask, LIZ_MATOCL_STOP_TASK, 0,
		uint32_t, msgid,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, requestTaskId, LIZ_MATOCL_REQUEST_TASK_ID, 0,
		uint32_t, msgid,
		uint32_t, taskid)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, snapshot, LIZ_MATOCL_FUSE_SNAPSHOT, 0,
		uint32_t, msgid,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, listDefectiveFiles, LIZ_MATOCL_LIST_DEFECTIVE_FILES, 0,
		uint64_t, last_entry_index,
		std::vector<DefectiveFileInfo>, files_info)

namespace matocl {

namespace fuseReadChunk {

const PacketVersion kStatusPacketVersion = 0;
const PacketVersion kResponsePacketVersion = 1;
const PacketVersion kECChunks_ResponsePacketVersion = 2;

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK, kStatusPacketVersion,
			messageId, status);
}

inline void deserialize(const std::vector<uint8_t>& source, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kStatusPacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId, status);
}

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
		const std::vector<legacy::ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK, kResponsePacketVersion,
			messageId, fileLength, chunkId, chunkVersion, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion,
		std::vector<legacy::ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId,
			fileLength, chunkId, chunkVersion, serversList);
}

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
		const std::vector<ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_READ_CHUNK, kECChunks_ResponsePacketVersion,
			messageId, fileLength, chunkId, chunkVersion, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion,
		std::vector<ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kECChunks_ResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId,
			fileLength, chunkId, chunkVersion, serversList);
}

} // namespace fuseReadChunk

namespace fuseWriteChunk {

const PacketVersion kStatusPacketVersion = 0;
const PacketVersion kResponsePacketVersion = 1;
const PacketVersion kECChunks_ResponsePacketVersion = 2;

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
		const std::vector<legacy::ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_WRITE_CHUNK, kResponsePacketVersion,
			messageId, fileLength, chunkId, chunkVersion, lockId, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion, uint32_t& lockId,
		std::vector<legacy::ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, dummyMessageId,
			fileLength, chunkId, chunkVersion, lockId, serversList);
}

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, uint64_t fileLength,
		uint64_t chunkId, uint32_t chunkVersion, uint32_t lockId,
		const std::vector<ChunkTypeWithAddress>& serversList) {
	serializePacket(destination, LIZ_MATOCL_FUSE_WRITE_CHUNK, kECChunks_ResponsePacketVersion,
			messageId, fileLength, chunkId, chunkVersion, lockId, serversList);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& fileLength, uint64_t& chunkId, uint32_t& chunkVersion, uint32_t& lockId,
		std::vector<ChunkTypeWithAddress>& serversList) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, kECChunks_ResponsePacketVersion);
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

} // namespace matocl
