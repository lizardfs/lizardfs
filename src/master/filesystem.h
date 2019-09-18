/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <inttypes.h>
#include <string.h>
#include <map>

#include "common/access_control_list.h"
#include "common/attributes.h"
#include "common/acl_type.h"
#include "common/exception.h"
#include "common/goal.h"
#include "common/richacl.h"
#include "common/tape_key.h"
#include "common/tape_copy_location_info.h"
#include "master/checksum.h"
#include "master/filesystem_node.h"
#include "master/fs_context.h"
#include "master/hstring.h"
#include "master/matotsserv.h"
#include "master/metadata_dumper.h"
#include "master/setgoal_task.h"
#include "master/settrashtime_task.h"
#include "protocol/named_inode_entry.h"
#include "protocol/quota.h"

LIZARDFS_CREATE_EXCEPTION_CLASS_MSG(NoMetadataException, Exception, "no metadata");

uint8_t fs_cancel_job(uint32_t job_id);
uint32_t fs_reserve_job_id();

/// Returns version of the loaded metadata.
uint64_t fs_getversion();

/// Returns checksum of the loaded metadata.
uint64_t fs_checksum(ChecksumMode mode);

/// Starts recalculating metadata checksum in background.
/// \return LIZARDFS_STATUS_OK iff dump started successfully, otherwise cause of the failure.
uint8_t fs_start_checksum_recalculation();

/// Load and apply changelogs.
void fs_load_changelogs();

/// Load whole filesystem information.
int fs_loadall();

/*! \brief Dump current state of file system metadata.
 *
 * \param dumpType - choose between foreground and background dumping.
 * \return LIZARDFS_STATUS_OK iff dump started/completed successfully, otherwise cause of the failure.
 */
uint8_t fs_storeall(MetadataDumper::DumpType dumpType);

// Functions which create/apply (depending on the given context) changes to the metadata.
// Common for metarestore and master server (both personalities)
uint8_t fs_acquire(const FsContext& context, uint32_t inode, uint32_t sessionid);
uint8_t fs_append(const FsContext& context, uint32_t inode, uint32_t inode_src);
uint8_t fs_deleteacl(const FsContext& context, uint32_t inode, AclType type);
uint8_t fs_link(const FsContext& context,
		uint32_t inode_src, uint32_t parent_dst, const HString &name_dst,
		uint32_t *inode, Attributes* attr);
uint8_t fs_purge(const FsContext& context, uint32_t inode);
uint8_t fs_rename(const FsContext& context,
		uint32_t parent_src, const HString &name_src,
		uint32_t parent_dst, const HString &name_dst,
		uint32_t *inode, Attributes* attr);
uint8_t fs_release(const FsContext& context, uint32_t inode, uint32_t sessionid);
uint8_t fs_setacl(const FsContext& context, uint32_t inode, AclType type, const AccessControlList &acl);
uint8_t fs_setacl(const FsContext& context, uint32_t inode, const RichACL &acl);
uint8_t fs_seteattr(const FsContext&
		context, uint32_t inode, uint8_t eattr, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
uint8_t fs_setgoal(const FsContext &context, uint32_t inode, uint8_t goal, uint8_t smode,
		std::shared_ptr<SetGoalTask::StatsArray> setgoal_stats,
		const std::function<void(int)> &callback);
uint8_t fs_apply_setgoal(const FsContext &context, uint32_t inode, uint8_t goal, uint8_t smode,
		uint32_t master_result);
uint8_t fs_deprecated_setgoal(const FsContext &context, uint32_t inode, uint8_t goal, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
uint8_t fs_settrashpath(const FsContext& context,
		uint32_t inode, const std::string &path);
uint8_t fs_settrashtime(const FsContext &context, uint32_t inode, uint32_t trashtime, uint8_t smode,
			std::shared_ptr<SetTrashtimeTask::StatsArray> settrashtime_stats,
			const std::function<void(int)> &callback);
uint8_t fs_apply_settrashtime(const FsContext &context, uint32_t inode, uint32_t trashtime,
			      uint8_t smode, uint32_t master_result);
uint8_t fs_deprecated_settrashtime(const FsContext &context, uint32_t inode, uint32_t trashtime,
				   uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
				   uint32_t *nsinodes);
uint8_t fs_symlink(const FsContext& context,
		uint32_t parent, const HString &name, const std::string &path,
		uint32_t *inode, Attributes* attr);
uint8_t fs_undel(const FsContext& context, uint32_t inode);
uint8_t fs_writechunk(const FsContext& context, uint32_t inode, uint32_t indx,
		bool usedummylockid, /* inout */ uint32_t *lockid,
		uint64_t *chunkid, uint8_t *opflag, uint64_t *length, uint32_t min_server_version = 0);
uint8_t fs_set_nextchunkid(const FsContext& context, uint64_t nextChunkId);
uint8_t fs_access(const FsContext& context,uint32_t inode,int modemask);
uint8_t fs_lookup(const FsContext &context, uint32_t parent, const HString &name, uint32_t *inode, Attributes &attr);
uint8_t fs_whole_path_lookup(const FsContext &context, uint32_t parent, const std::string &path, uint32_t *found_inode, Attributes &attr);
uint8_t fs_getattr(const FsContext &context, uint32_t inode, Attributes &attr);
uint8_t fs_try_setlength(const FsContext &context, uint32_t inode, uint8_t opened, uint64_t length,
						 bool denyTruncatingParity,uint32_t lockid,Attributes& attr,uint64_t *chunkid);
uint8_t fs_do_setlength(const FsContext &context,uint32_t inode,uint64_t length,Attributes& attr);
uint8_t fs_setattr(const FsContext &context,uint32_t inode,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,SugidClearMode sugidclearmode,Attributes& attr);
uint8_t fs_readlink(const FsContext &context,uint32_t inode,std::string &path);
void fs_statfs(const FsContext &context,uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes);
uint8_t fs_mknod(const FsContext &context,uint32_t parent,const HString &name,uint8_t type,uint16_t mode,uint16_t umask,uint32_t rdev,uint32_t *inode,Attributes& attr);
uint8_t fs_mkdir(const FsContext &context,uint32_t parent,const HString &name,uint16_t mode,uint16_t umask,uint8_t copysgid,uint32_t *inode,Attributes& attr);
uint8_t fs_repair(const FsContext &context,uint32_t inode,uint8_t correct_only,uint32_t *notchanged,uint32_t *erased,uint32_t *repaired);
uint8_t fs_rmdir(const FsContext &context,uint32_t parent,const HString &name);
uint8_t fs_recursive_remove(const FsContext &context, uint32_t parent, const HString &name, const std::function<void(int)> &callback, uint32_t job_id = fs_reserve_job_id());
uint8_t fs_readdir_size(const FsContext &context,uint32_t inode,uint8_t flags,void **dnode,uint32_t *dbuffsize);
void fs_readdir_data(const FsContext &context,uint8_t flags,void *dnode,uint8_t *dbuff);

template <typename SerializableDirentType>
uint8_t fs_readdir(const FsContext &context, uint32_t inode, uint64_t first_entry, uint64_t number_of_entries,
		std::vector<SerializableDirentType> &dir_entries);
extern template uint8_t fs_readdir<legacy::DirectoryEntry>(const FsContext &context, uint32_t inode, uint64_t first_entry, uint64_t number_of_entries,
		std::vector<legacy::DirectoryEntry> &dir_entries);
extern template uint8_t fs_readdir<DirectoryEntry>(const FsContext &context, uint32_t inode, uint64_t first_entry, uint64_t number_of_entries,
		std::vector<DirectoryEntry> &dir_entries);

uint8_t fs_checkfile(const FsContext &context,uint32_t inode,uint32_t chunkcount[CHUNK_MATRIX_SIZE]);
uint8_t fs_opencheck(const FsContext &context,uint32_t inode,uint8_t flags,Attributes& attr);
uint8_t fs_getgoal(const FsContext &context,uint32_t inode,uint8_t gmode,GoalStatistics &fgtab, GoalStatistics &dgtab);
uint8_t fs_gettrashtime_prepare(const FsContext &context, uint32_t inode, uint8_t gmode, TrashtimeMap &fileTrashtimes, TrashtimeMap &dirTrashtimes);
uint8_t fs_geteattr(const FsContext &context,uint32_t inode,uint8_t gmode,uint32_t feattrtab[16],uint32_t deattrtab[16]);
uint8_t fs_listxattr_leng(const FsContext &context,uint32_t inode,uint8_t opened,void **xanode,uint32_t *xasize);
uint8_t fs_getxattr(const FsContext &context,uint32_t inode,uint8_t opened,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue);
uint8_t fs_setxattr(const FsContext &context,uint32_t inode,uint8_t opened,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode);
uint8_t fs_quota_get_all(const FsContext &context, std::vector<QuotaEntry> &results);
uint8_t fs_quota_get(const FsContext &context, const std::vector<QuotaOwner> &owners,
					 std::vector<QuotaEntry> &results);
uint8_t fs_unlink(const FsContext &context,uint32_t parent,const HString &name);
uint8_t fs_getacl(const FsContext& context, uint32_t inode, RichACL &acl);
uint8_t fs_quota_set(const FsContext &context, const std::vector<QuotaEntry>& entries);
uint8_t fs_quota_get_info(const FsContext &context, const std::vector<QuotaEntry> &entries,
		std::vector<std::string> &result);
uint8_t fs_getchunksinfo(const FsContext& context, uint32_t current_ip, uint32_t inode,
		uint32_t chunk_index, uint32_t chunk_count, std::vector<ChunkWithAddressAndLabel> &chunks);

// Functions which apply changes from changelog, only for shadow master and metarestore
uint8_t fs_apply_checksum(const std::string& version, uint64_t checksum);
uint8_t fs_apply_create(uint32_t ts,uint32_t parent,const HString &name,uint8_t type,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode);
uint8_t fs_apply_access(uint32_t ts,uint32_t inode);
uint8_t fs_apply_attr(uint32_t ts,uint32_t inode,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime);
uint8_t fs_apply_session(uint32_t sessionid);
uint8_t fs_apply_emptytrash_deprecated(uint32_t ts,uint32_t freeinodes,uint32_t reservedinodes);
uint8_t fs_apply_emptyreserved_deprecated(uint32_t ts,uint32_t freeinodes);
uint8_t fs_apply_freeinodes(uint32_t ts,uint32_t freeinodes);
uint8_t fs_apply_incversion(uint64_t chunkid);
uint8_t fs_apply_length(uint32_t ts,uint32_t inode,uint64_t length);
uint8_t fs_apply_repair(uint32_t ts,uint32_t inode,uint32_t indx,uint32_t nversion);
uint8_t fs_apply_setxattr(uint32_t ts,uint32_t inode,uint32_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint32_t mode);
uint8_t fs_apply_setacl(uint32_t ts, uint32_t inode, char aclType, const char *aclString);
uint8_t fs_apply_setrichacl(uint32_t ts, uint32_t inode, const std::string &acl_string);
uint8_t fs_apply_setquota(char rigor, char resource, char ownerType, uint32_t ownerId, uint64_t limit);
uint8_t fs_apply_unlink(uint32_t ts,uint32_t parent,const HString &name,uint32_t inode);
uint8_t fs_apply_unlock(uint64_t chunkid);
uint8_t fs_apply_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid,uint32_t lockid);

/// Unloads metadata.
/// This should be called in the shadow master each time it needs to download
/// metadata file from the active metadata server again.
void fs_unload();

/// Removes metadata lock leaving working directory in a clean state
void fs_unlock();

// Number of changelog file versions
const uint32_t kDefaultStoredPreviousBackMetaCopies = 1;
const uint32_t kMaxStoredPreviousBackMetaCopies = 99;

extern uint32_t gStoredPreviousBackMetaCopies;

#ifdef METARESTORE

void fs_dump(void);
void fs_term(const char *fname, bool noLock);
int fs_init(const char *fname,int ignoreflag, bool noLock);
void fs_disable_checksum_verification(bool value);

#else

// Functions which modify metadata or return some information.
// To be used by the master server with personality == kMaster
void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes);
uint32_t fs_getdirpath_size(uint32_t inode);
void fs_getdirpath_data(uint32_t inode,uint8_t *buff,uint32_t size);
uint8_t fs_getrootinode(uint32_t *rootinode,const uint8_t *path);
uint8_t fs_end_setlength(uint64_t chunkid);
uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length);
uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid, uint32_t lockid);
void fs_gettrashtime_store(TrashtimeMap &fileTrashtimes, TrashtimeMap &dirTrashtimes,uint8_t *buff);
void fs_listxattr_data(void *xanode,uint8_t *xabuff);

uint32_t fs_newsessionid(void);

// RESERVED
uint8_t fs_readreserved_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize);
void fs_readreserved_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff);
void fs_readreserved(uint32_t off, uint32_t max_entries, std::vector<NamedInodeEntry> &entries);

// TRASH
uint8_t fs_readtrash_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize);
void fs_readtrash_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff);
void fs_readtrash(uint32_t off, uint32_t max_entries, std::vector<NamedInodeEntry> &entries);
uint8_t fs_gettrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,std::string &path);

// RESERVED+TRASH
uint8_t fs_getdetachedattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,Attributes& attr,uint8_t dtype);

// EXTRA
uint8_t fs_get_dir_stats(const FsContext &context,uint32_t inode,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *chunks,uint64_t *length,uint64_t *size,uint64_t *rsize);
uint8_t fs_get_chunkid(const FsContext& context,
		uint32_t inode, uint32_t index, uint64_t *chunkid);

// TAPES

/// Adds information that the given file has a copy on the given tapeserver.
uint8_t fs_add_tape_copy(const TapeKey& takeKey, TapeserverId tapeserver);

/// Get list of tape copies created
uint8_t fs_get_tape_copy_locations(uint32_t inode, std::vector<TapeCopyLocationInfo>& locations);

// SPECIAL - LOG EMERGENCY INCREASE VERSION FROM CHUNKS-MODULE
void fs_incversion(uint64_t chunkid);

void fs_cs_disconnected(void);

/// Return the current definitions of all goals.
const std::map<int, Goal>& fs_get_goal_definitions();

/// Return the current definition of the given (by ID) goal.
const Goal& fs_get_goal_definition(uint8_t goalId);

/// Return info about currently executed tasks
std::vector<JobInfo> fs_get_current_tasks_info();
// Disable saving metadata on exit
void fs_disable_metadata_dump_on_exit();

/// Erases a message from metadata lockfile.
/// This function should be called before the first operation which may change
/// files in data dir (e.g., rotation of logs, creating new metadata file, ...)
void fs_erase_message_from_lockfile();

int fs_init(void);
int fs_init(bool force);
#endif
