/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

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

#include "common/access_control_list.h"
#include "common/attributes.h"
#include "common/acl_type.h"
#include "common/exception.h"
#include "common/goal.h"
#include "common/goal_map.h"
#include "common/quota.h"
#include "master/checksum.h"
#include "master/fs_context.h"
#include "master/metadata_dumper.h"

LIZARDFS_CREATE_EXCEPTION_CLASS_MSG(NoMetadataException, Exception, "no metadata");

/// Returns version of the loaded metadata.
uint64_t fs_getversion();

/// Returns checksum of the loaded metadata.
uint64_t fs_checksum(ChecksumMode mode);

/// Starts recalculating metadata checksum in background.
void fs_start_checksum_recalculation();

/// Load and apply changelogs.
int fs_load_changelogs();

/// Load whole filesystem information.
int fs_loadall();

/*! \brief Dump current state of file system metadata.
 *
 * \param dumpType - choose between foreground and background dumping.
 * \return True iff dump completed succefully.
 */
bool fs_storeall(MetadataDumper::DumpType dumpType);

// Functions which create/apply (depending on the given context) changes to the metadata.
// Common for metarestore and master server (both personalities)
uint8_t fs_acquire(const FsContext& context, uint32_t inode, uint32_t sessionid);
uint8_t fs_append(const FsContext& context, uint32_t inode, uint32_t inode_src);
uint8_t fs_deleteacl(const FsContext& context, uint32_t inode, AclType type);
uint8_t fs_link(const FsContext& context,
		uint32_t inode_src, uint32_t parent_dst, uint16_t nleng_dst, const uint8_t *name_dst,
		uint32_t *inode, Attributes* attr);
uint8_t fs_purge(const FsContext& context, uint32_t inode);
uint8_t fs_rename(const FsContext& context,
		uint32_t parent_src, uint16_t nleng_src, const uint8_t *name_src,
		uint32_t parent_dst, uint16_t nleng_dst, const uint8_t *name_dst,
		uint32_t *inode, Attributes* attr);
uint8_t fs_release(const FsContext& context, uint32_t inode, uint32_t sessionid);
uint8_t fs_setacl(const FsContext& context, uint32_t inode, AclType type, AccessControlList acl);
uint8_t fs_seteattr(const FsContext&
		context, uint32_t inode, uint8_t eattr, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
uint8_t fs_setgoal(const FsContext& context,
		uint32_t inode, uint8_t goal, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
uint8_t fs_settrashpath(const FsContext& context,
		uint32_t inode, uint32_t pleng, const uint8_t *path);
uint8_t fs_settrashtime(const FsContext& context,
		uint32_t inode, uint32_t trashtime, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
uint8_t fs_snapshot(const FsContext& context,
		uint32_t inode_src, uint32_t parent_dst, uint16_t nleng_dst, const uint8_t *name_dst,
		uint8_t canoverwrite);
uint8_t fs_symlink(const FsContext& context,
		uint32_t parent, uint16_t nleng, const uint8_t *name, uint32_t pleng, const uint8_t *path,
		uint32_t *inode, Attributes* attr);
uint8_t fs_undel(const FsContext& context, uint32_t inode);
uint8_t fs_writechunk(const FsContext& context, uint32_t inode, uint32_t indx,
		uint64_t *chunkid, uint8_t *opflag, uint64_t *length);
uint8_t fs_set_nextchunkid(const FsContext& context, uint64_t nextChunkId);

// Functions which apply changes from changelog, only for shadow master and metarestore
uint8_t fs_apply_checksum(const std::string& version, uint64_t checksum);
uint8_t fs_apply_create(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint8_t type,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode);
uint8_t fs_apply_access(uint32_t ts,uint32_t inode);
uint8_t fs_apply_attr(uint32_t ts,uint32_t inode,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime);
uint8_t fs_apply_session(uint32_t sessionid);
uint8_t fs_apply_emptytrash(uint32_t ts,uint32_t freeinodes,uint32_t reservedinodes);
uint8_t fs_apply_emptyreserved(uint32_t ts,uint32_t freeinodes);
uint8_t fs_apply_freeinodes(uint32_t ts,uint32_t freeinodes);
uint8_t fs_apply_incversion(uint64_t chunkid);
uint8_t fs_apply_length(uint32_t ts,uint32_t inode,uint64_t length);
uint8_t fs_apply_repair(uint32_t ts,uint32_t inode,uint32_t indx,uint32_t nversion);
uint8_t fs_apply_setxattr(uint32_t ts,uint32_t inode,uint32_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint32_t mode);
uint8_t fs_apply_setacl(uint32_t ts, uint32_t inode, char aclType, const char *aclString);
uint8_t fs_apply_setquota(char rigor, char resource, char ownerType, uint32_t ownerId, uint64_t limit);
uint8_t fs_apply_unlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint32_t inode);
uint8_t fs_apply_unlock(uint64_t chunkid);
uint8_t fs_apply_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid);

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
void fs_stats(uint32_t stats[16]);
void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes);
void fs_test_getdata(uint32_t *loopstart,uint32_t *loopend,uint32_t *files,uint32_t *ugfiles,uint32_t *mfiles,uint32_t *chunks,uint32_t *ugchunks,uint32_t *mchunks,char **msgbuff,uint32_t *msgbuffleng);
uint32_t fs_getdirpath_size(uint32_t inode);
void fs_getdirpath_data(uint32_t inode,uint8_t *buff,uint32_t size);
uint8_t fs_getrootinode(uint32_t *rootinode,const uint8_t *path);
void fs_statfs(uint32_t rootinode,uint8_t sesflags,uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes);
uint8_t fs_access(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,int modemask);
uint8_t fs_lookup(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint32_t *inode,Attributes& attr);
uint8_t fs_getattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,Attributes& attr);
uint8_t fs_setattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,SugidClearMode sugidclearmode,Attributes& attr);
uint8_t fs_try_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,Attributes& attr,uint64_t *chunkid);
uint8_t fs_end_setlength(uint64_t chunkid);
uint8_t fs_do_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,Attributes& attr);
uint8_t fs_readlink(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path);
uint8_t fs_mknod(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t umask,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint32_t rdev,uint32_t *inode,Attributes& attr);
uint8_t fs_mkdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint16_t mode,uint16_t umask,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t copysgid,uint32_t *inode,Attributes& attr);
uint8_t fs_repair(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t *notchanged,uint32_t *erased,uint32_t *repaired);
uint8_t fs_rmdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_readdir_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags,void **dnode,uint32_t *dbuffsize);
void fs_readdir_data(uint32_t rootinode,uint8_t sesflags,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,void *dnode,uint8_t *dbuff);
uint8_t fs_checkfile(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t chunkcount[CHUNK_MATRIX_SIZE]);
uint8_t fs_opencheck(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,Attributes& attr);
uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length);
uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid);
uint8_t fs_getgoal(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,GoalMap<uint32_t>& fgtab,GoalMap<uint32_t>& dgtab);
uint8_t fs_gettrashtime_prepare(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,void **fptr,void **dptr,uint32_t *fnodes,uint32_t *dnodes);
void fs_gettrashtime_store(void *fptr,void *dptr,uint8_t *buff);
uint8_t fs_geteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t feattrtab[16],uint32_t deattrtab[16]);
uint8_t fs_listxattr_leng(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,void **xanode,uint32_t *xasize);
void fs_listxattr_data(void *xanode,uint8_t *xabuff);
uint8_t fs_getxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue);
uint8_t fs_setxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode);
uint8_t fs_unlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_getacl(const FsContext& context, uint32_t inode, AclType type, AccessControlList& acl);
uint8_t fs_quota_get_all(uint8_t sesflags, uint32_t uid,
		std::vector<QuotaOwnerAndLimits>& results);
uint8_t fs_quota_get(uint8_t sesflags, uint32_t uid, uint32_t gid,
		const std::vector<QuotaOwner>& owners, std::vector<QuotaOwnerAndLimits>& results);
uint8_t fs_quota_set(uint8_t seslfags, uint32_t uid, const std::vector<QuotaEntry>& entries);

uint32_t fs_newsessionid(void);

// RESERVED
uint8_t fs_readreserved_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize);
void fs_readreserved_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff);

// TRASH
uint8_t fs_readtrash_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize);
void fs_readtrash_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff);
uint8_t fs_gettrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path);

// RESERVED+TRASH
uint8_t fs_getdetachedattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,Attributes& attr,uint8_t dtype);

// EXTRA
uint8_t fs_get_dir_stats(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *chunks,uint64_t *length,uint64_t *size,uint64_t *rsize);

// SPECIAL - LOG EMERGENCY INCREASE VERSION FROM CHUNKS-MODULE
void fs_incversion(uint64_t chunkid);

void fs_cs_disconnected(void);

/// Return the current definitions of goals
const GoalMap<Goal>& fs_get_goal_definitions();

int fs_init(void);
int fs_init(bool force);
#endif
