/*
   Copyright 2008 Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _FILESYSTEM_H_
#define _FILESYSTEM_H_

#include <inttypes.h>

#ifdef METARESTORE


uint64_t fs_getversion(void);

uint8_t fs_access(uint32_t ts,uint32_t inode);
uint8_t fs_append(uint32_t ts,uint32_t inode,uint32_t inode_src);
uint8_t fs_aquire(uint32_t inode,uint32_t cuid);
uint8_t fs_attr(uint32_t ts,uint32_t inode,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime);
// int fs_copy(uint32_t ts,inode,parent,strlen(name),name);
uint8_t fs_create(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint8_t type,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode);
uint8_t fs_customer(uint32_t cuid);
uint8_t fs_emptytrash(uint32_t ts,uint32_t freeinodes,uint32_t reservedinodes);
uint8_t fs_emptyreserved(uint32_t ts,uint32_t freeinodes);
uint8_t fs_freeinodes(uint32_t ts,uint32_t freeinodes);
uint8_t fs_link(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint32_t nleng_dst,uint8_t *name_dst);
uint8_t fs_length(uint32_t ts,uint32_t inode,uint64_t length);
uint8_t fs_move(uint32_t ts,uint32_t parent_src,uint32_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint32_t nleng_dst,const uint8_t *name_dst,uint32_t inode);
uint8_t fs_reinit(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid);
uint8_t fs_release(uint32_t inode,uint32_t cuid);
uint8_t fs_symlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t inode);
uint8_t fs_setpath(uint32_t inode,const uint8_t *path);
uint8_t fs_unlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint32_t inode);
uint8_t fs_purge(uint32_t ts,uint32_t inode);
uint8_t fs_undel(uint32_t ts,uint32_t inode);
uint8_t fs_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid);
uint8_t fs_write(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid);
uint8_t fs_unlock(uint64_t chunkid);
uint8_t fs_incversion(uint64_t chunkid);
uint8_t fs_setgoal(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes);
uint8_t fs_settrashtime(uint32_t ts,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes);

void fs_dump(void);
void fs_term(const char *fname);
int fs_init(const char *fname);

#else
/* old mfsmount compatibility */
void fs_attr_to_attr32(const uint8_t attr[35],uint8_t attr32[32]);
void fs_attr32_to_attrvalues(const uint8_t attr32[32],uint16_t *attrmode,uint32_t *attruid,uint32_t *attrgid,uint32_t *attratime,uint32_t *attrmtime,uint64_t *attrlength);
/* -------------------------- */

// attr blob: [ type:8 goal:8 mode:16 uid:32 gid:32 atime:32 mtime:32 ctime:32 length:64 ]
void fs_stats(uint32_t stats[16]);
void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes,uint32_t *chunks,uint32_t *tdchunks);
void fs_test_getdata(uint32_t *loopstart,uint32_t *loopend,uint32_t *files,uint32_t *ugfiles,uint32_t *mfiles,uint32_t *chunks,uint32_t *ugchunks,uint32_t *mchunks,char **msgbuff,uint32_t *msgbuffleng);

// void fs_attrtoblob(uint8_t attr[32],uint8_t attrblob[32]);

void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes);
uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gid,int modemask);
uint8_t fs_lookup(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_getattr(uint32_t inode,uint8_t attr[35]);
uint8_t fs_setattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t attr[35]);

uint8_t fs_try_setlength(uint32_t inode,uint32_t uid,uint32_t gid,uint64_t length,uint8_t attr[35],uint64_t *chunkid);
uint8_t fs_end_setlength(uint64_t chunkid);
uint8_t fs_do_setlength(uint32_t inode,uint64_t length,uint8_t attr[35]);

uint8_t fs_readlink(uint32_t inode,uint32_t *pleng,uint8_t **path);
uint8_t fs_symlink(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t pleng,uint8_t *path,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_mknod(uint32_t parent,uint16_t nleng,uint8_t *name,uint8_t type,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_mkdir(uint32_t parent,uint16_t nleng,uint8_t *name,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_unlink(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_rmdir(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_rename(uint32_t parent_src,uint16_t nleng_src,uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint32_t uid,uint32_t gid);
uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_append(uint32_t inode,uint32_t inode_src,uint32_t uid,uint32_t gid);

uint8_t fs_readdir_size(uint32_t inode,uint32_t uid,uint32_t gid,void **dnode,uint32_t *dbuffsize,uint8_t packed);
void fs_readdir_data(void *dnode,uint8_t *dbuff,uint8_t packed);

uint8_t fs_checkfile(uint32_t inode,uint16_t chunkcount[256]);

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags);


uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length);
uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length);
uint8_t fs_reinitchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid);
uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid);

uint8_t fs_getgoal(uint32_t inode,uint8_t gmode,uint32_t fgtab[10],uint32_t dgtab[10]);
uint8_t fs_setgoal(uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes);

uint8_t fs_gettrashtime_prepare(uint32_t inode,uint8_t gmode,void **fptr,void **dptr,uint32_t *fnodes,uint32_t *dnodes);
void fs_gettrashtime_store(void *fptr,void *dptr,uint8_t *buff);
uint8_t fs_settrashtime(uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes);

// RESERVED
uint8_t fs_aquire(uint32_t inode,uint32_t cuid);
uint8_t fs_release(uint32_t inode,uint32_t cuid);
uint32_t fs_newcuid(void);

uint32_t fs_readreserved_size(uint8_t packed);
void fs_readreserved_data(uint8_t *dbuff,uint8_t packed);

// TRASH
uint32_t fs_readtrash_size(uint8_t packed);
void fs_readtrash_data(uint8_t *dbuff,uint8_t packed);
uint8_t fs_gettrashpath(uint32_t inode,uint32_t *pleng,uint8_t **path);
uint8_t fs_settrashpath(uint32_t inode,uint32_t pleng,uint8_t *path);
uint8_t fs_purge(uint32_t inode);
uint8_t fs_undel(uint32_t inode);

// RESERVED+TRASH
uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[35],uint8_t dtype);

// EXTRA
uint8_t fs_get_dir_stats(uint32_t inode,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *undergoalfiles,uint32_t *missingfiles,uint32_t *chunks,uint32_t *undergoalchunks,uint32_t *missingchunks,uint64_t *length,uint64_t *size,uint64_t *gsize);

// SPECIAL - LOG EMERGENCY INCREASE VERSION FROM CHUNKS-MODULE
void fs_incversion(uint64_t chunkid);

int fs_init(void);
#endif


#endif
