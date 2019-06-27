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

#include "common/platform.h"
#include "master/restore.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "protocol/MFSCommunication.h"
#include "common/lizardfs_error_codes.h"
#include "common/slogger.h"
#include "master/filesystem.h"
#include "master/filesystem_snapshot.h"
#include "master/filesystem_operations.h"

#define EAT(clptr,fn,vno,c) { \
	if (*(clptr)!=(c)) { \
		lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": '%c' expected", (fn), (vno), (c)); \
		return -1; \
	} \
	(clptr)++; \
}

#define GETNAME(name,clptr,fn,vno,c) { \
	uint32_t _tmp_i; \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	memset((void*)(name),0,256); \
	_tmp_i = 0; \
	while ((_tmp_c=*((clptr)++))!=c && _tmp_i<255) { \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": hex expected", (fn), (vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": hex expected", (fn), (vno)); \
				return -1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		name[_tmp_i++] = _tmp_c; \
	} \
	(clptr)--; \
	name[_tmp_i]=0; \
}

#define GETPATH(path,size,clptr,fn,vno,c) { \
	uint32_t _tmp_i; \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	_tmp_i = 0; \
	while ((_tmp_c=*((clptr)++))!=c) { \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": hex expected", (fn), (vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": hex expected", (fn), (vno)); \
				return -1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		if ((_tmp_i)>=(size)) { \
			(size) = _tmp_i+1000; \
			if ((path)==NULL) { \
				(path) = (uint8_t*)malloc(size); \
			} else { \
				uint8_t *_tmp_path = (path); \
				(path) = (uint8_t*)realloc((path),(size)); \
				if ((path)==NULL) { \
					free(_tmp_path); \
				} \
			} \
			if ((path)==NULL) { \
				lzfs_pretty_syslog(LOG_ERR, "out of memory"); \
				exit(1); \
			} \
		} \
		(path)[_tmp_i++]=_tmp_c; \
	} \
	if ((_tmp_i)>=(size)) { \
		(size) = _tmp_i+1000; \
		if ((path)==NULL) { \
			(path) = (uint8_t*)malloc(size); \
		} else { \
			uint8_t *_tmp_path = (path); \
			(path) = (uint8_t*)realloc((path),(size)); \
			if ((path)==NULL) { \
				free(_tmp_path); \
			} \
		} \
		if ((path)==NULL) { \
			lzfs_pretty_syslog(LOG_ERR, "out of memory"); \
			exit(1); \
		} \
	} \
	(clptr)--; \
	(path)[_tmp_i]=0; \
}

#define GETDATA(buff,leng,size,clptr,fn,vno,c) { \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	(leng) = 0; \
	while ((_tmp_c=*((clptr)++))!=c) { \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": hex expected", (fn), (vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": hex expected", (fn), (vno)); \
				return -1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		if ((leng)>=(size)) { \
			(size) = (leng)+1000; \
			if ((buff)==NULL) { \
				(buff) = (uint8_t*)malloc(size); \
			} else { \
				uint8_t *_tmp_buff = (buff); \
				(buff) = (uint8_t*)realloc((buff),(size)); \
				if ((buff)==NULL) { \
					free(_tmp_buff); \
				} \
			} \
			if ((buff)==NULL) { \
				lzfs_pretty_syslog(LOG_ERR, "out of memory"); \
				exit(1); \
			} \
		} \
		(buff)[(leng)++]=_tmp_c; \
	} \
	(clptr)--; \
}

#define GETCHAR(data,clptr) { \
	if (*(clptr)) { \
		(data) = *((clptr)++); \
	} \
}

#define GETU32(data,clptr) do { char* end_ = NULL; (data)=strtoul(clptr,&end_,10); clptr = end_; } while (0)
#define GETU64(data,clptr) do { char* end_ = NULL; (data)=strtoull(clptr,&end_,10); clptr = end_; } while (0)

int do_access(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_apply_access(ts,inode);
}

int do_append(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode,inode_src;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(inode_src,ptr);
	EAT(ptr,filename,lv,')');
	return fs_append(FsContext::getForRestore(ts), inode, inode_src);
}

int do_acquire(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode,cuid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_acquire(FsContext::getForRestore(ts), inode, cuid);
}

int do_attr(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode,mode,uid,gid,atime,mtime;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(atime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mtime,ptr);
	EAT(ptr,filename,lv,')');
	return fs_apply_attr(ts,inode,mode,uid,gid,atime,mtime);
}

int do_checksum(const char *filename, uint64_t lv, uint32_t, const char *ptr) {
	uint8_t version[256];
	uint64_t checksum;
	EAT(ptr,filename,lv,'(');
	GETNAME(version,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(checksum,ptr);
	return fs_apply_checksum((char*)&version, checksum);
}

int do_create(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t parent,mode,uid,gid,rdev,inode;
	uint8_t type,name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	type = *ptr;
	ptr++;
	EAT(ptr,filename,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(rdev,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_apply_create(ts, parent, HString((const char*)name), type, mode, uid, gid, rdev, inode);
}

int do_session(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t cuid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(cuid,ptr);
	return fs_apply_session(cuid);
}

int do_emptytrash_deprecated(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t reservedinodes,freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(reservedinodes,ptr);
	return fs_apply_emptytrash_deprecated(ts,freeinodes,reservedinodes);
}

int do_emptyreserved_deprecated(const char *filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	return fs_apply_emptyreserved_deprecated(ts,freeinodes);
}

int do_freeinodes(const char *filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	return fs_apply_freeinodes(ts,freeinodes);
}

int do_incversion(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_apply_incversion(chunkid);
}

int do_link(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	return fs_link(FsContext::getForRestore(ts), inode, parent, HString((const char*)name),
			nullptr, nullptr);
}

int do_length(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode;
	uint64_t length;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(length,ptr);
	EAT(ptr,filename,lv,')');
	return fs_apply_length(ts,inode,length);
}

int do_move(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,parent_src,parent_dst;
	uint8_t name_src[256],name_dst[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent_src,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name_src,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(parent_dst,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name_dst,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_rename(FsContext::getForRestore(ts),
			parent_src, HString((const char*)name_src),
			parent_dst, HString((const char*)name_dst),
			&inode, nullptr);
}

int do_lock_op(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t lock_type, inode, sessionid;
	uint64_t start, end;
	uint64_t owner;
	uint32_t op;
	const bool nonblocking = false;
	std::vector<FileLocks::Owner> dummy_applied;
	EAT(ptr, filename, lv, '(');
	GETU32(lock_type, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETU64(start, ptr);
	EAT(ptr ,filename, lv, ',');
	GETU64(end, ptr);
	EAT(ptr, filename, lv, ',');
	GETU64(owner, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(sessionid, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(op, ptr);
	EAT(ptr,filename,lv,')');

	int status = LIZARDFS_STATUS_OK;

	switch (static_cast<lzfs_locks::Type>(lock_type)) {
	case lzfs_locks::Type::kFlock:
		status = fs_flock_op(FsContext::getForRestore(ts), inode, owner, sessionid, 0, 0,
				op, nonblocking, dummy_applied);
		break;
	case lzfs_locks::Type::kPosix:
		status = fs_posixlock_op(FsContext::getForRestore(ts), inode, start, end, owner, sessionid, 0, 0,
				op, nonblocking, dummy_applied);
		break;
	default:
		lzfs_pretty_syslog(LOG_ERR, "Invalid lock type passed to restore: %u", lock_type);
		return LIZARDFS_ERROR_EINVAL;
	}

	if (status==LIZARDFS_ERROR_WAITING) {
		return LIZARDFS_STATUS_OK;
	}
	return status;
}

int do_remove_pending_op(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t lock_type;
	uint64_t ownerid;
	uint32_t sessionid;
	uint32_t inode;
	uint64_t reqid;

	EAT(ptr, filename, lv, '(');
	GETU32(lock_type, ptr);
	EAT(ptr, filename, lv, ',');
	GETU64(ownerid, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(sessionid, ptr);
	EAT(ptr ,filename, lv, ',');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETU64(reqid, ptr);
	EAT(ptr,filename,lv,')');

	return fs_locks_remove_pending(FsContext::getForRestore(ts), lock_type, ownerid, sessionid,
		inode, reqid);
}

int do_lock_clear_session(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t lock_type, inode, sessionid;
	std::vector<FileLocks::Owner> applied;

	EAT(ptr, filename, lv, '(');
	GETU32(lock_type, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(sessionid, ptr);
	EAT(ptr, filename, lv, ')');

	return fs_locks_clear_session(FsContext::getForRestore(ts), lock_type, inode, sessionid, applied);
}

int do_lock_unlock_inode(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t lock_type, inode;
	std::vector<FileLocks::Owner> applied;

	EAT(ptr, filename, lv, '(');
	GETU32(lock_type, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ')');

	return fs_locks_unlock_inode(FsContext::getForRestore(ts), lock_type, inode, applied);
}

int do_purge(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_purge(FsContext::getForRestore(ts), inode);
}

int do_release(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,cuid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_release(FsContext::getForRestore(ts), inode, cuid);
}

int do_repair(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,indx;
	uint32_t version;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(version,ptr);
	return fs_apply_repair(ts,inode,indx,version);
}

int do_seteattr(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t eattr,smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(eattr,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	return fs_seteattr(FsContext::getForRestoreWithUidGid(ts, uid, 0), inode, eattr, smode, &ci, &nci, &npi);
}

int do_setgoal(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode, uid, ci, nci, npi;
	uint8_t goal, smode;
	EAT(ptr, filename, lv, '(');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(uid, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(goal, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(smode, ptr);
	EAT(ptr, filename, lv, ')');
	if (*(ptr) == ':') {
		EAT(ptr, filename, lv, ':');
		GETU32(ci, ptr);
		if (*(ptr) == ',') {
			EAT(ptr, filename, lv, ',');
			GETU32(nci, ptr);
			EAT(ptr, filename, lv, ',');
			GETU32(npi, ptr);
			return fs_deprecated_setgoal(FsContext::getForRestoreWithUidGid(ts, uid, 0),
			                             inode, goal, smode, &ci, &nci, &npi);
		} else {
			return fs_apply_setgoal(FsContext::getForRestoreWithUidGid(ts, uid, 0),
			                        inode, goal, smode, ci);
		}
	} else {
		return fs_apply_setgoal(FsContext::getForRestoreWithUidGid(ts, uid, 0), inode, goal,
		                        smode, SetGoalTask::kChanged);
	}
}

int do_setpath(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode;
	static uint8_t *path = NULL;
	static uint32_t pathsize = 0;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETPATH(path,pathsize,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	return fs_settrashpath(FsContext::getForRestore(ts), inode, std::string((const char*)path));
}

int do_settrashtime(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode, uid, ci, nci, npi;
	uint32_t trashtime;
	uint8_t smode;
	EAT(ptr, filename, lv, '(');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(uid, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(trashtime, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(smode, ptr);
	EAT(ptr, filename, lv, ')');
	if ((*ptr) == ':') {
		EAT(ptr, filename, lv, ':');
		GETU32(ci, ptr);
		if ((*ptr) == ',') {
			EAT(ptr, filename, lv, ',');
			GETU32(nci, ptr);
			EAT(ptr, filename, lv, ',');
			GETU32(npi, ptr);
			return fs_deprecated_settrashtime(
			        FsContext::getForRestoreWithUidGid(ts, uid, 0), inode, trashtime,
			        smode, &ci, &nci, &npi);
		} else {
			return fs_apply_settrashtime(FsContext::getForRestoreWithUidGid(ts, uid, 0),
			                             inode, trashtime, smode, ci);
		}
	} else {
		return fs_apply_settrashtime(FsContext::getForRestoreWithUidGid(ts, uid, 0), inode,
		                             trashtime, smode, SetTrashtimeTask::kChanged);
	}
}

int do_setxattr(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,valueleng,mode;
	uint8_t name[256];
	static uint8_t *value = NULL;
	static uint32_t valuesize = 0;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETDATA(value,valueleng,valuesize,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_apply_setxattr(ts,inode,strlen((char*)name),name,valueleng,value,mode);
}

int do_deleteacl(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode;
	char aclTypeRaw = '\0';

	EAT(ptr, filename, lv, '(');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETCHAR(aclTypeRaw, ptr);
	EAT(ptr, filename, lv, ')');
	AclType aclType;
	if (aclTypeRaw == 'd') {
		aclType = AclType::kDefault;
	} else if (aclTypeRaw == 'a') {
		aclType = AclType::kAccess;
	} else if (aclTypeRaw == 'r') {
		aclType = AclType::kRichACL;
	} else {
		lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": corrupted ACL type", filename, lv);
		return -1;
	}
	return fs_deleteacl(FsContext::getForRestore(ts), inode, aclType);
}

int do_setacl(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode;
	char aclType = '\0';
	static uint8_t *aclString = NULL;
	static uint32_t aclSize = 0;

	EAT(ptr, filename, lv, '(');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETCHAR(aclType, ptr);
	EAT(ptr, filename, lv, ',');
	GETPATH(aclString, aclSize, ptr, filename, lv, ')');
	EAT(ptr, filename, lv, ')');

	return fs_apply_setacl(ts, inode, aclType, reinterpret_cast<const char*>(aclString));
}

int do_setrichacl(const char *filename, uint64_t lv, uint32_t ts, const char *ptr) {
	uint32_t inode;
	static uint8_t *acl_string = NULL;
	static uint32_t acl_size = 0;

	EAT(ptr, filename, lv, '(');
	GETU32(inode, ptr);
	EAT(ptr, filename, lv, ',');
	GETPATH(acl_string, acl_size, ptr, filename, lv, ')');
	EAT(ptr, filename, lv, ')');

	return fs_apply_setrichacl(ts, inode, reinterpret_cast<const char*>(acl_string));
}

int do_setquota(const char *filename, uint64_t lv, uint32_t, const char *ptr) {
	char rigor = '\0', resource = '\0', ownerType = '\0';
	uint32_t ownerId;
	uint64_t limit;

	EAT(ptr, filename, lv, '(');
	GETCHAR(rigor, ptr);
	EAT(ptr, filename, lv, ',');
	GETCHAR(resource, ptr);
	EAT(ptr, filename, lv, ',');
	GETCHAR(ownerType, ptr);
	EAT(ptr, filename, lv, ',');
	GETU32(ownerId, ptr);
	EAT(ptr, filename, lv, ',');
	GETU64(limit, ptr);
	EAT(ptr, filename, lv, ')');

	return fs_apply_setquota(rigor, resource, ownerType, ownerId, limit);
}

int do_snapshot(const char* /*filename*/, uint64_t /*lv*/, uint32_t /*ts*/, const char* /*ptr*/) {
	lzfs_pretty_syslog(LOG_ERR, "Trying to execute deprecated do_snapshot");
	return -1;
}

int do_clone_node(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t src_inode, dst_parent, dst_inode, can_overwrite;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(src_inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(dst_parent,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(dst_inode,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(can_overwrite,ptr);
	EAT(ptr,filename,lv,')');
	return fs_clone_node(FsContext::getForRestore(ts), src_inode, dst_parent, dst_inode,
				HString((const char*)name), can_overwrite);
}

int do_symlink(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t parent,uid,gid,inode;
	uint8_t name[256];
	static uint8_t *path = NULL;
	static uint32_t pathsize = 0;
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETPATH(path,pathsize,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_symlink(FsContext::getForRestoreWithUidGid(ts, uid, gid),
			parent, HString((char*)name), std::string((char*)path), &inode, nullptr);
}

int do_undel(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_undel(FsContext::getForRestore(ts), inode);
}

int do_unlink(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_apply_unlink(ts, parent, HString((char*)name), inode);
}

int do_unlock(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_apply_unlock(chunkid);
}

int do_nextchunkid(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint64_t nextChunkId;
	EAT(ptr, filename, lv, '(');
	GETU64(nextChunkId, ptr);
	EAT(ptr, filename, lv, ')');
	return fs_set_nextchunkid(FsContext::getForRestore(ts), nextChunkId);
}


int do_trunc(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,indx,lockid;
	uint64_t chunkid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(lockid,ptr);
	} else {
		lockid = 0;
	}
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	return fs_apply_trunc(ts,inode,indx,chunkid,lockid);
}

int do_write(const char* filename, uint64_t lv, uint32_t ts, const char* ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	uint32_t lockid;
	uint8_t opflag;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(opflag,ptr);
	} else {
		opflag=1;
	}
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(lockid,ptr);
	} else {
		lockid=1;
	}
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	return fs_writechunk(FsContext::getForRestore(ts), inode, indx, false, &lockid, &chunkid, &opflag, nullptr);
}

int restore_line(const char* filename, uint64_t lv, const char* line) {
	uint32_t ts;
	int status;

	status = LIZARDFS_ERROR_MAX;
	const char* ptr = line;

	EAT(ptr,filename,lv,':');
	EAT(ptr,filename,lv,' ');
	GETU32(ts,ptr);
	EAT(ptr,filename,lv,'|');
	switch (*ptr) {
		case 'A':
			if (strncmp(ptr,"ACCESS",6)==0) {
				status = do_access(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"ATTR",4)==0) {
				status = do_attr(filename,lv,ts,ptr+4);
			} else if (strncmp(ptr,"APPEND",6)==0) {
				status = do_append(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"ACQUIRE",7)==0) {
				status = do_acquire(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"AQUIRE",6)==0) {
				status = do_acquire(filename,lv,ts,ptr+6);
			}
			break;
		case 'C':
			if (strncmp(ptr,"CHECKSUM",8)==0) {
				status = do_checksum(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"CLONE",5)==0) {
				status = do_clone_node(filename,lv,ts,ptr+5);
			} else if (strncmp(ptr,"CREATE",6)==0) {
				status = do_create(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"CUSTOMER",8)==0) {      // deprecated
				status = do_session(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"CLRLCK",6)==0) {
				status = do_lock_clear_session(filename,lv,ts,ptr+6);
			}
			break;
		case 'D':
			if (strncmp(ptr,"DELETEACL",9)==0) {
				status = do_deleteacl(filename,lv,ts,ptr+9);
			}
			break;
		case 'E':
			if (strncmp(ptr,"EMPTYTRASH",10)==0) {
				status = do_emptytrash_deprecated(filename,lv,ts,ptr+10);
			} else if (strncmp(ptr,"EMPTYRESERVED",13)==0) {
				status = do_emptyreserved_deprecated(filename,lv,ts,ptr+13);
			}
			break;
		case 'F':
			if (strncmp(ptr,"FLCKINODE",9)==0) {
				status = do_lock_unlock_inode(filename,lv,ts,ptr+9);
			} else if (strncmp(ptr, "FLCK", 4) == 0) {
				status = do_lock_op(filename,lv,ts,ptr+4);
			} else if (strncmp(ptr, "FREEINODES", 10) == 0) {
				status = do_freeinodes(filename,lv,ts,ptr+10);
			}
			break;
		case 'I':
			if (strncmp(ptr,"INCVERSION",10)==0) {
				status = do_incversion(filename,lv,ts,ptr+10);
			}
			break;
		case 'L':
			if (strncmp(ptr,"LENGTH",6)==0) {
				status = do_length(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"LINK",4)==0) {
				status = do_link(filename,lv,ts,ptr+4);
			}
			break;
		case 'M':
			if (strncmp(ptr,"MOVE",4)==0) {
				status = do_move(filename,lv,ts,ptr+4);
			}
			break;
		case 'N':
			if (strncmp(ptr, "NEXTCHUNKID", 11) == 0) {
				status = do_nextchunkid(filename,lv,ts,ptr + 11);
			}
			break;
		case 'P':
			if (strncmp(ptr,"PURGE",5)==0) {
				status = do_purge(filename,lv,ts,ptr+5);
			}
			break;
		case 'R':
			if (strncmp(ptr,"RELEASE",7)==0) {
				status = do_release(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"REPAIR",6)==0) {
				status = do_repair(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"RMPLOCK",7)==0) {
				status = do_remove_pending_op(filename,lv,ts,ptr+7);
			}
			break;
		case 'S':
			if (strncmp(ptr,"SESSION",7)==0) {
				status = do_session(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETACL",6)==0) {
				status = do_setacl(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"SETEATTR",8)==0) {
				status = do_seteattr(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SETGOAL",7)==0) {
				status = do_setgoal(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETPATH",7)==0) {
				status = do_setpath(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETQUOTA",8)==0) {
				status = do_setquota(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SETTRASHTIME",12)==0) {
				status = do_settrashtime(filename,lv,ts,ptr+12);
			} else if (strncmp(ptr,"SETXATTR",8)==0) {
				status = do_setxattr(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SNAPSHOT",8)==0) {    // deprecated
				status = do_snapshot(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SYMLINK",7)==0) {
				status = do_symlink(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETRICHACL",10)==0) {
				status = do_setrichacl(filename,lv,ts,ptr+10);
			}
			break;
		case 'T':
			if (strncmp(ptr,"TRUNC",5)==0) {
				status = do_trunc(filename,lv,ts,ptr+5);
			}
			break;
		case 'U':
			if (strncmp(ptr,"UNLINK",6)==0) {
				status = do_unlink(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"UNDEL",5)==0) {
				status = do_undel(filename,lv,ts,ptr+5);
			} else if (strncmp(ptr,"UNLOCK",6)==0) {
				status = do_unlock(filename,lv,ts,ptr+6);
			}
			break;
		case 'W':
			if (strncmp(ptr,"WRITE",5)==0) {
				status = do_write(filename,lv,ts,ptr+5);
			}
			break;
		default:
			break;
	}

	if (status == LIZARDFS_ERROR_MAX) {
#ifndef METARESTORE
		lzfs_silent_syslog(LOG_DEBUG, "master.mismatch File %s, %" PRIu64 ", %s -- unknown entry",
			   filename, lv, line);
#endif
		lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": unknown entry '%s'", filename, lv, ptr);
	} else if (status != LIZARDFS_STATUS_OK) {
#ifndef METARESTORE
		lzfs_silent_syslog(LOG_DEBUG, "master.mismatch File %s, %" PRIu64 ", %s -- %s",
			   filename, lv, line, lizardfs_error_string(status));
#endif
		lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ": error: %d (%s)", filename, lv, status,
			lizardfs_error_string(status));
	}
	return status;
}

namespace {

uint64_t nextFsVersion = 0;
uint64_t currentFsVersion = 0; /* Metadata version from before restore_line() call. */
/*
 * By definition:
 *   nextFsVersion == currentFsVersion + 1
 * or:
 *   currentFsVersion == nextFsVersion - 1
 */
const char *lastfn = NULL;
uint8_t verbosity = 0;

}

void restore_reset() {
	nextFsVersion = 0;
	currentFsVersion = 0;
	lastfn = NULL;
}

uint8_t restore(const char* filename, uint64_t newLogVersion, const char *ptr, RestoreRigor rigor) {
	if (currentFsVersion == 0 || nextFsVersion == 0) {
		/*
		 * This is first call to restore().
		 */
		nextFsVersion = fs_getversion();
		currentFsVersion = nextFsVersion - 1;
		lastfn = "(no file)";
	}
	if (verbosity > 1) {
		lzfs_pretty_syslog(LOG_NOTICE, "filename: %s ; current meta version: %" PRIu64 " ; previous changeid: %"
				PRIu64 " ; current changeid: %" PRIu64 " ; change data%s",
				filename, nextFsVersion, currentFsVersion, newLogVersion, ptr);
	}
	if (newLogVersion < currentFsVersion) {
		lzfs_pretty_syslog(LOG_ERR,
				"merge error - possibly corrupted input file - ignore entry"
				" (filename: %s, versions: %" PRIu64 ", %" PRIu64 ")",
				filename, newLogVersion, currentFsVersion);
		return LIZARDFS_STATUS_OK;
	} else if (newLogVersion >= nextFsVersion) {
		if (newLogVersion == currentFsVersion) {
			if (verbosity > 1) {
				lzfs_pretty_syslog(LOG_WARNING, "duplicated entry: %" PRIu64 " (previous file: %s, current file: %s)",
						newLogVersion, lastfn, filename);
			}
		} else if (newLogVersion > currentFsVersion + 1) {
			lzfs_pretty_syslog(LOG_ERR, "hole in change files (entries from %s:%" PRIu64 " to %s:%" PRIu64
					" are missing) - add more files", lastfn, currentFsVersion + 1, filename, newLogVersion - 1);
			return LIZARDFS_ERROR_CHANGELOGINCONSISTENT;
		} else {
			if (verbosity > 0) {
				lzfs_pretty_syslog(LOG_NOTICE, "%s: change %s", filename, ptr);
			}
			int status = restore_line(filename,newLogVersion,ptr);
			if (status<0) { // parse error - stop processing if requested
				return (rigor == RestoreRigor::kIgnoreParseErrors ? 0 : LIZARDFS_ERROR_PARSE);
			}
			if (status != LIZARDFS_STATUS_OK) { // other errors - stop processing data
				return status;
			}
			nextFsVersion = fs_getversion();
			if ((newLogVersion + 1) != nextFsVersion) {
				/*
				 * restore_line() should bump nextFsVersion by exactly 1, but it didn't.
				 */
				lzfs_pretty_syslog(LOG_ERR, "%s:%" PRIu64 ":%" PRIu64 " version mismatch", filename, newLogVersion, nextFsVersion);
				return LIZARDFS_ERROR_METADATAVERSIONMISMATCH;
			}
		}
	}
	currentFsVersion = newLogVersion;
	lastfn = filename;
	return LIZARDFS_STATUS_OK;
}

void restore_setverblevel(uint8_t _vlevel) {
	verbosity = _vlevel;
}
