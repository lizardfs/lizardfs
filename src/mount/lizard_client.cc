/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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
#include "mount/lizard_client.h"

#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <cstdint>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "common/access_control_list.h"
#include "common/acl_converter.h"
#include "common/acl_type.h"
#include "common/datapack.h"
#include "common/lru_cache.h"
#include "common/mfserr.h"
#include "common/slogger.h"
#include "common/special_inode_defs.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"
#include "mount/acl_cache.h"
#include "mount/chunk_locator.h"
#include "mount/client_common.h"
#include "mount/direntry_cache.h"
#include "mount/errno_defs.h"
#include "mount/g_io_limiters.h"
#include "mount/io_limit_group.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/oplog.h"
#include "mount/readdata.h"
#include "mount/special_inode.h"
#include "mount/stats.h"
#include "mount/symlinkcache.h"
#include "mount/tweaks.h"
#include "mount/writedata.h"
#include "protocol/MFSCommunication.h"
#include "protocol/matocl.h"

#include "mount/stat_defs.h" // !!! This must be last include. Do not move !!!

namespace LizardClient {

#define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

#define PKGVERSION \
		((LIZARDFS_PACKAGE_VERSION_MAJOR)*1000000 + \
		(LIZARDFS_PACKAGE_VERSION_MINOR)*1000 + \
		(LIZARDFS_PACKAGE_VERSION_MICRO))

// #define MASTER_NAME ".master"
// #define MASTER_INODE 0x7FFFFFFF
// 0x01b6 == 0666
// static uint8_t masterattr[35]={'f', 0x01,0xB6, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define IS_SPECIAL_INODE(ino) ((ino)>=SPECIAL_INODE_BASE)
#define IS_SPECIAL_NAME(name) ((name)[0]=='.' && (strcmp(SPECIAL_FILE_NAME_STATS,(name))==0 \
		|| strcmp(SPECIAL_FILE_NAME_MASTERINFO,(name))==0 || strcmp(SPECIAL_FILE_NAME_OPLOG,(name))==0 \
		|| strcmp(SPECIAL_FILE_NAME_OPHISTORY,(name))==0 || strcmp(SPECIAL_FILE_NAME_TWEAKS,(name))==0 \
		|| strcmp(SPECIAL_FILE_NAME_FILE_BY_INODE,(name))==0))

static GroupCache gGroupCache;

#define RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, group_id, function_expression) \
		do { \
			const uint32_t kSecondaryGroupsBit = (uint32_t)1 << 31; \
			status = function_expression; \
			if (status == LIZARDFS_ERROR_GROUPNOTREGISTERED) { \
				uint32_t index = group_id ^ kSecondaryGroupsBit; \
				GroupCache::Groups groups = gGroupCache.findByIndex(index); \
				if (!groups.empty()) { \
					update_credentials(index, groups); \
					status = function_expression; \
				} \
			} \
		} while (0);

int updateGroups(const GroupCache::Groups &groups) {
	static const uint32_t kSecondaryGroupsBit = (uint32_t)1 << 31;

	auto result = gGroupCache.find(groups);
	uint32_t gid = 0;
	uint32_t index;
	if (result.found == false) {
		try {
			index = gGroupCache.put(groups);
			LizardClient::update_credentials(index, groups);
			gid = index | kSecondaryGroupsBit;
		} catch (LizardClient::RequestException &e) {
			lzfs_pretty_syslog(LOG_ERR, "Cannot update groups: %d", e.errNo);
		}
	} else {
		gid = result.index | kSecondaryGroupsBit;
	}
	return gid;
}

Inode getSpecialInodeByName(const char *name) {
	if (strcmp(name, SPECIAL_FILE_NAME_MASTERINFO) == 0) {
		return SPECIAL_INODE_MASTERINFO;
	} else if (strcmp(name, SPECIAL_FILE_NAME_STATS) == 0) {
		return SPECIAL_INODE_STATS;
	} else if (strcmp(name, SPECIAL_FILE_NAME_TWEAKS) == 0) {
		return SPECIAL_INODE_TWEAKS;
	} else if (strcmp(name, SPECIAL_FILE_NAME_OPLOG) == 0) {
		return SPECIAL_INODE_OPLOG;
	} else if (strcmp(name, SPECIAL_FILE_NAME_OPHISTORY) == 0) {
		return SPECIAL_INODE_OPHISTORY;
	} else if (strcmp(name, SPECIAL_FILE_NAME_FILE_BY_INODE) == 0) {
		return SPECIAL_INODE_FILE_BY_INODE;
	} else {
		return MAX_REGULAR_INODE;
	}
}

bool isSpecialInode(Inode ino) {
	return IS_SPECIAL_INODE(ino);
}

enum {IO_NONE,IO_READ,IO_WRITE,IO_READONLY,IO_WRITEONLY};

typedef struct _finfo {
	uint8_t mode;
	void *data;
	uint8_t use_flocks;
	uint8_t use_posixlocks;
	pthread_mutex_t lock;
	pthread_mutex_t flushlock;
} finfo;

static DirEntryCache gDirEntryCache;
static unsigned gDirEntryCacheMaxSize = 100000;

static int debug_mode = 0;
static int usedircache = 1;
static int keep_cache = 0;
static double direntry_cache_timeout = 0.1;
static double entry_cache_timeout = 0.0;
static double attr_cache_timeout = 0.1;
static int mkdir_copy_sgid = 0;
static int sugid_clear_mode = 0;
static bool acl_enabled = 0;
bool use_rwlock = 0;
static std::atomic<bool> gDirectIo(false);

// lock_request_counter shared by flock and setlk
static uint32_t lock_request_counter = 0;
static std::mutex lock_request_mutex;


static std::unique_ptr<AclCache> acl_cache;

inline void eraseAclCache(Inode inode) {
	acl_cache->erase(
			inode    , 0, 0, (AclType)0,
			inode + 1, 0, 0, (AclType)0);
}

// TODO consider making oplog_printf asynchronous

/**
 * A wrapper around pthread_rwlock, acquiring a lock during construction and releasing it during
 * destruction in case if the lock wasn't released beforehand.
 */
struct PthreadRwLockWrapper {
	PthreadRwLockWrapper(pthread_rwlock_t& mutex, bool write = true)
		: rwlock_(mutex), locked_(false) {
		lock(write);
	}

	~PthreadRwLockWrapper() {
		if (locked_) {
			unlock();
		}
	}

	void lock(bool write = true) {
		sassert(!locked_);
		if (write) {
			pthread_rwlock_wrlock(&rwlock_);
		} else {
			pthread_rwlock_rdlock(&rwlock_);
		}
		locked_ = true;
	}
	void unlock() {
		sassert(locked_);
		locked_ = false;
		pthread_rwlock_unlock(&rwlock_);
	}

private:
	pthread_rwlock_t& rwlock_;
	bool locked_;
};

static uint64_t *statsptr[STATNODES];

/// prints "status: string-representation" if status is non zero and debug_mode is true
inline int errorconv_dbg(uint8_t status) {
	auto ret = mfs_errorconv(status);
	if (debug_mode && ret != 0) {
		fprintf(stderr, "status: %s\n", strerr(ret));
	}
	return ret;
}

void statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"fuse_ops",0);
	statsptr[OP_SETXATTR] = stats_get_counterptr(stats_get_subnode(s,"setxattr",0));
	statsptr[OP_GETXATTR] = stats_get_counterptr(stats_get_subnode(s,"getxattr",0));
	statsptr[OP_LISTXATTR] = stats_get_counterptr(stats_get_subnode(s,"listxattr",0));
	statsptr[OP_REMOVEXATTR] = stats_get_counterptr(stats_get_subnode(s,"removexattr",0));
	statsptr[OP_FSYNC] = stats_get_counterptr(stats_get_subnode(s,"fsync",0));
	statsptr[OP_FLUSH] = stats_get_counterptr(stats_get_subnode(s,"flush",0));
	statsptr[OP_WRITE] = stats_get_counterptr(stats_get_subnode(s,"write",0));
	statsptr[OP_READ] = stats_get_counterptr(stats_get_subnode(s,"read",0));
	statsptr[OP_RELEASE] = stats_get_counterptr(stats_get_subnode(s,"release",0));
	statsptr[OP_OPEN] = stats_get_counterptr(stats_get_subnode(s,"open",0));
	statsptr[OP_CREATE] = stats_get_counterptr(stats_get_subnode(s,"create",0));
	statsptr[OP_RELEASEDIR] = stats_get_counterptr(stats_get_subnode(s,"releasedir",0));
	statsptr[OP_READDIR] = stats_get_counterptr(stats_get_subnode(s,"readdir",0));
	statsptr[OP_OPENDIR] = stats_get_counterptr(stats_get_subnode(s,"opendir",0));
	statsptr[OP_LINK] = stats_get_counterptr(stats_get_subnode(s,"link",0));
	statsptr[OP_RENAME] = stats_get_counterptr(stats_get_subnode(s,"rename",0));
	statsptr[OP_READLINK] = stats_get_counterptr(stats_get_subnode(s,"readlink",0));
	statsptr[OP_READLINK_CACHED] = stats_get_counterptr(stats_get_subnode(s,"readlink-cached",0));
	statsptr[OP_SYMLINK] = stats_get_counterptr(stats_get_subnode(s,"symlink",0));
	statsptr[OP_RMDIR] = stats_get_counterptr(stats_get_subnode(s,"rmdir",0));
	statsptr[OP_MKDIR] = stats_get_counterptr(stats_get_subnode(s,"mkdir",0));
	statsptr[OP_UNLINK] = stats_get_counterptr(stats_get_subnode(s,"unlink",0));
	statsptr[OP_MKNOD] = stats_get_counterptr(stats_get_subnode(s,"mknod",0));
	statsptr[OP_SETATTR] = stats_get_counterptr(stats_get_subnode(s,"setattr",0));
	statsptr[OP_GETATTR] = stats_get_counterptr(stats_get_subnode(s,"getattr",0));
	statsptr[OP_DIRCACHE_GETATTR] = stats_get_counterptr(stats_get_subnode(s,"getattr-cached",0));
	statsptr[OP_LOOKUP] = stats_get_counterptr(stats_get_subnode(s,"lookup",0));
	statsptr[OP_LOOKUP_INTERNAL] = stats_get_counterptr(stats_get_subnode(s,"lookup-internal",0));
	if (usedircache) {
		statsptr[OP_DIRCACHE_LOOKUP] = stats_get_counterptr(stats_get_subnode(s,"lookup-cached",0));
	}
	statsptr[OP_ACCESS] = stats_get_counterptr(stats_get_subnode(s,"access",0));
	statsptr[OP_STATFS] = stats_get_counterptr(stats_get_subnode(s,"statfs",0));
	if (usedircache) {
		statsptr[OP_GETDIR_FULL] = stats_get_counterptr(stats_get_subnode(s,"getdir-full",0));
	} else {
		statsptr[OP_GETDIR_SMALL] = stats_get_counterptr(stats_get_subnode(s,"getdir-small",0));
	}
	statsptr[OP_GETLK] = stats_get_counterptr(stats_get_subnode(s,"getlk",0));
	statsptr[OP_SETLK] = stats_get_counterptr(stats_get_subnode(s,"setlk",0));
	statsptr[OP_FLOCK] = stats_get_counterptr(stats_get_subnode(s,"flock",0));
}

void stats_inc(uint8_t id) {
	if (id < STATNODES) {
		stats_lock();
		(*statsptr[id])++;
		stats_unlock();
	}
}

void type_to_stat(uint32_t inode,uint8_t type, struct stat *stbuf) {
	memset(stbuf,0,sizeof(struct stat));
	stbuf->st_ino = inode;
	switch (type) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR;
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK;
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG;
		break;
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO;
		break;
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK;
		break;
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK;
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR;
		break;
	default:
		stbuf->st_mode = 0;
	}
}

uint8_t attr_get_mattr(const Attributes &attr) {
	return (attr[1]>>4);    // higher 4 bits of mode
}

void attr_to_stat(uint32_t inode, const Attributes &attr, struct stat *stbuf) {
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime,attrnlink,attrrdev;
	uint64_t attrlength;
	const uint8_t *ptr;
	ptr = attr.data();
	attrtype = get8bit(&ptr);
	attrmode = get16bit(&ptr);
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);
	attratime = get32bit(&ptr);
	attrmtime = get32bit(&ptr);
	attrctime = get32bit(&ptr);
	attrnlink = get32bit(&ptr);
	memset(stbuf, 0, sizeof(*stbuf));
	stbuf->st_ino = inode;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLKSIZE
	stbuf->st_blksize = MFSBLOCKSIZE;
#endif
	switch (attrtype) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR | (attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK | (attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG | (attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO | (attrmode & 07777);
		stbuf->st_size = 0;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK | (attrmode & 07777);
		stbuf->st_size = 0;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK | (attrmode & 07777);
		attrrdev = get32bit(&ptr);
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR | (attrmode & 07777);
		attrrdev = get32bit(&ptr);
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	default:
		stbuf->st_mode = 0;
	}
	stbuf->st_uid = attruid;
	stbuf->st_gid = attrgid;
	stbuf->st_atime = attratime;
	stbuf->st_mtime = attrmtime;
	stbuf->st_ctime = attrctime;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BIRTHTIME
	stbuf->st_birthtime = attrctime;        // for future use
#endif
	stbuf->st_nlink = attrnlink;
}

void makemodestr(char modestr[11],uint16_t mode) {
	uint32_t i;
	strcpy(modestr,"?rwxrwxrwx");
	switch (mode & S_IFMT) {
	case S_IFSOCK:
		modestr[0] = 's';
		break;
	case S_IFLNK:
		modestr[0] = 'l';
		break;
	case S_IFREG:
		modestr[0] = '-';
		break;
	case S_IFBLK:
		modestr[0] = 'b';
		break;
	case S_IFDIR:
		modestr[0] = 'd';
		break;
	case S_IFCHR:
		modestr[0] = 'c';
		break;
	case S_IFIFO:
		modestr[0] = 'f';
		break;
	}
	if (mode & S_ISUID) {
		modestr[3] = 's';
	}
	if (mode & S_ISGID) {
		modestr[6] = 's';
	}
	if (mode & S_ISVTX) {
		modestr[9] = 't';
	}
	for (i=0 ; i<9 ; i++) {
		if ((mode & (1<<i))==0) {
			if (modestr[9-i]=='s' || modestr[9-i]=='t') {
				modestr[9-i]&=0xDF;
			} else {
				modestr[9-i]='-';
			}
		}
	}
}

void makeattrstr(char *buff,uint32_t size,struct stat *stbuf) {
	char modestr[11];
	makemodestr(modestr,stbuf->st_mode);
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_RDEV
	if (modestr[0]=='b' || modestr[0]=='c') {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%" PRIu64 ",%08lX]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(uint64_t)(stbuf->st_size),(unsigned long int)(stbuf->st_rdev));
	} else {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%" PRIu64 "]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(uint64_t)(stbuf->st_size));
	}
#else
	snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%" PRIu64 "]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(uint64_t)(stbuf->st_size));
#endif
}

RequestException::RequestException(int errNo) : errNo(errNo) {
	sassert(errNo != 0);
}

struct statvfs statfs(Context ctx, Inode ino) {
	uint64_t totalspace,availspace,trashspace,reservedspace;
	uint32_t inodes;
	uint32_t bsize;
	struct statvfs stfsbuf;
	memset(&stfsbuf,0,sizeof(stfsbuf));

	stats_inc(OP_STATFS);
	if (debug_mode) {
		oplog_printf(ctx, "statfs (%lu)", (unsigned long int)ino);
	}
	(void)ino;
	fs_statfs(&totalspace,&availspace,&trashspace,&reservedspace,&inodes);

#if defined(__APPLE__)
	if (totalspace>0x0001000000000000ULL) {
		bsize = 0x20000;
	} else {
		bsize = 0x10000;
	}
#else
	bsize = 0x10000;
#endif

	stfsbuf.f_namemax = MFS_NAME_MAX;
	stfsbuf.f_frsize = bsize;
	stfsbuf.f_bsize = bsize;
#if defined(__APPLE__)
	// FUSE on apple (or other parts of kernel) expects 32-bit values, so it's better to saturate this values than let being cut on 32-bit
	// can't change bsize also because 64k seems to be the biggest acceptable value for bsize

	if (totalspace/bsize>0xFFFFFFFFU) {
		stfsbuf.f_blocks = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_blocks = totalspace/bsize;
	}
	if (availspace/bsize>0xFFFFFFFFU) {
		stfsbuf.f_bfree = 0xFFFFFFFFU;
		stfsbuf.f_bavail = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_bfree = availspace/bsize;
		stfsbuf.f_bavail = availspace/bsize;
	}
#else
	stfsbuf.f_blocks = totalspace/bsize;
	stfsbuf.f_bfree = availspace/bsize;
	stfsbuf.f_bavail = availspace/bsize;
#endif
	stfsbuf.f_files = MAX_REGULAR_INODE;
	stfsbuf.f_ffree = MAX_REGULAR_INODE - inodes;
	stfsbuf.f_favail = MAX_REGULAR_INODE - inodes;
	//stfsbuf.f_flag = ST_RDONLY;
	oplog_printf(ctx, "statfs (%lu): OK (%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu32 ")",
			(unsigned long int)ino,
			totalspace,
			availspace,
			trashspace,
			reservedspace,
			inodes);
	return stfsbuf;
}

void access(Context ctx, Inode ino, int mask) {
	int status;

	int mmode;

	oplog_printf(ctx, "access (%lu,0x%X)",
			(unsigned long int)ino,
			mask);
	stats_inc(OP_ACCESS);
#if (R_OK==MODE_MASK_R) && (W_OK==MODE_MASK_W) && (X_OK==MODE_MASK_X)
	mmode = mask & (MODE_MASK_R | MODE_MASK_W | MODE_MASK_X);
#else
	mmode = 0;
	if (mask & R_OK) {
		mmode |= MODE_MASK_R;
	}
	if (mask & W_OK) {
		mmode |= MODE_MASK_W;
	}
	if (mask & X_OK) {
		mmode |= MODE_MASK_X;
	}
#endif
	if (IS_SPECIAL_INODE(ino)) {
		if (mask & (W_OK | X_OK)) {
			throw RequestException(EACCES);
		}
		return;
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_access(ino,ctx.uid,ctx.gid,mmode));
	status = errorconv_dbg(status);
	if (status!=0) {
		throw RequestException(status);
	}
}

EntryParam lookup(Context ctx, Inode parent, const char *name, bool whole_path_lookup) {
	EntryParam e;
	uint64_t maxfleng;
	uint32_t inode;
	uint32_t nleng;
	Attributes attr;
	char attrstr[256];
	uint8_t mattr;
	uint8_t icacheflag;
	int status;

	if (debug_mode) {
		oplog_printf(ctx, "lookup (%lu,%s) ...",
				(unsigned long int)parent,
				name);
		fprintf(stderr,"lookup (%lu,%s)\n",(unsigned long int)parent,name);
	}
	nleng = strlen(name);
	if (nleng > MFS_NAME_MAX) {
		stats_inc(OP_LOOKUP);
		oplog_printf(ctx, "lookup (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}
	if (parent == SPECIAL_INODE_ROOT) {
		if (nleng == 2 && name[0] == '.' && name[1] == '.') {
			nleng = 1;
		}

		Inode ino = getSpecialInodeByName(name);
		if (IS_SPECIAL_INODE(ino)) {
			return special_lookup(ino, ctx, parent, name, attrstr);
		}
	}
	if (parent == SPECIAL_INODE_FILE_BY_INODE) {
		char *endptr = nullptr;
		inode = strtol(name, &endptr, 10);
		if (endptr == nullptr || *endptr != '\0') {
			throw RequestException(EINVAL);
		}
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_getattr(inode, ctx.uid, ctx.gid, attr));
		status = errorconv_dbg(status);
		icacheflag = 0;
	} else if (usedircache && gDirEntryCache.lookup(ctx,parent,std::string(name,nleng),inode,attr)) {
		if (debug_mode) {
			fprintf(stderr,"lookup: sending data from dircache\n");
		}
		stats_inc(OP_DIRCACHE_LOOKUP);
		status = 0;
		icacheflag = 1;
//              oplog_printf(ctx, "lookup (%lu,%s) (using open dir cache): OK (%lu)",(unsigned long int)parent,name,(unsigned long int)inode);
	} else {
		stats_inc(OP_LOOKUP);
		if (whole_path_lookup) {
			RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_whole_path_lookup(parent, std::string(name, nleng), ctx.uid, ctx.gid, &inode, attr));
		} else {
			RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid,&inode,attr));
		}
		status = errorconv_dbg(status);
		icacheflag = 0;
	}
	if (status!=0) {
		oplog_printf(ctx, "lookup (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				strerr(status));
		throw RequestException(status);
	}
	if (attr[0]==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(inode);
	} else {
		maxfleng = 0;
	}
	e.ino = inode;
	mattr = attr_get_mattr(attr);
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:((attr[0]==TYPE_DIRECTORY)?direntry_cache_timeout:entry_cache_timeout);
	attr_to_stat(inode,attr,&e.attr);
	if (maxfleng>(uint64_t)(e.attr.st_size)) {
		e.attr.st_size=maxfleng;
	}
	makeattrstr(attrstr,256,&e.attr);
	oplog_printf(ctx, "lookup (%lu,%s)%s: OK (%.1f,%lu,%.1f,%s)",
			(unsigned long int)parent,
			name,
			icacheflag?" (using open dir cache)":"",
			e.entry_timeout,
			(unsigned long int)e.ino,
			e.attr_timeout,
			attrstr);
	return e;
}

AttrReply getattr(Context ctx, Inode ino, FileInfo *fi) {
	uint64_t maxfleng;
	double attr_timeout;
	struct stat o_stbuf;
	Attributes attr;
	char attrstr[256];
	int status;
	(void)fi;

	if (debug_mode) {
		oplog_printf(ctx, "getattr (%lu) ...",
				(unsigned long int)ino);
		fprintf(stderr,"getattr (%lu)\n",(unsigned long int)ino);
	}

	if (IS_SPECIAL_INODE(ino)) {
		return special_getattr(ino, ctx, fi, attrstr);
	}

	maxfleng = write_data_getmaxfleng(ino);
	if (usedircache && gDirEntryCache.lookup(ctx,ino,attr)) {
		if (debug_mode) {
			fprintf(stderr,"getattr: sending data from dircache\n");
		}
		stats_inc(OP_DIRCACHE_GETATTR);
		status = 0;
	} else {
		stats_inc(OP_GETATTR);
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_getattr(ino,ctx.uid,ctx.gid,attr));
		status = errorconv_dbg(status);
	}
	if (status!=0) {
		oplog_printf(ctx, "getattr (%lu): %s",
				(unsigned long int)ino,
				strerr(status));
		throw RequestException(status);
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	attr_to_stat(ino,attr,&o_stbuf);
	if (attr[0]==TYPE_FILE && maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	attr_timeout = (attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(ctx, "getattr (%lu): OK (%.1f,%s)",
			(unsigned long int)ino,
			attr_timeout,
			attrstr);
	return AttrReply{o_stbuf, attr_timeout};
}

AttrReply setattr(Context ctx, Inode ino, struct stat *stbuf,
	          int to_set, FileInfo *fi) {
	struct stat o_stbuf;
	uint64_t maxfleng;
	Attributes attr;
	char modestr[11];
	char attrstr[256];
	double attr_timeout;
	int status;

	makemodestr(modestr,stbuf->st_mode);
	stats_inc(OP_SETATTR);
	if (debug_mode) {
		oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]) ...",
			(unsigned long int)ino,
			to_set,
			modestr+1,
			(unsigned int)(stbuf->st_mode & 07777),
			(long int)stbuf->st_uid,
			(long int)stbuf->st_gid,
			(unsigned long int)(stbuf->st_atime),
			(unsigned long int)(stbuf->st_mtime),
			(uint64_t)(stbuf->st_size));
		fprintf(stderr,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "])\n",
			(unsigned long int)ino,
			to_set,
			modestr+1,
			(unsigned int)(stbuf->st_mode & 07777),
			(long int)stbuf->st_uid,
			(long int)stbuf->st_gid,
			(unsigned long int)(stbuf->st_atime),
			(unsigned long int)(stbuf->st_mtime),
			(uint64_t)(stbuf->st_size));
	}

	if (IS_SPECIAL_INODE(ino)) {
		return special_setattr(ino, ctx, stbuf, to_set, fi, modestr, attrstr);
	}

	status = EINVAL;
	maxfleng = write_data_getmaxfleng(ino);
	if ((to_set & (LIZARDFS_SET_ATTR_MODE
			| LIZARDFS_SET_ATTR_UID
			| LIZARDFS_SET_ATTR_GID
			| LIZARDFS_SET_ATTR_ATIME
			| LIZARDFS_SET_ATTR_ATIME_NOW
			| LIZARDFS_SET_ATTR_MTIME
			| LIZARDFS_SET_ATTR_MTIME_NOW
			| LIZARDFS_SET_ATTR_SIZE)) == 0) { // change other flags or change nothing
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_setattr(ino,ctx.uid,ctx.gid,0,0,0,0,0,0,0,attr));    // ext3 compatibility - change ctime during this operation (usually chown(-1,-1))
		status = errorconv_dbg(status);
		if (status!=0) {
			oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",
					(unsigned long int)ino,
					to_set,
					modestr+1,
					(unsigned int)(stbuf->st_mode & 07777),
					(long int)stbuf->st_uid,
					(long int)stbuf->st_gid,
					(unsigned long int)(stbuf->st_atime),
					(unsigned long int)(stbuf->st_mtime),
					(uint64_t)(stbuf->st_size),
					strerr(status));
			throw RequestException(status);
		}
	}
	if (to_set & (LIZARDFS_SET_ATTR_MODE
			| LIZARDFS_SET_ATTR_UID
			| LIZARDFS_SET_ATTR_GID
			| LIZARDFS_SET_ATTR_ATIME
			| LIZARDFS_SET_ATTR_MTIME
			| LIZARDFS_SET_ATTR_ATIME_NOW
			| LIZARDFS_SET_ATTR_MTIME_NOW)) {
		uint8_t setmask = 0;
		if (to_set & LIZARDFS_SET_ATTR_MODE) {
			setmask |= SET_MODE_FLAG;
		}
		if (to_set & LIZARDFS_SET_ATTR_UID) {
			setmask |= SET_UID_FLAG;
		}
		if (to_set & LIZARDFS_SET_ATTR_GID) {
			setmask |= SET_GID_FLAG;
		}
		if (to_set & LIZARDFS_SET_ATTR_ATIME) {
			setmask |= SET_ATIME_FLAG;
		}
		if (to_set & LIZARDFS_SET_ATTR_ATIME_NOW) {
			setmask |= SET_ATIME_NOW_FLAG;
		}
		if (to_set & LIZARDFS_SET_ATTR_MTIME) {
			setmask |= SET_MTIME_FLAG;
		}
		if (to_set & LIZARDFS_SET_ATTR_MTIME_NOW) {
			setmask |= SET_MTIME_NOW_FLAG;
		}
		if (to_set & (LIZARDFS_SET_ATTR_MTIME | LIZARDFS_SET_ATTR_MTIME_NOW)) {
			// in this case we want flush all pending writes because they could overwrite mtime
			write_data_flush_inode(ino);
		}
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_setattr(ino,ctx.uid,ctx.gid,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,sugid_clear_mode,attr));
		if (to_set & (LIZARDFS_SET_ATTR_MODE | LIZARDFS_SET_ATTR_UID | LIZARDFS_SET_ATTR_GID)) {
			eraseAclCache(ino);
		}
		status = errorconv_dbg(status);
		if (status!=0) {
			oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",
					(unsigned long int)ino,
					to_set,
					modestr+1,
					(unsigned int)(stbuf->st_mode & 07777),
					(long int)stbuf->st_uid,
					(long int)stbuf->st_gid,
					(unsigned long int)(stbuf->st_atime),
					(unsigned long int)(stbuf->st_mtime),
					(uint64_t)(stbuf->st_size),
					strerr(status));
			throw RequestException(status);
		}
	}
	if (to_set & LIZARDFS_SET_ATTR_SIZE) {
		if (stbuf->st_size<0) {
			oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",
					(unsigned long int)ino,
					to_set,
					modestr+1,
					(unsigned int)(stbuf->st_mode & 07777),
					(long int)stbuf->st_uid,
					(long int)stbuf->st_gid,
					(unsigned long int)(stbuf->st_atime),
					(unsigned long int)(stbuf->st_mtime),
					(uint64_t)(stbuf->st_size),
					strerr(EINVAL));
			throw RequestException(EINVAL);
		}
		if (stbuf->st_size>=MAX_FILE_SIZE) {
			oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",
					(unsigned long int)ino,
					to_set,
					modestr+1,
					(unsigned int)(stbuf->st_mode & 07777),
					(long int)stbuf->st_uid,
					(long int)stbuf->st_gid,
					(unsigned long int)(stbuf->st_atime),
					(unsigned long int)(stbuf->st_mtime),
					(uint64_t)(stbuf->st_size),
					strerr(EFBIG));
			throw RequestException(EFBIG);
		}
		try {
			bool opened = (fi != NULL);
			RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
				write_data_truncate(ino, opened, ctx.uid, ctx.gid, stbuf->st_size, attr));
			maxfleng = 0; // after the flush master server has valid length, don't use our length cache
		} catch (Exception& ex) {
			status = errorconv_dbg(ex.status());
		}
		read_inode_ops(ino);
		if (status!=0) {
			oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",
					(unsigned long int)ino,
					to_set,
					modestr+1,
					(unsigned int)(stbuf->st_mode & 07777),
					(long int)stbuf->st_uid,
					(long int)stbuf->st_gid,
					(unsigned long int)(stbuf->st_atime),
					(unsigned long int)(stbuf->st_mtime),
					(uint64_t)(stbuf->st_size),
					strerr(status));
			throw RequestException(status);
		}
	}
	if (status!=0) {        // should never happen but better check than sorry
		oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",
				(unsigned long int)ino,
				to_set,
				modestr+1,
				(unsigned int)(stbuf->st_mode & 07777),
				(long int)stbuf->st_uid,
				(long int)stbuf->st_gid,
				(unsigned long int)(stbuf->st_atime),
				(unsigned long int)(stbuf->st_mtime),
				(uint64_t)(stbuf->st_size),
				strerr(status));
		throw RequestException(status);
	}
	gDirEntryCache.lockAndInvalidateInode(ino);
	memset(&o_stbuf, 0, sizeof(struct stat));
	attr_to_stat(ino,attr,&o_stbuf);
	if (attr[0]==TYPE_FILE && maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	attr_timeout = (attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(ctx, "setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): OK (%.1f,%s)",
			(unsigned long int)ino,
			to_set,
			modestr+1,
			(unsigned int)(stbuf->st_mode & 07777),
			(long int)stbuf->st_uid,
			(long int)stbuf->st_gid,
			(unsigned long int)(stbuf->st_atime),
			(unsigned long int)(stbuf->st_mtime),
			(uint64_t)(stbuf->st_size),
			attr_timeout,
			attrstr);
	return AttrReply{o_stbuf, attr_timeout};
}

EntryParam mknod(Context ctx, Inode parent, const char *name, mode_t mode, dev_t rdev) {
	EntryParam e;
	uint32_t inode;
	Attributes attr;
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	uint8_t type;

	makemodestr(modestr,mode);
	stats_inc(OP_MKNOD);
	if (debug_mode) {
		oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX) ...",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev);
		fprintf(stderr,"mknod (%lu,%s,%s:0%04o,0x%08lX)\n",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): %s",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}
	if (S_ISFIFO(mode)) {
		type = TYPE_FIFO;
	} else if (S_ISCHR(mode)) {
		type = TYPE_CHARDEV;
	} else if (S_ISBLK(mode)) {
		type = TYPE_BLOCKDEV;
	} else if (S_ISSOCK(mode)) {
		type = TYPE_SOCKET;
	} else if (S_ISREG(mode) || (mode&0170000)==0) {
		type = TYPE_FILE;
	} else {
		oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): %s",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev,
				strerr(EPERM));
		throw RequestException(EPERM);
	}

	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): %s",
					(unsigned long int)parent,
					name,
					modestr,
					(unsigned int)mode,
					(unsigned long int)rdev,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,ctx.umask,ctx.uid,ctx.gid,rdev,inode,attr));
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): %s",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev,
				strerr(status));
		throw RequestException(status);
	} else {
		e.ino = inode;
		mattr = attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		attr_to_stat(inode,attr,&e.attr);
		makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): OK (%.1f,%lu,%.1f,%s)",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev,
				e.entry_timeout,
				(unsigned long int)e.ino,
				e.attr_timeout,
				attrstr);
		return e;
	}
}

void unlink(Context ctx, Inode parent, const char *name) {
	uint32_t nleng;
	int status;

	stats_inc(OP_UNLINK);
	if (debug_mode) {
		oplog_printf(ctx, "unlink (%lu,%s) ...",
				(unsigned long int)parent,
				name);
		fprintf(stderr,"unlink (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "unlink (%lu,%s): %s",
					(unsigned long int)parent,
					name,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}

	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "unlink (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_unlink(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid));
	gDirEntryCache.lockAndInvalidateParent(parent);
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "unlink (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				strerr(status));
		throw RequestException(status);
	} else {
		oplog_printf(ctx, "unlink (%lu,%s): OK",
				(unsigned long int)parent,
				name);
		return;
	}
}

EntryParam mkdir(Context ctx, Inode parent, const char *name, mode_t mode) {
	struct EntryParam e;
	uint32_t inode;
	Attributes attr;
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;

	makemodestr(modestr,mode);
	stats_inc(OP_MKDIR);
	if (debug_mode) {
		oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o) ...",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode);
		fprintf(stderr,"mkdir (%lu,%s,d%s:0%04o)\n",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o): %s",
					(unsigned long int)parent,
					name,
					modestr+1,
					(unsigned int)mode,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_mkdir(parent,nleng,(const uint8_t*)name,mode,ctx.umask,ctx.uid,ctx.gid,mkdir_copy_sgid,inode,attr));
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				strerr(status));
		throw RequestException(status);
	} else {
		e.ino = inode;
		mattr = attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:direntry_cache_timeout;
		attr_to_stat(inode,attr,&e.attr);
		makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o): OK (%.1f,%lu,%.1f,%s)",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				e.entry_timeout,
				(unsigned long int)e.ino,
				e.attr_timeout,
				attrstr);
		return e;
	}
}

void rmdir(Context ctx, Inode parent, const char *name) {
	uint32_t nleng;
	int status;

	stats_inc(OP_RMDIR);
	if (debug_mode) {
		oplog_printf(ctx, "rmdir (%lu,%s) ...",
				(unsigned long int)parent,
				name);
		fprintf(stderr,"rmdir (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "rmdir (%lu,%s): %s",
					(unsigned long int)parent,
					name,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "rmdir (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_rmdir(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid));
	gDirEntryCache.lockAndInvalidateParent(parent);
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "rmdir (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				strerr(status));
		throw RequestException(status);
	} else {
		oplog_printf(ctx, "rmdir (%lu,%s): OK",
				(unsigned long int)parent,
				name);
		return;
	}
}

EntryParam symlink(Context ctx, const char *path, Inode parent,
			 const char *name) {
	struct EntryParam e;
	uint32_t inode;
	Attributes attr;
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;

	stats_inc(OP_SYMLINK);
	if (debug_mode) {
		oplog_printf(ctx, "symlink (%s,%lu,%s) ...",
				path,
				(unsigned long int)parent,
				name);
		fprintf(stderr,"symlink (%s,%lu,%s)\n",path,(unsigned long int)parent,name);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "symlink (%s,%lu,%s): %s",
					path,
					(unsigned long int)parent,
					name,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "symlink (%s,%lu,%s): %s",
				path,
				(unsigned long int)parent,
				name,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)path,ctx.uid,ctx.gid,&inode,attr));
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "symlink (%s,%lu,%s): %s",
				path,
				(unsigned long int)parent,
				name,
				strerr(status));
		throw RequestException(status);
	} else {
		e.ino = inode;
		mattr = attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		attr_to_stat(inode,attr,&e.attr);
		makeattrstr(attrstr,256,&e.attr);
		symlink_cache_insert(inode, (const uint8_t *)path);
		oplog_printf(ctx, "symlink (%s,%lu,%s): OK (%.1f,%lu,%.1f,%s)",
				path,
				(unsigned long int)parent,
				name,
				e.entry_timeout,
				(unsigned long int)e.ino,
				e.attr_timeout,
				attrstr);
		return e;
	}
}

std::string readlink(Context ctx, Inode ino) {
	int status;
	const uint8_t *path;

	if (debug_mode) {
		oplog_printf(ctx, "readlink (%lu) ...",
				(unsigned long int)ino);
		fprintf(stderr,"readlink (%lu)\n",(unsigned long int)ino);
	}
	if (symlink_cache_search(ino,&path)) {
		stats_inc(OP_READLINK_CACHED);
		oplog_printf(ctx, "readlink (%lu) (using cache): OK (%s)",
				(unsigned long int)ino,
				(char*)path);
		return std::string((char*)path);
	}
	stats_inc(OP_READLINK);
	status = fs_readlink(ino,&path);
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "readlink (%lu): %s",
				(unsigned long int)ino,
				strerr(status));
		throw RequestException(status);
	} else {
		symlink_cache_insert(ino,path);
		oplog_printf(ctx, "readlink (%lu): OK (%s)",
				(unsigned long int)ino,
				(char*)path);
		return std::string((char*)path);
	}
}

void rename(Context ctx, Inode parent, const char *name,
			Inode newparent, const char *newname) {
	uint32_t nleng,newnleng;
	int status;
	uint32_t inode;
	Attributes attr;

	stats_inc(OP_RENAME);
	if (debug_mode) {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s) ...",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname);
		fprintf(stderr,"rename (%lu,%s,%lu,%s)\n",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
					(unsigned long int)parent,
					name,
					(unsigned long int)newparent,
					newname,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	if (newparent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
					(unsigned long int)parent,
					name,
					(unsigned long int)newparent,
					newname,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
	fs_rename(parent,nleng,(const uint8_t*)name,newparent,newnleng,(const uint8_t*)newname,ctx.uid,ctx.gid,&inode,attr));
	status = errorconv_dbg(status);
	gDirEntryCache.lockAndInvalidateParent(parent);
	gDirEntryCache.lockAndInvalidateParent(newparent);
	if (status!=0) {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname,
				strerr(status));
		throw RequestException(status);
	} else {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s): OK",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname);
		return;
	}
}

EntryParam link(Context ctx, Inode ino, Inode newparent, const char *newname) {
	uint32_t newnleng;
	int status;
	EntryParam e;
	uint32_t inode;
	Attributes attr;
	char attrstr[256];
	uint8_t mattr;


	stats_inc(OP_LINK);
	if (debug_mode) {
		oplog_printf(ctx, "link (%lu,%lu,%s) ...",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname);
		fprintf(stderr,"link (%lu,%lu,%s)\n",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "link (%lu,%lu,%s): %s",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname,
				strerr(EACCES));
		throw RequestException(EACCES);
	}
	if (newparent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(ctx, "link (%lu,%lu,%s): %s",
					(unsigned long int)ino,
					(unsigned long int)newparent,
					newname,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "link (%lu,%lu,%s): %s",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx.uid,ctx.gid,&inode,attr));
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "link (%lu,%lu,%s): %s",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname,
				strerr(status));
		throw RequestException(status);
	} else {
		e.ino = inode;
		mattr = attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		attr_to_stat(inode,attr,&e.attr);
		makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx, "link (%lu,%lu,%s): OK (%.1f,%lu,%.1f,%s)",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname,
				e.entry_timeout,
				(unsigned long int)e.ino,
				e.attr_timeout,
				attrstr);
		return e;
	}
}

void opendir(Context ctx, Inode ino, FileInfo *fi) {
	int status;

	fi->fh = 0;

	stats_inc(OP_OPENDIR);
	if (debug_mode) {
		oplog_printf(ctx, "opendir (%lu) ...",
				(unsigned long int)ino);
		fprintf(stderr,"opendir (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "opendir (%lu): %s",
				(unsigned long int)ino,
				strerr(ENOTDIR));
		throw RequestException(ENOTDIR);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_access(ino,ctx.uid,ctx.gid,MODE_MASK_R));    // at least test rights
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "opendir (%lu): %s",
				(unsigned long int)ino,
				strerr(status));
		throw RequestException(status);
	}
}

std::vector<DirEntry> readdir(Context ctx, Inode ino, off_t off, size_t max_entries, FileInfo */*fi*/) {
	static constexpr int kBatchSize = 1000;

	stats_inc(OP_READDIR);
	if (debug_mode) {
		oplog_printf(ctx, "readdir (%lu,%" PRIu64 ",%" PRIu64 ") ...",
				(unsigned long int)ino,
				(uint64_t)max_entries,
				(uint64_t)off);
		fprintf(stderr,"readdir (%lu,%" PRIu64 ",%" PRIu64 ")\n",
				(unsigned long int)ino,
				(uint64_t)max_entries,
				(uint64_t)off);
	}
	if (off<0) {
		oplog_printf(ctx, "readdir (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)max_entries,
				(uint64_t)off,
				strerr(EINVAL));
		throw RequestException(EINVAL);
	}

	std::vector<DirEntry> result;
	shared_lock<shared_mutex> access_guard(gDirEntryCache.rwlock());
	gDirEntryCache.updateTime();

	uint64_t entry_index = off;
	auto it = gDirEntryCache.find(ctx, ino, entry_index);

	result.reserve(max_entries);
	for(;it != gDirEntryCache.index_end() && max_entries > 0;++it) {
		if (!gDirEntryCache.isValid(it) || it->index != entry_index) {
			break;
		}

		if (it->inode == 0) {
			// we have valid 'no more entries' marker
			assert(it->name.empty());
			max_entries = 0;
			break;
		}

		++entry_index;
		--max_entries;

		struct stat stats;
		attr_to_stat(it->inode,it->attr,&stats);
		result.emplace_back(it->name, stats, entry_index);
	}

	if (max_entries == 0) {
		return result;
	}

	access_guard.unlock();

	std::vector<DirectoryEntry> dir_entries;
	uint8_t status;
	uint64_t request_size = std::min<std::size_t>(std::max<std::size_t>(kBatchSize, max_entries),
	                                              matocl::fuseGetDir::kMaxNumberOfDirectoryEntries);
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_getdir(ino, ctx.uid, ctx.gid, entry_index, request_size, dir_entries));
	auto data_acquire_time = gDirEntryCache.updateTime();

	if(status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}

	std::unique_lock<shared_mutex> write_guard(gDirEntryCache.rwlock());
	gDirEntryCache.updateTime();

	gDirEntryCache.insertSubsequent(ctx, ino, entry_index, dir_entries, data_acquire_time);
	if (dir_entries.size() < request_size) {
		// insert 'no more entries' marker
		gDirEntryCache.insert(ctx, ino, 0, entry_index + dir_entries.size(), "", Attributes{{}}, data_acquire_time);
		gDirEntryCache.invalidate(ctx,ino,entry_index + dir_entries.size() + 1);
	}

	if (gDirEntryCache.size() > gDirEntryCacheMaxSize) {
		gDirEntryCache.removeOldest(gDirEntryCache.size() - gDirEntryCacheMaxSize);
	}

	write_guard.unlock();

	for(auto it = dir_entries.begin(); it != dir_entries.end() && max_entries > 0; ++it) {
		--max_entries;
		++entry_index;

		struct stat stats;
		attr_to_stat(it->inode,it->attributes,&stats);
		result.emplace_back(it->name, stats, entry_index);
	}

	return result;
}

void releasedir(Context ctx, Inode ino, FileInfo */*fi*/) {
	static constexpr int kBatchSize = 1000;

	(void)ino;

	stats_inc(OP_RELEASEDIR);
	if (debug_mode) {
		oplog_printf(ctx, "releasedir (%lu) ...",
				(unsigned long int)ino);
		fprintf(stderr,"releasedir (%lu)\n",(unsigned long int)ino);
	}
	oplog_printf(ctx, "releasedir (%lu): OK",
			(unsigned long int)ino);

	std::unique_lock<shared_mutex> write_guard(gDirEntryCache.rwlock());
	gDirEntryCache.updateTime();
	gDirEntryCache.removeExpired(kBatchSize);
}


static finfo* fs_newfileinfo(uint8_t accmode, uint32_t inode) {
	finfo *fileinfo;
	fileinfo = (finfo*) malloc(sizeof(finfo));
	pthread_mutex_init(&(fileinfo->flushlock),NULL);
	pthread_mutex_init(&(fileinfo->lock),NULL);
	PthreadMutexWrapper lock((fileinfo->lock)); // make helgrind happy
#ifdef __FreeBSD__
	/* old FreeBSD fuse reads whole file when opening with O_WRONLY|O_APPEND,
	 * so can't open it write-only */
	(void)accmode;
	(void)inode;
	fileinfo->mode = IO_NONE;
	fileinfo->data = NULL;
#else
	if (accmode == O_RDONLY) {
		fileinfo->mode = IO_READONLY;
		fileinfo->data = read_data_new(inode);
	} else if (accmode == O_WRONLY) {
		fileinfo->mode = IO_WRITEONLY;
		fileinfo->data = write_data_new(inode);
	} else {
		fileinfo->mode = IO_NONE;
		fileinfo->data = NULL;
	}
#endif
	fileinfo->use_flocks = false;
	fileinfo->use_posixlocks = false;

	return fileinfo;
}

void remove_file_info(FileInfo *f) {
	finfo* fileinfo = (finfo*)(f->fh);
	PthreadMutexWrapper lock(fileinfo->lock);
	if (fileinfo->mode == IO_READONLY || fileinfo->mode == IO_READ) {
		read_data_end(fileinfo->data);
	} else if (fileinfo->mode == IO_WRITEONLY || fileinfo->mode == IO_WRITE) {
		write_data_end(fileinfo->data);
	}
	lock.unlock(); // This unlock is needed, since we want to destroy the mutex
	pthread_mutex_destroy(&(fileinfo->lock));
	pthread_mutex_destroy(&(fileinfo->flushlock));
	free(fileinfo);
}

EntryParam create(Context ctx, Inode parent, const char *name, mode_t mode,
		FileInfo* fi) {
	struct EntryParam e;
	uint32_t inode;
	uint8_t oflags;
	Attributes attr;
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;

	finfo *fileinfo;

	makemodestr(modestr,mode);
	stats_inc(OP_CREATE);
	if (debug_mode) {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o)",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode);
		fprintf(stderr,"create (%lu,%s,-%s:0%04o)\n",
				(unsigned long int)parent,
				name,modestr+1,
				(unsigned int)mode);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "create (%lu,%s,-%s:0%04o): %s",
					(unsigned long int)parent,
					name,
					modestr+1,
					(unsigned int)mode,
					strerr(EACCES));
			throw RequestException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				strerr(ENAMETOOLONG));
		throw RequestException(ENAMETOOLONG);
	}

	oflags = AFTER_CREATE;
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	} else {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				strerr(EINVAL));
		throw RequestException(EINVAL);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_mknod(parent,nleng,(const uint8_t*)name,TYPE_FILE,mode&07777,ctx.umask,ctx.uid,ctx.gid,0,inode,attr));
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o) (mknod): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				strerr(status));
		throw RequestException(status);
	}
	Attributes tmp_attr;
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_opencheck(inode,ctx.uid,ctx.gid,oflags,tmp_attr));

	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o) (open): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				strerr(status));
		throw RequestException(status);
	}

	mattr = attr_get_mattr(attr);
	fileinfo = fs_newfileinfo(fi->flags & O_ACCMODE,inode);
	fi->fh = reinterpret_cast<uintptr_t>(fileinfo);
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"create (%lu) ok -> keep cache: %lu\n",
				(unsigned long int)inode,
				(unsigned long int)fi->keep_cache);
	}
	gDirEntryCache.lockAndInvalidateParent(ctx, parent);
	e.ino = inode;
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
	attr_to_stat(inode,attr,&e.attr);
	makeattrstr(attrstr,256,&e.attr);
	oplog_printf(ctx, "create (%lu,%s,-%s:0%04o): OK (%.1f,%lu,%.1f,%s,%lu)",
			(unsigned long int)parent,
			name,
			modestr+1,
			(unsigned int)mode,
			e.entry_timeout,
			(unsigned long int)e.ino,
			e.attr_timeout,
			attrstr,
			(unsigned long int)fi->keep_cache);
	return e;
}

void open(Context ctx, Inode ino, FileInfo *fi) {
	uint8_t oflags;
	Attributes attr;
	uint8_t mattr;
	int status;

	finfo *fileinfo;

	stats_inc(OP_OPEN);
	if (debug_mode) {
		oplog_printf(ctx, "open (%lu) ...",
				(unsigned long int)ino);
		fprintf(stderr,"open (%lu)\n",(unsigned long int)ino);
	}

	if (IS_SPECIAL_INODE(ino)) {
		special_open(ino, ctx, fi);
		return;
	}

	oflags = 0;
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_opencheck(ino,ctx.uid,ctx.gid,oflags,attr));
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "open (%lu): %s",
				(unsigned long int)ino,
				strerr(status));
		throw RequestException(status);
	}

	mattr = attr_get_mattr(attr);
	fileinfo = fs_newfileinfo(fi->flags & O_ACCMODE,ino);
	fi->fh = reinterpret_cast<uintptr_t>(fileinfo);
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"open (%lu) ok -> keep cache: %lu\n",
				(unsigned long int)ino,
				(unsigned long int)fi->keep_cache);
	}
	fi->direct_io = gDirectIo;
	oplog_printf(ctx, "open (%lu): OK (%lu,%lu)",
			(unsigned long int)ino,
			(unsigned long int)fi->direct_io,
			(unsigned long int)fi->keep_cache);
}

void update_credentials(int index, const GroupCache::Groups &groups) {
	uint8_t status = fs_update_credentials(index, groups);
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(errorconv_dbg(status));
	}
}

void release(Context ctx, Inode ino, FileInfo *fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);

	stats_inc(OP_RELEASE);
	if (debug_mode) {
		oplog_printf(ctx, "release (%lu) ...",
				(unsigned long int)ino);
		fprintf(stderr,"release (%lu)\n",(unsigned long int)ino);
	}

	if (IS_SPECIAL_INODE(ino)) {
		special_release(ino, ctx, fi);
		return;
	}

	if (fileinfo != NULL){
		if (fileinfo->use_flocks) {
			fs_flock_send(ino, fi->lock_owner, 0, lzfs_locks::kRelease);
			fileinfo->use_flocks = false;
		}
		fileinfo->use_posixlocks = false;
		remove_file_info(fi);
	}
	fs_release(ino);
	oplog_printf(ctx, "release (%lu): OK",
			(unsigned long int)ino);
}

std::vector<uint8_t> read_special_inode(Context ctx,
			Inode ino,
			size_t size,
			off_t off,
			FileInfo* fi) {
	LOG_AVG_TILL_END_OF_SCOPE0("read");
	stats_inc(OP_READ);

	return special_read(ino, ctx, size, off, fi, debug_mode);
}

ReadCache::Result read(Context ctx,
			Inode ino,
			size_t size,
			off_t off,
			FileInfo *fi) {
	LOG_AVG_TILL_END_OF_SCOPE0("read");
	stats_inc(OP_READ);

	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);
	int err;
	ReadCache::Result ret;
	if (debug_mode) {
		fprintf(stderr,"read from inode %lu up to %" PRIu64 " bytes from position %" PRIu64 "\n",
		                (unsigned long int)ino,
		                (uint64_t)size,
		                (uint64_t)off);
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(EBADF));
		throw RequestException(EBADF);
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(EFBIG));
		throw RequestException(EFBIG);
	}
	try {
		const SteadyTimePoint deadline = SteadyClock::now() + std::chrono::seconds(30);
		uint8_t status = gLocalIoLimiter().waitForRead(ctx.pid, size, deadline);
		if (status == LIZARDFS_STATUS_OK) {
			status = gGlobalIoLimiter().waitForRead(ctx.pid, size, deadline);
		}
		if (status != LIZARDFS_STATUS_OK) {
			err = (status == LIZARDFS_ERROR_EPERM ? EPERM : EIO);
			oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
					(unsigned long int)ino,
					(uint64_t)size,
					(uint64_t)off,
					strerr(err));
			throw RequestException(err);
		}
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "I/O limiting error: %s", ex.what());
		throw RequestException(EIO);
	}
	PthreadMutexWrapper lock(fileinfo->lock);
	PthreadMutexWrapper flushlock(fileinfo->flushlock);
	if (fileinfo->mode==IO_WRITEONLY) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(EACCES));
		throw RequestException(EACCES);
	}
	if (fileinfo->mode==IO_WRITE) {
		err = write_data_flush(fileinfo->data);
		if (err!=0) {
			if (debug_mode) {
				fprintf(stderr,"IO error occurred while writing inode %lu\n",
						(unsigned long int)ino);
			}
			oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
					(unsigned long int)ino,
					(uint64_t)size,
					(uint64_t)off,
					strerr(err));
			throw RequestException(err);
		}
		write_data_end(fileinfo->data);
	}
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_READ;
		fileinfo->data = read_data_new(ino);
	}
	// end of reader critical section
	flushlock.unlock();

	write_data_flush_inode(ino);

	uint64_t firstBlockToRead = off / MFSBLOCKSIZE;
	uint64_t firstBlockNotToRead = (off + size + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE;
	uint64_t alignedOffset = firstBlockToRead * MFSBLOCKSIZE;
	uint64_t alignedSize = (firstBlockNotToRead - firstBlockToRead) * MFSBLOCKSIZE;

	uint32_t ssize = alignedSize;

	err = read_data(fileinfo->data, alignedOffset, ssize, ret);
	ssize = ret.requestSize(alignedOffset, ssize);
	if (err != 0) {
		if (debug_mode) {
			fprintf(stderr,"IO error occurred while reading inode %lu\n",
					(unsigned long int)ino);
		}
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(err));
		throw RequestException(err);
	} else {
		uint32_t replyOffset = off - alignedOffset;
		if (ssize > replyOffset) {
			ssize -= replyOffset;
			if (ssize > size) {
				ssize = size;
			}
		} else {
			ssize = 0;
		}
		if (debug_mode) {
			fprintf(stderr,"%" PRIu32 " bytes have been read from inode %lu\n",
					ssize,
					(unsigned long int)ino);
		}
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				(unsigned long int)ssize);
	}
	return ret;
}

BytesWritten write(Context ctx, Inode ino, const char *buf, size_t size, off_t off,
			FileInfo *fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);
	int err;

	stats_inc(OP_WRITE);
	if (debug_mode) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 ") ...",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off);
		fprintf(stderr,"write to inode %lu %" PRIu64 " bytes at position %" PRIu64 "\n",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off);
	}

	if (IS_SPECIAL_INODE(ino)) {
		return special_write(ino, ctx, buf, size, off, fi);
	}

	if (fileinfo==NULL) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(EBADF));
		throw RequestException(EBADF);
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(EFBIG));
		throw RequestException(EFBIG);
	}
	try {
		const SteadyTimePoint deadline = SteadyClock::now() + std::chrono::seconds(30);
		uint8_t status = gLocalIoLimiter().waitForWrite(ctx.pid, size, deadline);
		if (status == LIZARDFS_STATUS_OK) {
			status = gGlobalIoLimiter().waitForWrite(ctx.pid, size, deadline);
		}
		if (status != LIZARDFS_STATUS_OK) {
			err = (status == LIZARDFS_ERROR_EPERM ? EPERM : EIO);
			oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): %s",
							(unsigned long int)ino,
							(uint64_t)size,
							(uint64_t)off,
							strerr(err));
			throw RequestException(err);
		}
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "I/O limiting error: %s", ex.what());
		throw RequestException(EIO);
	}
	PthreadMutexWrapper lock(fileinfo->lock);
	if (fileinfo->mode==IO_READONLY) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(EACCES));
		throw RequestException(EACCES);
	}
	if (fileinfo->mode==IO_READ) {
		read_data_end(fileinfo->data);
		fileinfo->data = NULL;
	}
	if (fileinfo->mode==IO_READ || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_WRITE;
		fileinfo->data = write_data_new(ino);
	}
	err = write_data(fileinfo->data,off,size,(const uint8_t*)buf);
	if (err!=0) {
		if (debug_mode) {
			fprintf(stderr,"IO error occurred while writing inode %lu\n",(unsigned long int)ino);
		}
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				strerr(err));
		throw RequestException(err);
	} else {
		if (debug_mode) {
			fprintf(stderr,"%" PRIu64 " bytes have been written to inode %lu\n",
					(uint64_t)size,
					(unsigned long int)ino);
		}
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				(unsigned long int)size);
		return size;
	}
}

void flush(Context ctx, Inode ino, FileInfo* fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);
	int err;

	stats_inc(OP_FLUSH);
	if (debug_mode) {
		oplog_printf(ctx, "flush (%lu) ...",
				(unsigned long int)ino);
		fprintf(stderr,"flush (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "flush (%lu): OK",
				(unsigned long int)ino);
		return;
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx, "flush (%lu): %s",
				(unsigned long int)ino,
				strerr(EBADF));
		throw RequestException(EBADF);
	}
//      lzfs_pretty_syslog(LOG_NOTICE,"remove_locks inode:%lu owner:%" PRIu64 "",(unsigned long int)ino,(uint64_t)fi->lock_owner);
	err = 0;
	PthreadMutexWrapper lock(fileinfo->lock);
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	lzfs_locks::FlockWrapper file_lock(lzfs_locks::kRelease,0,0,0);
	auto use_posixlocks = fileinfo->use_posixlocks;
	lock.unlock();
	if (use_posixlocks) {
		fs_setlk_send(ino, fi->lock_owner, 0, file_lock);
	}
	if (err!=0) {
		oplog_printf(ctx, "flush (%lu): %s",
				(unsigned long int)ino,
				strerr(err));
		throw RequestException(err);
	} else {
		oplog_printf(ctx, "flush (%lu): OK",
				(unsigned long int)ino);
	}
}

void fsync(Context ctx, Inode ino, int datasync, FileInfo* fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);
	int err;

	stats_inc(OP_FSYNC);
	if (debug_mode) {
		oplog_printf(ctx, "fsync (%lu,%d) ...",
				(unsigned long int)ino,
				datasync);
		fprintf(stderr,"fsync (%lu,%d)\n",(unsigned long int)ino,datasync);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "fsync (%lu,%d): OK",
				(unsigned long int)ino,
				datasync);
		return;
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx, "fsync (%lu,%d): %s",
				(unsigned long int)ino,
				datasync,
				strerr(EBADF));
		throw RequestException(EBADF);
	}
	err = 0;
	PthreadMutexWrapper lock(fileinfo->lock);
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	if (err!=0) {
		oplog_printf(ctx, "fsync (%lu,%d): %s",
				(unsigned long int)ino,
				datasync,
				strerr(err));
		throw RequestException(err);
	} else {
		oplog_printf(ctx, "fsync (%lu,%d): OK",
				(unsigned long int)ino,
				datasync);
	}
}

namespace {

class XattrHandler {
public:
	virtual ~XattrHandler() {}

	/*
	 * handler for request to set an extended attribute
	 * mode - one of XATTR_SMODE_*
	 * returns status
	 */
	virtual uint8_t setxattr(const Context& ctx, Inode ino, const char *name,
			uint32_t nleng, const char *value, size_t size, int mode) = 0;

	/*
	 * handler for request to get an extended attribute
	 * mode - one of XATTR_GMODE_*
	 * returns status and:
	 * * sets value is mode is XATTR_GMODE_GET_DATA
	 * * sets valueLength is mode is XATTR_GMODE_LENGTH_ONLY
	 */
	virtual uint8_t getxattr(const Context& ctx, Inode ino, const char *name,
			uint32_t nleng, int mode, uint32_t& valueLength, std::vector<uint8_t>& value) = 0;

	/*
	 * handler for request to remove an extended attribute
	 * returns status
	 */
	virtual uint8_t removexattr(const Context& ctx, Inode ino, const char *name,
			uint32_t nleng) = 0;
};

class PlainXattrHandler : public XattrHandler {
public:
	virtual uint8_t setxattr(const Context& ctx, Inode ino, const char *name,
			uint32_t nleng, const char *value, size_t size, int mode) {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_setxattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name,
				(uint32_t)size, (const uint8_t*)value, mode));
		return status;
	}

	virtual uint8_t getxattr(const Context& ctx, Inode ino, const char *name,
			uint32_t nleng, int mode, uint32_t& valueLength, std::vector<uint8_t>& value) {
		const uint8_t *buff;
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_getxattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name,
				mode, &buff, &valueLength));
		if (mode == XATTR_GMODE_GET_DATA && status == LIZARDFS_STATUS_OK) {
			value = std::vector<uint8_t>(buff, buff + valueLength);
		}
		return status;
	}

	virtual uint8_t removexattr(const Context& ctx, Inode ino, const char *name,
			uint32_t nleng) {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_removexattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name));
		return status;
	}
};

class ErrorXattrHandler : public XattrHandler {
public:
	ErrorXattrHandler(uint8_t error) : error_(error) {}
	virtual uint8_t setxattr(const Context&, Inode, const char *,
			uint32_t, const char *, size_t, int) {
		return error_;
	}

	virtual uint8_t getxattr(const Context&, Inode, const char *,
			uint32_t, int, uint32_t&, std::vector<uint8_t>&) {
		return error_;
	}

	virtual uint8_t removexattr(const Context&, Inode, const char *,
			uint32_t) {
		return error_;
	}
private:
	uint8_t error_;
};

class AclXattrHandler : public XattrHandler {
public:
	AclXattrHandler(AclType type) : type_(type) { }

	virtual uint8_t setxattr(const Context& ctx, Inode ino, const char *,
			uint32_t, const char *value, size_t size, int) {
		static constexpr size_t kEmptyAclSize = 4;
		if (!acl_enabled) {
			return LIZARDFS_ERROR_ENOTSUP;
		}
		AccessControlList acl;
		try {
			if (size <= kEmptyAclSize) {
				uint8_t status;
				RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
					fs_deletacl(ino, ctx.uid, ctx.gid, type_));
				return status;
			}
			acl = aclConverter::extractAclObject((const uint8_t*)value, size);
		} catch (Exception&) {
			return LIZARDFS_ERROR_EINVAL;
		}
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_setacl(ino, ctx.uid, ctx.gid, type_, acl));
		eraseAclCache(ino);
		gDirEntryCache.lockAndInvalidateInode(ino);
		return status;
	}

	virtual uint8_t getxattr(const Context& ctx, Inode ino, const char *,
			uint32_t, int /*mode*/, uint32_t& valueLength, std::vector<uint8_t>& value) {
		if (!acl_enabled) {
			return LIZARDFS_ERROR_ENOTSUP;
		}

		try {
			AclCacheEntry cacheEntry = acl_cache->get(clock_.now(), ino, ctx.uid, ctx.gid, type_);
			if (cacheEntry) {
				value = aclConverter::aclObjectToXattr(*cacheEntry);
				valueLength = value.size();
				return LIZARDFS_STATUS_OK;
			} else {
				return LIZARDFS_ERROR_ENOATTR;
			}
		} catch (AclAcquisitionException& e) {
			sassert((e.status() != LIZARDFS_STATUS_OK) && (e.status() != LIZARDFS_ERROR_ENOATTR));
			return e.status();
		} catch (Exception&) {
			lzfs_pretty_syslog(LOG_WARNING, "Failed to convert ACL to xattr, looks like a bug");
			return LIZARDFS_ERROR_IO;
		}
	}

	virtual uint8_t removexattr(const Context& ctx, Inode ino, const char *,
			uint32_t) {
		if (!acl_enabled) {
			return LIZARDFS_ERROR_ENOTSUP;
		}
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
			fs_deletacl(ino, ctx.uid, ctx.gid, type_));
		eraseAclCache(ino);
		return status;
	}

private:
	AclType type_;
	SteadyClock clock_;
};

} // anonymous namespace

static AclXattrHandler accessAclXattrHandler(AclType::kAccess);
static AclXattrHandler defaultAclXattrHandler(AclType::kDefault);
static ErrorXattrHandler enotsupXattrHandler(LIZARDFS_ERROR_ENOTSUP);
static PlainXattrHandler plainXattrHandler;

static std::map<std::string, XattrHandler*> xattr_handlers = {
	{"system.posix_acl_access", &accessAclXattrHandler},
	{"system.posix_acl_default", &defaultAclXattrHandler},
	{"security.capability", &enotsupXattrHandler},
};

static XattrHandler* choose_xattr_handler(const char *name) {
	try {
		return xattr_handlers.at(name);
	} catch (std::out_of_range&) {
		return &plainXattrHandler;
	}
}

void setxattr(Context ctx, Inode ino, const char *name, const char *value,
			size_t size, int flags, uint32_t position) {
	uint32_t nleng;
	int status;
	uint8_t mode;


	stats_inc(OP_SETXATTR);
	if (debug_mode) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d) ...",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags);
		fprintf(stderr,"setxattr (%lu,%s,%" PRIu64 ",%d)",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(EPERM));
		throw RequestException(EPERM);
	}
	if (size>MFS_XATTR_SIZE_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns E2BIG here
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(E2BIG));
		throw RequestException(E2BIG);
#else
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(ERANGE));
		throw RequestException(ERANGE);
#endif
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(EPERM));
		throw RequestException(EPERM);
#else
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(ERANGE));
		throw RequestException(ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(EINVAL));
		throw RequestException(EINVAL);
	}
	if (strcmp(name,"security.capability")==0) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(ENOTSUP));
		throw RequestException(ENOTSUP);
	}
#if defined(XATTR_CREATE) && defined(XATTR_REPLACE)
	if ((flags&XATTR_CREATE) && (flags&XATTR_REPLACE)) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(EINVAL));
		throw RequestException(EINVAL);
	}
	mode = (flags==XATTR_CREATE)?XATTR_SMODE_CREATE_ONLY:(flags==XATTR_REPLACE)?XATTR_SMODE_REPLACE_ONLY:XATTR_SMODE_CREATE_OR_REPLACE;
#else
	mode = 0;
#endif
	(void)position;
	status = choose_xattr_handler(name)->setxattr(ctx, ino, name, nleng, value, size, mode);
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				strerr(status));
		throw RequestException(status);
	}
	oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): OK",
			(unsigned long int)ino,
			name,
			(uint64_t)size,
			flags);
}

XattrReply getxattr(Context ctx, Inode ino, const char *name, size_t size, uint32_t position) {
	uint32_t nleng;
	int status;
	uint8_t mode;
	std::vector<uint8_t> buffer;
	const uint8_t *buff;
	uint32_t leng;


	stats_inc(OP_GETXATTR);
	if (debug_mode) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 ") ...",
				(unsigned long int)ino,
				name,
				(uint64_t)size);
		fprintf(stderr,"getxattr (%lu,%s,%" PRIu64 ")",(unsigned long int)ino,name,(uint64_t)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				strerr(ENODATA));
		throw RequestException(ENODATA);
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				strerr(EPERM));
		throw RequestException(EPERM);
#else
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				strerr(ERANGE));
		throw RequestException(ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				strerr(EINVAL));
		throw RequestException(EINVAL);
	}
	if (strcmp(name,"security.capability")==0) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				strerr(ENOTSUP));
		throw RequestException(ENOTSUP);
	}
	if (size==0) {
		mode = XATTR_GMODE_LENGTH_ONLY;
	} else {
		mode = XATTR_GMODE_GET_DATA;
	}
	(void)position;
	status = choose_xattr_handler(name)->getxattr(ctx, ino, name, nleng, mode, leng, buffer);
	buff = buffer.data();
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				strerr(status));
		throw RequestException(status);
	}
	if (size==0) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): OK (%" PRIu32 ")",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				leng);
		return XattrReply{leng, {}};
	} else {
		if (leng>size) {
			oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
					(unsigned long int)ino,
					name,
					(uint64_t)size,
					strerr(ERANGE));
			throw RequestException(ERANGE);
		} else {
			oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): OK (%" PRIu32 ")",
					(unsigned long int)ino,
					name,
					(uint64_t)size,
					leng);
			return XattrReply{leng, std::vector<uint8_t>(buff, buff + leng)};
		}
	}
}

XattrReply listxattr(Context ctx, Inode ino, size_t size) {
	const uint8_t *buff;
	uint32_t leng;
	int status;
	uint8_t mode;

	stats_inc(OP_LISTXATTR);
	if (debug_mode) {
		oplog_printf(ctx, "listxattr (%lu,%" PRIu64 ") ...",
				(unsigned long int)ino,
				(uint64_t)size);
		fprintf(stderr,"listxattr (%lu,%" PRIu64 ")",(unsigned long int)ino,(uint64_t)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				strerr(EPERM));
		throw RequestException(EPERM);
	}
	if (size==0) {
		mode = XATTR_GMODE_LENGTH_ONLY;
	} else {
		mode = XATTR_GMODE_GET_DATA;
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx.gid,
		fs_listxattr(ino,0,ctx.uid,ctx.gid,mode,&buff,&leng));
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				strerr(status));
		throw RequestException(status);
	}
	if (size==0) {
		oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): OK (%" PRIu32 ")",
				(unsigned long int)ino,
				(uint64_t)size,
				leng);
		return XattrReply{leng, {}};
	} else {
		if (leng>size) {
			oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): %s",
					(unsigned long int)ino,
					(uint64_t)size,
					strerr(ERANGE));
			throw RequestException(ERANGE);
		} else {
			oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): OK (%" PRIu32 ")",
					(unsigned long int)ino,
					(uint64_t)size,
					leng);
			return XattrReply{leng, std::vector<uint8_t>(buff, buff + leng)};
		}
	}
}

void removexattr(Context ctx, Inode ino, const char *name) {
	uint32_t nleng;
	int status;

	stats_inc(OP_REMOVEXATTR);
	if (debug_mode) {
		oplog_printf(ctx, "removexattr (%lu,%s) ...",
				(unsigned long int)ino,
				name);
		fprintf(stderr,"removexattr (%lu,%s)",(unsigned long int)ino,name);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				strerr(EPERM));
		throw RequestException(EPERM);
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				strerr(EPERM));
		throw RequestException(EPERM);
#else
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				strerr(ERANGE));
		throw RequestException(ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				strerr(EINVAL));
		throw RequestException(EINVAL);
	}
	status = choose_xattr_handler(name)->removexattr(ctx, ino, name, nleng);
	status = errorconv_dbg(status);
	if (status!=0) {
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				strerr(status));
		throw RequestException(status);
	} else {
		oplog_printf(ctx, "removexattr (%lu,%s): OK",
				(unsigned long int)ino,
				name);
	}
}

void flock_interrupt(const lzfs_locks::InterruptData &data) {
	fs_flock_interrupt(data);
}

void setlk_interrupt(const lzfs_locks::InterruptData &data) {
	fs_setlk_interrupt(data);
}

void getlk(Context ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock) {
	uint32_t status;

	stats_inc(OP_FLOCK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(ctx, "flock(ctx, %lu, fi): %s", (unsigned long int)ino, strerr(EPERM));
			fprintf(stderr, "flock(ctx, %lu, fi): %s\n", (unsigned long int)ino, strerr(EPERM));
		}
		throw RequestException(EINVAL);
	}

	if (!fi) {
		if (debug_mode) {
			oplog_printf(ctx,"flock(ctx, %lu, fi): %s",(unsigned long int)ino, strerr(EPERM));
			fprintf(stderr,"flock(ctx, %lu, fi): %s\n",(unsigned long int)ino, strerr(EPERM));
		}
		throw RequestException(EINVAL);
	}

	// communicate with master
	status = fs_getlk(ino, fi->lock_owner, lock);

	if (status) {
		status = mfs_errorconv(status);
		throw RequestException(status);
	}
}

uint32_t setlk_send(Context ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock) {
	uint32_t reqid;
	uint32_t status;
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);

	stats_inc(OP_SETLK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(ctx, "flock(ctx, %lu, fi): %s", (unsigned long int)ino, strerr(EPERM));
			fprintf(stderr, "flock(ctx, %lu, fi): %s\n", (unsigned long int)ino, strerr(EPERM));
		}
		throw RequestException(EINVAL);
	}

	if (!fi) {
		if (debug_mode) {
			oplog_printf(ctx,"flock(ctx, %lu, fi): %s",(unsigned long int)ino, strerr(EPERM));
			fprintf(stderr,"flock(ctx, %lu, fi): %s\n",(unsigned long int)ino, strerr(EPERM));
		}
		throw RequestException(EINVAL);
	}

	// increase flock_id counter
	lock_request_mutex.lock();
	reqid = lock_request_counter++;
	lock_request_mutex.unlock();

	if (fileinfo != NULL) {
		PthreadMutexWrapper lock(fileinfo->lock);
		fileinfo->use_posixlocks = true;
	}

	// communicate with master
	status = fs_setlk_send(ino, fi->lock_owner, reqid, lock);

	if (status) {
		status = mfs_errorconv(status);
		throw RequestException(status);
	}

	return reqid;
}

void setlk_recv() {
	uint32_t status = fs_setlk_recv();

	if (status) {
		status = mfs_errorconv(status);
		throw RequestException(status);
	}
}

uint32_t flock_send(Context ctx, Inode ino, FileInfo* fi, int op) {
	uint32_t reqid;
	uint32_t status;
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);

	stats_inc(OP_FLOCK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(ctx, "flock(ctx, %lu, fi): %s", (unsigned long int)ino, strerr(EPERM));
			fprintf(stderr, "flock(ctx, %lu, fi): %s\n", (unsigned long int)ino, strerr(EPERM));
		}
		throw RequestException(EINVAL);
	}

	if (!fi) {
		if (debug_mode) {
			oplog_printf(ctx,"flock(ctx, %lu, fi): %s",(unsigned long int)ino, strerr(EPERM));
			fprintf(stderr,"flock(ctx, %lu, fi): %s\n",(unsigned long int)ino, strerr(EPERM));
		}
		throw RequestException(EINVAL);
	}

	// increase flock_id counter
	lock_request_mutex.lock();
	reqid = lock_request_counter++;
	lock_request_mutex.unlock();

	if (fileinfo != NULL) {
		PthreadMutexWrapper lock(fileinfo->lock);
		fileinfo->use_flocks = true;
	}

	// communicate with master
	status = fs_flock_send(ino, fi->lock_owner, reqid, op);

	if (status) {
		status = mfs_errorconv(status);
		throw RequestException(status);
	}

	return reqid;
}

void flock_recv() {
	uint32_t status = fs_flock_recv();

	if (status) {
		status = mfs_errorconv(status);
		throw RequestException(status);
	}
}

void init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_, unsigned direntry_cache_size_,
		double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
		SugidClearMode sugid_clear_mode_, bool acl_enabled_, bool use_rwlock_,
		double acl_cache_timeout_, unsigned acl_cache_size_) {
	const char* sugid_clear_mode_strings[] = {SUGID_CLEAR_MODE_STRINGS};
	debug_mode = debug_mode_;
	keep_cache = keep_cache_;
	direntry_cache_timeout = direntry_cache_timeout_;
	entry_cache_timeout = entry_cache_timeout_;
	attr_cache_timeout = attr_cache_timeout_;
	mkdir_copy_sgid = mkdir_copy_sgid_;
	sugid_clear_mode = static_cast<decltype (sugid_clear_mode)>(sugid_clear_mode_);
	acl_enabled = acl_enabled_;
	use_rwlock = use_rwlock_;
	uint64_t timeout = (uint64_t)(direntry_cache_timeout * 1000000);
	gDirEntryCache.setTimeout(timeout);
	gDirEntryCacheMaxSize = direntry_cache_size_;
	if (debug_mode) {
		fprintf(stderr,"cache parameters: file_keep_cache=%s direntry_cache_timeout=%.2f entry_cache_timeout=%.2f attr_cache_timeout=%.2f\n",(keep_cache==1)?"always":(keep_cache==2)?"never":"auto",direntry_cache_timeout,entry_cache_timeout,attr_cache_timeout);
		fprintf(stderr,"mkdir copy sgid=%d\nsugid clear mode=%s\n",mkdir_copy_sgid_,(sugid_clear_mode<SUGID_CLEAR_MODE_OPTIONS)?sugid_clear_mode_strings[sugid_clear_mode]:"???");
		fprintf(stderr, "ACL support %s\n", acl_enabled ? "enabled" : "disabled");
		fprintf(stderr, "RW lock %s\n", use_rwlock ? "enabled" : "disabled");
		fprintf(stderr, "ACL acl_cache_timeout=%.2f, acl_cache_size=%u\n",
				acl_cache_timeout_, acl_cache_size_);
	}
	statsptr_init();

	acl_cache.reset(new AclCache(
			std::chrono::milliseconds((int)(1000 * acl_cache_timeout_)),
			acl_cache_size_,
			getAcl));

	gTweaks.registerVariable("DirectIO", gDirectIo);
	gTweaks.registerVariable("AclCacheMaxTime", acl_cache->maxTime_ms);
	gTweaks.registerVariable("AclCacheHit", acl_cache->cacheHit);
	gTweaks.registerVariable("AclCacheExpired", acl_cache->cacheExpired);
	gTweaks.registerVariable("AclCacheMiss", acl_cache->cacheMiss);
}

} // namespace LizardClient
