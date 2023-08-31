/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2019 Skytechnology sp. z o.o.

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

#include <atomic>
#include <new>
#include <memory>
#include <vector>
#include <list>
#include <map>
#include <unordered_map>
#include <string>
#include <fstream>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <unistd.h>

#include "common/access_control_list.h"
#include "common/acl_converter.h"
#include "common/acl_type.h"
#include "common/crc.h"
#include "common/datapack.h"
#include "common/errno_defs.h"
#include "common/lru_cache.h"
#include "common/mfserr.h"
#include "common/richacl_converter.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/special_inode_defs.h"
#include "common/time_utils.h"
#include "common/user_groups.h"
#include "devtools/request_log.h"
#include "mount/acl_cache.h"
#include "mount/chunk_locator.h"
#include "mount/client_common.h"
#include "mount/direntry_cache.h"
#include "mount/g_io_limiters.h"
#include "mount/io_limit_group.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/oplog.h"
#include "mount/readdata.h"
#include "mount/special_inode.h"
#include "mount/stats.h"
#include "mount/sugid_clear_mode_string.h"
#include "mount/symlinkcache.h"
#include "mount/tweaks.h"
#include "mount/writedata.h"
#include "protocol/MFSCommunication.h"
#include "protocol/matocl.h"

#include "client/crash-log.h"

#ifdef __APPLE__
#include "mount/osx_acl_converter.h"
#endif

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

struct ReaddirSession {
	uint64_t lastReadIno;
	std::atomic<bool> restarted;
	ReaddirSession(uint64_t ino = 0)
		: lastReadIno(ino)
		, restarted(false) {
	}
};

using ReaddirSessions = std::map<std::uint64_t, ReaddirSession>;

std::mutex gReaddirMutex;
inline ReaddirSessions gReaddirSessions;

static void update_credentials(Context::IdType index, const GroupCache::Groups &groups);
static void registerGroupsInMaster(Context &ctx);

#define RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, context, function_expression) \
		do { \
			status = function_expression; \
			if (status == LIZARDFS_ERROR_GROUPNOTREGISTERED) { \
				registerGroupsInMaster(context); \
				status = function_expression; \
			} \
		} while (0);

void updateGroups(Context &ctx) {
	if (ctx.gids.empty()) {
		return;
	}

    if (ctx.gids.size() == 1) {
		ctx.gid = ctx.gids[0];
		return;
	}

	static_assert(sizeof(Context::IdType) >= sizeof(uint32_t), "IdType too small");

	auto result = gGroupCache.find(ctx.gids);
	Context::IdType gid = 0;
	if (result.found == false) {
		try {
			uint32_t index = gGroupCache.put(ctx.gids);
			update_credentials(index, ctx.gids);
			gid = user_groups::encodeGroupCacheId(index);
		} catch (RequestException &e) {
			lzfs_pretty_syslog(LOG_ERR, "Cannot update groups: %d", e.system_error_code);
		}
	} else {
        //gid = user_groups::encodeGroupCacheId(result.index);
        // testing the overflow:
        /*uint32_t v = result.index;
        uint64_t b = (uint32_t)1 << (uint32_t)31;
        uint64_t res = v | b;
        crashLog("overflow: v: %d b: %llu result: %llu Line: %d",
                 v, b, res, __LINE__);
        crashLog("lizard_client updated gid: %d Line: %d", gid, __LINE__);*/
	}

    //ctx.gid = gid;
    /*crashLog("lizard_client updateGroups ctx.uid: %d ctx.gid: %d exit Line: %d",
             ctx.uid, ctx.gid, __LINE__);*/
}

static void registerGroupsInMaster(Context &ctx) {
	std::uint32_t index = user_groups::decodeGroupCacheId(ctx.gid);
	GroupCache::Groups groups = gGroupCache.findByIndex(index);
	if (!groups.empty()) {
		update_credentials(index, groups);
	} else {
		updateGroups(ctx);
	}
}

Inode getSpecialInodeByName(const char *name) {
	assert(name);

	while (name[0] == '/') {
		++name;
	}

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
bool use_rwlock = 0;
static std::atomic<bool> gDirectIo(false);

// lock_request_counter shared by flock and setlk
static uint32_t lock_request_counter = 0;
static std::mutex lock_request_mutex;

static std::unique_ptr<AclCache> acl_cache;

void update_readdir_session(uint64_t sessId, uint64_t entryIno) {
	std::lock_guard<std::mutex> sessions_lock(gReaddirMutex);
	gReaddirSessions[sessId].lastReadIno = entryIno;
}

void drop_readdir_session(uint64_t opendirSessionID) {
	std::lock_guard<std::mutex> sessions_lock(gReaddirMutex);
	gReaddirSessions.erase(opendirSessionID);
}

static void updateNextReaddirEntryIndexIfMasterRestarted(ReaddirSession& readdirSession, uint64_t &nextEntryIndex,
		Context &ctx, Inode parentInode, uint64_t requestSize) {
	if (!readdirSession.restarted) {
		return;
    }
	std::vector<DirectoryEntry> dirEntries;
	uint8_t status = 0;
	nextEntryIndex = 0;
	while (true) {
		dirEntries.clear();
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(
			status, ctx,
			fs_getdir(parentInode, ctx.uid, ctx.gid, nextEntryIndex, requestSize, dirEntries)
		);
		if (dirEntries.empty()) {
			break;
		}
		std::vector<DirectoryEntry>::const_iterator direntIt = find_if(
				dirEntries.cbegin(),
				dirEntries.cend(),
				[&readdirSession](DirectoryEntry const& de) {
					return (de.inode == readdirSession.lastReadIno);
				}
			);
		if (direntIt != dirEntries.end()) {
			nextEntryIndex = direntIt->index;
			dirEntries.clear();
			break;
		}
		nextEntryIndex = dirEntries.back().next_index;
	}
	readdirSession.restarted = false;
}

void masterDisconnectedCallback() {
	gGroupCache.reset();
	gDirEntryCache.clear();
	std::lock_guard<std::mutex> sessions_lock(gReaddirMutex);
	for (auto& rs : gReaddirSessions) {
		rs.second.restarted = true;
	}
}

inline void eraseAclCache(Inode inode) {
	acl_cache->erase(
			inode    , 0, 0,
			inode + 1, 0, 0);
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

void statsptr_init(void) {
	statsnode *s;
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
	statsptr[OP_READRESERVED] = stats_get_counterptr(stats_get_subnode(s,"readreserved",0));
	statsptr[OP_READTRASH] = stats_get_counterptr(stats_get_subnode(s,"readtrash",0));
	statsptr[OP_OPENDIR] = stats_get_counterptr(stats_get_subnode(s,"opendir",0));
	statsptr[OP_LINK] = stats_get_counterptr(stats_get_subnode(s,"link",0));
	statsptr[OP_RENAME] = stats_get_counterptr(stats_get_subnode(s,"rename",0));
	statsptr[OP_READLINK] = stats_get_counterptr(stats_get_subnode(s,"readlink",0));
	statsptr[OP_READLINK_CACHED] = stats_get_counterptr(stats_get_subnode(s,"readlink-cached",0));
	statsptr[OP_SYMLINK] = stats_get_counterptr(stats_get_subnode(s,"symlink",0));
	statsptr[OP_RMDIR] = stats_get_counterptr(stats_get_subnode(s,"rmdir",0));
	statsptr[OP_MKDIR] = stats_get_counterptr(stats_get_subnode(s,"mkdir",0));
	statsptr[OP_UNLINK] = stats_get_counterptr(stats_get_subnode(s,"unlink",0));
	statsptr[OP_UNDEL] = stats_get_counterptr(stats_get_subnode(s,"undel",0));
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

RequestException::RequestException(int error_code) : system_error_code(), lizardfs_error_code() {
	assert(error_code != LIZARDFS_STATUS_OK);

	lizardfs_error_code = error_code;
	system_error_code = lizardfs_error_conv(error_code);
	if (debug_mode) {
		lzfs::log_debug("status: {}", lizardfs_error_string(error_code));
	}
}

struct statvfs statfs(Context &ctx, Inode ino) {
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

void access(Context &ctx, Inode ino, int mask) {
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
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
		return;
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_access(ino,ctx.uid,ctx.gid,mmode));
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}
}

EntryParam lookup(Context &ctx, Inode parent, const char *name) {
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
		oplog_printf(ctx, "lookup (%lu,%s) ...", (unsigned long int)parent, name);
	}
	nleng = strlen(name);
	if (nleng > MFS_NAME_MAX) {
		stats_inc(OP_LOOKUP);
		oplog_printf(ctx, "lookup (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
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
			throw RequestException(LIZARDFS_ERROR_EINVAL);
		}
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_getattr(inode, ctx.uid, ctx.gid, attr));
		icacheflag = 0;
	} else if (usedircache && gDirEntryCache.lookup(ctx,parent,std::string(name,nleng),inode,attr)) {
		if (debug_mode) {
			lzfs::log_debug("lookup: sending data from dircache");
		}
		stats_inc(OP_DIRCACHE_LOOKUP);
		status = 0;
		icacheflag = 1;
//              oplog_printf(ctx, "lookup (%lu,%s) (using open dir cache): OK (%lu)",(unsigned long int)parent,name,(unsigned long int)inode);
	} else {
		stats_inc(OP_LOOKUP);
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_lookup(parent, std::string(name, nleng), ctx.uid, ctx.gid, &inode, attr));
		icacheflag = 0;
	}
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "lookup (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				lizardfs_error_string(status));
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

AttrReply getattr(Context &ctx, Inode ino) {
	uint64_t maxfleng;
	double attr_timeout;
	struct stat o_stbuf;
	Attributes attr;
	char attrstr[256];
	int status;

	if (debug_mode) {
		oplog_printf(ctx, "getattr (%lu) ...", (unsigned long int)ino);
	}

	if (IS_SPECIAL_INODE(ino)) {
		return special_getattr(ino, ctx, attrstr);
	}

	maxfleng = write_data_getmaxfleng(ino);
	if (usedircache && gDirEntryCache.lookup(ctx,ino,attr)) {
		if (debug_mode) {
			lzfs::log_debug("getattr: sending data from dircache\n");
		}
		stats_inc(OP_DIRCACHE_GETATTR);
		status = LIZARDFS_STATUS_OK;
	} else {
		stats_inc(OP_GETATTR);
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_getattr(ino,ctx.uid,ctx.gid,attr));
	}
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "getattr (%lu): %s",
				(unsigned long int)ino,
				lizardfs_error_string(status));
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

AttrReply setattr(Context &ctx, Inode ino, struct stat *stbuf, int to_set) {
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
	}

	if (IS_SPECIAL_INODE(ino)) {
		return special_setattr(ino, ctx, stbuf, to_set, modestr, attrstr);
	}

	status = LIZARDFS_ERROR_EINVAL;
	maxfleng = write_data_getmaxfleng(ino);
	if ((to_set & (LIZARDFS_SET_ATTR_MODE
			| LIZARDFS_SET_ATTR_UID
			| LIZARDFS_SET_ATTR_GID
			| LIZARDFS_SET_ATTR_ATIME
			| LIZARDFS_SET_ATTR_ATIME_NOW
			| LIZARDFS_SET_ATTR_MTIME
			| LIZARDFS_SET_ATTR_MTIME_NOW
			| LIZARDFS_SET_ATTR_SIZE)) == 0) { // change other flags or change nothing
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_setattr(ino,ctx.uid,ctx.gid,0,0,0,0,0,0,0,attr));    // ext3 compatibility - change ctime during this operation (usually chown(-1,-1))
		if (status != LIZARDFS_STATUS_OK) {
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
					lizardfs_error_string(status));
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
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_setattr(ino,ctx.uid,ctx.gid,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,sugid_clear_mode,attr));
		if (to_set & (LIZARDFS_SET_ATTR_MODE | LIZARDFS_SET_ATTR_UID | LIZARDFS_SET_ATTR_GID)) {
			eraseAclCache(ino);
		}
		if (status != LIZARDFS_STATUS_OK) {
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
					lizardfs_error_string(status));
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
					lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
			throw RequestException(LIZARDFS_ERROR_EINVAL);
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
					lizardfs_error_string(LIZARDFS_ERROR_EFBIG));
			throw RequestException(LIZARDFS_ERROR_EFBIG);
		}
		try {
			RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
				write_data_truncate(ino, false, ctx.uid, ctx.gid, stbuf->st_size, attr));
			maxfleng = 0; // after the flush master server has valid length, don't use our length cache
		} catch (Exception& ex) {
			status = ex.status();
		}
		read_inode_ops(ino);
		if (status != LIZARDFS_STATUS_OK) {
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
					lizardfs_error_string(status));
			throw RequestException(status);
		}
	}
	if (status != LIZARDFS_STATUS_OK) {        // should never happen but better check than sorry
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
				lizardfs_error_string(status));
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

EntryParam mknod(Context &ctx, Inode parent, const char *name, mode_t mode, dev_t rdev) {
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
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): %s",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
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
				lizardfs_error_string(LIZARDFS_ERROR_EPERM));
		throw RequestException(LIZARDFS_ERROR_EPERM);
	}

	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): %s",
					(unsigned long int)parent,
					name,
					modestr,
					(unsigned int)mode,
					(unsigned long int)rdev,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,ctx.umask,ctx.uid,ctx.gid,rdev,inode,attr));
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "mknod (%lu,%s,%s:0%04o,0x%08lX): %s",
				(unsigned long int)parent,
				name,
				modestr,
				(unsigned int)mode,
				(unsigned long int)rdev,
				lizardfs_error_string(status));
		throw RequestException(status);
	} else {
		gDirEntryCache.lockAndInvalidateParent(ctx, parent);
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

void unlink(Context &ctx, Inode parent, const char *name) {
	uint32_t nleng;
	int status;

	stats_inc(OP_UNLINK);
	if (debug_mode) {
		oplog_printf(ctx, "unlink (%lu,%s) ...", (unsigned long int)parent, name);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "unlink (%lu,%s): %s",
					(unsigned long int)parent,
					name,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}

	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "unlink (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_unlink(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid));
	gDirEntryCache.lockAndInvalidateParent(parent);
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "unlink (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				lizardfs_error_string(status));
		throw RequestException(status);
	} else {
		oplog_printf(ctx, "unlink (%lu,%s): OK",
				(unsigned long int)parent,
				name);
		return;
	}
}

void undel(Context &ctx, Inode ino) {
	stats_inc(OP_UNDEL);
	if (debug_mode) {
		oplog_printf(ctx, "undel (%lu) ...", (unsigned long)ino);
	}
	uint8_t status;
	// FIXME(haze): modify undel to return parent inode and call gDirEntryCache.lockAndInvalidateParent(parent)
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx, fs_undel(ino));
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}
}

EntryParam mkdir(Context &ctx, Inode parent, const char *name, mode_t mode) {
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
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o): %s",
					(unsigned long int)parent,
					name,
					modestr+1,
					(unsigned int)mode,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_mkdir(parent,nleng,(const uint8_t*)name,mode,ctx.umask,ctx.uid,ctx.gid,mkdir_copy_sgid,inode,attr));
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "mkdir (%lu,%s,d%s:0%04o): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				lizardfs_error_string(status));
		throw RequestException(status);
	} else {
		gDirEntryCache.lockAndInvalidateParent(parent);
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

void rmdir(Context &ctx, Inode parent, const char *name) {
	uint32_t nleng;
	int status;

	stats_inc(OP_RMDIR);
	if (debug_mode) {
		oplog_printf(ctx, "rmdir (%lu,%s) ...", (unsigned long int)parent, name);
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "rmdir (%lu,%s): %s",
					(unsigned long int)parent,
					name,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "rmdir (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_rmdir(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid));
	gDirEntryCache.lockAndInvalidateParent(parent);
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "rmdir (%lu,%s): %s",
				(unsigned long int)parent,
				name,
				lizardfs_error_string(status));
		throw RequestException(status);
	} else {
		oplog_printf(ctx, "rmdir (%lu,%s): OK",
				(unsigned long int)parent,
				name);
		return;
	}
}

EntryParam symlink(Context &ctx, const char *path, Inode parent,
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
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "symlink (%s,%lu,%s): %s",
					path,
					(unsigned long int)parent,
					name,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "symlink (%s,%lu,%s): %s",
				path,
				(unsigned long int)parent,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)path,ctx.uid,ctx.gid,&inode,attr));
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "symlink (%s,%lu,%s): %s",
				path,
				(unsigned long int)parent,
				name,
				lizardfs_error_string(status));
		throw RequestException(status);
	} else {
		gDirEntryCache.lockAndInvalidateParent(parent);
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

std::string readlink(Context &ctx, Inode ino) {
	int status;
	const uint8_t *path;

	if (debug_mode) {
		oplog_printf(ctx, "readlink (%lu) ...",
				(unsigned long int)ino);
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
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "readlink (%lu): %s",
				(unsigned long int)ino,
				lizardfs_error_string(status));
		throw RequestException(status);
	} else {
		symlink_cache_insert(ino,path);
		oplog_printf(ctx, "readlink (%lu): OK (%s)",
				(unsigned long int)ino,
				(char*)path);
		return std::string((char*)path);
	}
}

void rename(Context &ctx, Inode parent, const char *name,
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
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
					(unsigned long int)parent,
					name,
					(unsigned long int)newparent,
					newname,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	if (newparent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
					(unsigned long int)parent,
					name,
					(unsigned long int)newparent,
					newname,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
	fs_rename(parent,nleng,(const uint8_t*)name,newparent,newnleng,(const uint8_t*)newname,ctx.uid,ctx.gid,&inode,attr));
	gDirEntryCache.lockAndInvalidateParent(parent);
	gDirEntryCache.lockAndInvalidateParent(newparent);
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "rename (%lu,%s,%lu,%s): %s",
				(unsigned long int)parent,
				name,
				(unsigned long int)newparent,
				newname,
				lizardfs_error_string(status));
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

EntryParam link(Context &ctx, Inode ino, Inode newparent, const char *newname) {
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
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "link (%lu,%lu,%s): %s",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname,
				lizardfs_error_string(LIZARDFS_ERROR_EACCES));
		throw RequestException(LIZARDFS_ERROR_EACCES);
	}
	if (newparent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(ctx, "link (%lu,%lu,%s): %s",
					(unsigned long int)ino,
					(unsigned long int)newparent,
					newname,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "link (%lu,%lu,%s): %s",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx.uid,ctx.gid,&inode,attr));
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "link (%lu,%lu,%s): %s",
				(unsigned long int)ino,
				(unsigned long int)newparent,
				newname,
				lizardfs_error_string(status));
		throw RequestException(status);
	} else {
		gDirEntryCache.lockAndInvalidateParent(newparent);
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

void opendir(Context &ctx, Inode ino) {
	int status;

	stats_inc(OP_OPENDIR);
	if (debug_mode) {
		oplog_printf(ctx, "opendir (%lu) ...", (unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "opendir (%lu): %s",
				(unsigned long int)ino,
				lizardfs_error_string(LIZARDFS_ERROR_ENOTDIR));
		throw RequestException(LIZARDFS_ERROR_ENOTDIR);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_access(ino,ctx.uid,ctx.gid,MODE_MASK_R));    // at least test rights
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "opendir (%lu): %s",
				(unsigned long int)ino,
				lizardfs_error_string(status));
		throw RequestException(status);
	}
}

/// List DirEntry objects in the directory described by \a ino inode.
/**
 * \param ino parent directory inode
 * \param off offset (index) of the first dir entry to list
 * \param max_entries max number of dir entries to list
 * \return std::vector of directory entries
 */
std::vector<DirEntry> readdir(Context &ctx, uint64_t fh, Inode ino, off_t off, size_t max_entries) {
	static constexpr int kBatchSize = 1000;
	const uint64_t start_off = static_cast<std::make_unsigned<off_t>::type>(off);
	// type to cast to should be the same size to avoid potential sign-extension
	// (LizardFS's offset can be interpreted as negative on signed integer types (e.g. off_t used by libfuse),
	// as it is 64bit unsigned int on master)

	stats_inc(OP_READDIR);
	if (debug_mode) {
		oplog_printf(ctx, "readdir (%lu,%" PRIu64 ",%" PRIu64 ") ...",
				static_cast<unsigned long int>(ino),
				static_cast<uint64_t>(max_entries),
				start_off);
	}

	// for more detailed oplogging
	size_t initial_max_entries = max_entries;
	size_t entries_from_cache = 0;
	size_t entries_from_master = 0;

	std::vector<DirEntry> result;
	shared_lock<shared_mutex> access_guard(gDirEntryCache.rwlock());
	gDirEntryCache.updateTime();

	uint64_t entry_index = start_off;
	auto it = gDirEntryCache.find(ctx, ino, entry_index);

	result.reserve(max_entries);

    for(; it != gDirEntryCache.index_end() && max_entries > 0; ++it) {
		if (!gDirEntryCache.isValid(it) || it->index != entry_index ||
				it->parent_inode != ino || it->uid != ctx.uid || it->gid != ctx.gid) {
			break;
		}

		if (it->inode == 0) {
			// we have valid 'no more entries' marker
			assert(it->name.empty());
			max_entries = 0;
			break;
		}

		entry_index = it->next_index;
		--max_entries;
		++entries_from_cache;

		struct stat stats;
		attr_to_stat(it->inode,it->attr,&stats);
		result.emplace_back(it->name, stats, entry_index); // nextEntryOffset = entry_index
	}

	if (max_entries == 0) {
		if (debug_mode) {
			oplog_printf(ctx, "readdir (%lu,%" PRIu64 ",%" PRIu64 ") returned %zu dirents all from direntrycache; index of next dirent is %" PRIu64
				" (%#" PRIx64 ")",
					static_cast<unsigned long int>(ino),
					static_cast<uint64_t>(initial_max_entries),
					start_off,
					entries_from_cache,
					entry_index,
					entry_index);
		}
		return result;
	}

	access_guard.unlock();

	std::vector<DirectoryEntry> dir_entries;
	uint8_t status;
	uint64_t request_size = std::min<std::size_t>(std::max<std::size_t>(kBatchSize, max_entries),
	                                              matocl::fuseGetDir::kMaxNumberOfDirectoryEntries);

	ReaddirSession* readdirSession(nullptr);
	/* Scope for lock guard. */ {
		std::lock_guard<std::mutex> sessions_guard(gReaddirMutex);
		ReaddirSessions::iterator sessionIt = gReaddirSessions.find(fh);
		sassert(sessionIt != gReaddirSessions.end());
		readdirSession = &sessionIt->second;
	}
	do {
		updateNextReaddirEntryIndexIfMasterRestarted(*readdirSession, entry_index, ctx, ino, request_size);
        status = fs_getdir(ino, ctx.uid, ctx.gid, entry_index, request_size, dir_entries);

		if (status == LIZARDFS_ERROR_GROUPNOTREGISTERED) {
			registerGroupsInMaster(ctx);
			updateNextReaddirEntryIndexIfMasterRestarted(*readdirSession, entry_index, ctx, ino, request_size);
			status = fs_getdir(ino, ctx.uid, ctx.gid, entry_index, request_size, dir_entries);
		}
	} while (readdirSession->restarted);

	auto data_acquire_time = gDirEntryCache.updateTime();

	if(status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}

	std::unique_lock<shared_mutex> write_guard(gDirEntryCache.rwlock());
	gDirEntryCache.updateTime();

	// dir_entries.front().index must be equal to entry_index
	gDirEntryCache.insertSequence(ctx, ino, dir_entries, data_acquire_time);

    if (dir_entries.size() < request_size) {
		// insert 'no more entries' marker
		auto marker_index = entry_index;
		if (!dir_entries.empty()) {
			marker_index = dir_entries.back().next_index;
		}
		gDirEntryCache.invalidate(ctx, ino, marker_index);
		gDirEntryCache.insert(ctx, ino, 0, marker_index, marker_index, "", Attributes{{}}, data_acquire_time);
	}

	if (gDirEntryCache.size() > gDirEntryCacheMaxSize) {
		gDirEntryCache.removeOldest(gDirEntryCache.size() - gDirEntryCacheMaxSize);
	}

	write_guard.unlock();

	for(auto it = dir_entries.begin(); it != dir_entries.end() && max_entries > 0; ++it) {
		--max_entries;
		entry_index = it->next_index;
		++entries_from_master;

		struct stat stats;
		attr_to_stat(it->inode,it->attributes,&stats);
		result.emplace_back(it->name, stats, it->next_index);

		if (debug_mode) {
			oplog_printf(ctx, "readdir (%lu ,%" PRIu64 ",%#" PRIx64 ") from master: entry index: %#" PRIx64 ", next: %#" PRIx64 ", name: %s",
					static_cast<unsigned long int>(ino),
					static_cast<uint64_t>(initial_max_entries),
					start_off,
					it->index,
					it->next_index,
					it->name.c_str());
		}
	}

	if (debug_mode) {
		oplog_printf(ctx, "readdir (%lu,%" PRIu64 ",%" PRIu64 ") returned %zu dirents (%zu from cache, %zu from master); index of next dirent is %" PRIu64
			" (%#" PRIx64 ")",
				static_cast<unsigned long int>(ino),
				static_cast<uint64_t>(initial_max_entries),
				start_off,
				result.size(),
				entries_from_cache,
				entries_from_master,
				entry_index,
				entry_index);
	}
	return result;
}

std::vector<NamedInodeEntry> readreserved(Context &ctx, NamedInodeOffset off, NamedInodeOffset max_entries) {
	stats_inc(OP_READRESERVED);
	if (debug_mode) {
		oplog_printf(ctx, "readreserved (%" PRIu64 ",%" PRIu64 ") ...",
				(uint64_t)max_entries,
				(uint64_t)off);
	}

	std::vector<NamedInodeEntry> entries;
	uint8_t status;
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_getreserved(off, max_entries, entries));

	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}

	return entries;
}

std::vector<NamedInodeEntry> readtrash(Context &ctx, NamedInodeOffset off, NamedInodeOffset max_entries) {
	stats_inc(OP_READTRASH);
	if (debug_mode) {
		oplog_printf(ctx, "readtrash (%" PRIu64 ",%" PRIu64 ") ...",
				(uint64_t)max_entries,
				(uint64_t)off);
	}

	std::vector<NamedInodeEntry> entries;
	uint8_t status;
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_gettrash(off, max_entries, entries));

	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}

	return entries;
}

void releasedir(Inode ino) {
	static constexpr int kBatchSize = 1000;

	stats_inc(OP_RELEASEDIR);
	if (debug_mode) {
		oplog_printf("releasedir (%lu) ...",
				(unsigned long int)ino);
	}
	oplog_printf("releasedir (%lu): OK",
			(unsigned long int)ino);

	std::unique_lock<shared_mutex> write_guard(gDirEntryCache.rwlock());
	gDirEntryCache.updateTime();
	gDirEntryCache.removeExpired(kBatchSize);
}


static finfo* fs_newfileinfo(uint8_t accmode, uint32_t inode) {
	finfo *fileinfo;
	if (!(fileinfo = (finfo*)malloc(sizeof(finfo))))
		throw RequestException(LIZARDFS_ERROR_OUTOFMEMORY);
	if (pthread_mutex_init(&(fileinfo->flushlock), NULL)) {
		free(fileinfo);
		throw RequestException(LIZARDFS_ERROR_EPERM);
	}
	if (pthread_mutex_init(&(fileinfo->lock), NULL)) {
		pthread_mutex_destroy(&(fileinfo->flushlock));
		free(fileinfo);
		throw RequestException(LIZARDFS_ERROR_EPERM);
	}
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

EntryParam create(Context &ctx, Inode parent, const char *name, mode_t mode,
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
	}
	if (parent==SPECIAL_INODE_ROOT) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx, "create (%lu,%s,-%s:0%04o): %s",
					(unsigned long int)parent,
					name,
					modestr+1,
					(unsigned int)mode,
					lizardfs_error_string(LIZARDFS_ERROR_EACCES));
			throw RequestException(LIZARDFS_ERROR_EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				lizardfs_error_string(LIZARDFS_ERROR_ENAMETOOLONG));
		throw RequestException(LIZARDFS_ERROR_ENAMETOOLONG);
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
				lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}

	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_mknod(parent,nleng,(const uint8_t*)name,TYPE_FILE,mode&07777,ctx.umask,ctx.uid,ctx.gid,0,inode,attr));
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o) (mknod): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				lizardfs_error_string(status));
		throw RequestException(status);
	}
	Attributes tmp_attr;
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_opencheck(inode,ctx.uid,ctx.gid,oflags,tmp_attr));

	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "create (%lu,%s,-%s:0%04o) (open): %s",
				(unsigned long int)parent,
				name,
				modestr+1,
				(unsigned int)mode,
				lizardfs_error_string(status));
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
		lzfs::log_debug("create ({}) ok -> keep cache: {}\n", inode, (int)fi->keep_cache);
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

void open(Context &ctx, Inode ino, FileInfo *fi) {
	uint8_t oflags;
	Attributes attr;
	uint8_t mattr;
	int status;

	finfo *fileinfo;

	stats_inc(OP_OPEN);
	if (debug_mode) {
		oplog_printf(ctx, "open (%lu) ...", (unsigned long int)ino);
	}

	if (IS_SPECIAL_INODE(ino)) {
		special_open(ino, ctx, fi);
		return;
	}

	oflags = 0;
	if ((fi->flags & O_CREAT) == O_CREAT) {
		oflags |= AFTER_CREATE;
	}
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_opencheck(ino,ctx.uid,ctx.gid,oflags,attr));
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "open (%lu): %s",
				(unsigned long int)ino,
				lizardfs_error_string(status));
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
		lzfs::log_debug("open ({}) ok -> keep cache: {}\n", ino, (int)fi->keep_cache);
	}
	fi->direct_io = gDirectIo;
	oplog_printf(ctx, "open (%lu): OK (%lu,%lu)",
			(unsigned long int)ino,
			(unsigned long int)fi->direct_io,
			(unsigned long int)fi->keep_cache);
}

static void update_credentials(Context::IdType index, const GroupCache::Groups &groups) {
	uint8_t status = fs_update_credentials(index, groups);
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}
}

void release(Inode ino, FileInfo *fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);

	stats_inc(OP_RELEASE);
	if (debug_mode) {
		oplog_printf("release (%lu) ...", (unsigned long int)ino);
	}

	if (IS_SPECIAL_INODE(ino)) {
		special_release(ino, fi);
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
	oplog_printf("release (%lu): OK",
			(unsigned long int)ino);
}

std::vector<uint8_t> read_special_inode(Context &ctx,
			Inode ino,
			size_t size,
			off_t off,
			FileInfo* fi) {
	LOG_AVG_TILL_END_OF_SCOPE0("read");
	stats_inc(OP_READ);

	return special_read(ino, ctx, size, off, fi, debug_mode);
}

ReadCache::Result read(Context &ctx,
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
		lzfs::log_debug("read from inode {} up to {} bytes from position {}",
		                ino, size, off);
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				lizardfs_error_string(LIZARDFS_ERROR_EBADF));
		throw RequestException(LIZARDFS_ERROR_EBADF);
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				lizardfs_error_string(LIZARDFS_ERROR_EFBIG));
		throw RequestException(LIZARDFS_ERROR_EFBIG);
	}
	try {
		const SteadyTimePoint deadline = SteadyClock::now() + std::chrono::seconds(30);
		uint8_t status = gLocalIoLimiter().waitForRead(ctx.pid, size, deadline);
		if (status == LIZARDFS_STATUS_OK) {
			status = gGlobalIoLimiter().waitForRead(ctx.pid, size, deadline);
		}
		if (status != LIZARDFS_STATUS_OK) {
			err = (status == LIZARDFS_ERROR_EPERM ? LIZARDFS_ERROR_EPERM : LIZARDFS_ERROR_IO);
			oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
					(unsigned long int)ino,
					(uint64_t)size,
					(uint64_t)off,
					lizardfs_error_string(err));
			throw RequestException(err);
		}
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "I/O limiting error: %s", ex.what());
		throw RequestException(LIZARDFS_ERROR_IO);
	}
	PthreadMutexWrapper lock(fileinfo->lock);
	PthreadMutexWrapper flushlock(fileinfo->flushlock);
	if (fileinfo->mode==IO_WRITEONLY) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				lizardfs_error_string(LIZARDFS_ERROR_EACCES));
		throw RequestException(LIZARDFS_ERROR_EACCES);
	}
	if (fileinfo->mode==IO_WRITE) {
		err = write_data_flush(fileinfo->data);
		if (err != LIZARDFS_STATUS_OK) {
			oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
					(unsigned long int)ino,
					(uint64_t)size,
					(uint64_t)off,
					lizardfs_error_string(err));
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

	err = read_data(fileinfo->data, off, size, alignedOffset, ssize, ret);
	ssize = ret.requestSize(alignedOffset, ssize);
	if (err != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				lizardfs_error_string(err));
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
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				(unsigned long int)ssize);
	}
	return ret;
}

BytesWritten write(Context &ctx, Inode ino, const char *buf, size_t size, off_t off,
			FileInfo *fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);
	int err;

	stats_inc(OP_WRITE);
	if (debug_mode) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 ") ...",
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
				lizardfs_error_string(LIZARDFS_ERROR_EBADF));
		throw RequestException(LIZARDFS_ERROR_EBADF);
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				lizardfs_error_string(LIZARDFS_ERROR_EFBIG));
		throw RequestException(LIZARDFS_ERROR_EFBIG);
	}
	try {
		const SteadyTimePoint deadline = SteadyClock::now() + std::chrono::seconds(30);
		uint8_t status = gLocalIoLimiter().waitForWrite(ctx.pid, size, deadline);
		if (status == LIZARDFS_STATUS_OK) {
			status = gGlobalIoLimiter().waitForWrite(ctx.pid, size, deadline);
		}
		if (status != LIZARDFS_STATUS_OK) {
			err = status == LIZARDFS_ERROR_EPERM ? LIZARDFS_ERROR_EPERM : LIZARDFS_ERROR_IO;
			oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): (logical) %s",
							(unsigned long int)ino,
							(uint64_t)size,
							(uint64_t)off,
							lizardfs_error_string(err));
			throw RequestException(err);
		}
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "I/O limiting error: %s", ex.what());
		throw RequestException(LIZARDFS_ERROR_IO);
	}
	PthreadMutexWrapper lock(fileinfo->lock);
	if (fileinfo->mode==IO_READONLY) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				lizardfs_error_string(LIZARDFS_ERROR_EACCES));
		throw RequestException(LIZARDFS_ERROR_EACCES);
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
	gDirEntryCache.lockAndInvalidateInode(ino);
	if (err != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): (physical) %s",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				lizardfs_error_string(err));
		throw RequestException(err);
	} else {
		oplog_printf(ctx, "write (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",
				(unsigned long int)ino,
				(uint64_t)size,
				(uint64_t)off,
				(unsigned long int)size);
		return size;
	}
}

void flush(Context &ctx, Inode ino, FileInfo* fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);
	int err;

	stats_inc(OP_FLUSH);
	if (debug_mode) {
		oplog_printf(ctx, "flush (%lu) ...",
				(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "flush (%lu): OK",
				(unsigned long int)ino);
		return;
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx, "flush (%lu): %s",
				(unsigned long int)ino,
				lizardfs_error_string(LIZARDFS_ERROR_EBADF));
		throw RequestException(LIZARDFS_ERROR_EBADF);
	}

	err = LIZARDFS_STATUS_OK;
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
	if (err != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "flush (%lu): %s",
				(unsigned long int)ino,
				lizardfs_error_string(err));
		throw RequestException(err);
	} else {
		oplog_printf(ctx, "flush (%lu): OK",
				(unsigned long int)ino);
	}
}

void fsync(Context &ctx, Inode ino, int datasync, FileInfo* fi) {
	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);
	int err;

	stats_inc(OP_FSYNC);
	if (debug_mode) {
		oplog_printf(ctx, "fsync (%lu,%d) ...",
				(unsigned long int)ino,
				datasync);
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
				lizardfs_error_string(LIZARDFS_ERROR_EBADF));
		throw RequestException(LIZARDFS_ERROR_EBADF);
	}
	err = LIZARDFS_STATUS_OK;
	PthreadMutexWrapper lock(fileinfo->lock);
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	if (err != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "fsync (%lu,%d): %s",
				(unsigned long int)ino,
				datasync,
				lizardfs_error_string(err));
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
	virtual uint8_t setxattr(Context& ctx, Inode ino, const char *name,
			uint32_t nleng, const char *value, size_t size, int mode) = 0;

	/*
	 * handler for request to get an extended attribute
	 * mode - one of XATTR_GMODE_*
	 * returns status and:
	 * * sets value is mode is XATTR_GMODE_GET_DATA
	 * * sets valueLength is mode is XATTR_GMODE_LENGTH_ONLY
	 */
	virtual uint8_t getxattr(Context& ctx, Inode ino, const char *name,
			uint32_t nleng, int mode, uint32_t& valueLength, std::vector<uint8_t>& value) = 0;

	/*
	 * handler for request to remove an extended attribute
	 * returns status
	 */
	virtual uint8_t removexattr(Context& ctx, Inode ino, const char *name,
			uint32_t nleng) = 0;
};

class PlainXattrHandler : public XattrHandler {
public:
	uint8_t setxattr(Context& ctx, Inode ino, const char *name,
		uint32_t nleng, const char *value, size_t size, int mode) override {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_setxattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name,
				(uint32_t)size, (const uint8_t*)value, mode));
		return status;
	}

	uint8_t getxattr(Context& ctx, Inode ino, const char *name,
		uint32_t nleng, int mode, uint32_t& valueLength, std::vector<uint8_t>& value) override {
		const uint8_t *buff;
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_getxattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name,
				mode, &buff, &valueLength));
		if (mode == XATTR_GMODE_GET_DATA && status == LIZARDFS_STATUS_OK) {
			value = std::vector<uint8_t>(buff, buff + valueLength);
		}
		return status;
	}

	uint8_t removexattr(Context& ctx, Inode ino, const char *name,
			uint32_t nleng) override {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_removexattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name));
		return status;
	}
};

class ErrorXattrHandler : public XattrHandler {
public:
	ErrorXattrHandler(uint8_t error) : error_(error) {}
	uint8_t setxattr(Context&, Inode, const char *,
			uint32_t, const char *, size_t, int) override {
		return error_;
	}

	uint8_t getxattr(Context&, Inode, const char *,
			uint32_t, int, uint32_t&, std::vector<uint8_t>&) override {
		return error_;
	}

	uint8_t removexattr(Context&, Inode, const char *,
			uint32_t) override {
		return error_;
	}
private:
	uint8_t error_;
};

class PosixAclXattrHandler : public XattrHandler {
public:
	PosixAclXattrHandler(AclType type) : type_(type) { }

	uint8_t setxattr(Context& ctx, Inode ino, const char *,
			uint32_t, const char *value, size_t size, int) override {
		static constexpr size_t kEmptyAclSize = 4;
		AccessControlList posix_acl;
		try {
			if (size <= kEmptyAclSize) {
				uint8_t status;
				RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
					fs_deletacl(ino, ctx.uid, ctx.gid, type_));
				return status;
			}
			posix_acl = aclConverter::extractAclObject((const uint8_t*)value, size);
		} catch (Exception&) {
			return LIZARDFS_ERROR_EINVAL;
		}
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_setacl(ino, ctx.uid, ctx.gid, type_, posix_acl));
		eraseAclCache(ino);
		gDirEntryCache.lockAndInvalidateInode(ino);
		return status;
	}

	uint8_t getxattr(Context& ctx, Inode ino, const char *,
			uint32_t, int /*mode*/, uint32_t& valueLength, std::vector<uint8_t>& value) override {
		try {
			AclCacheEntry cacheEntry = acl_cache->get(clock_.now(), ino, ctx.uid, ctx.gid);
			if (cacheEntry) {
				std::pair<bool, AccessControlList> posix_acl;
				if (type_ == AclType::kAccess) {
					posix_acl = cacheEntry->acl.convertToPosixACL();
				} else {
					posix_acl = cacheEntry->acl.convertToDefaultPosixACL();
				}
				if (!posix_acl.first) {
					return LIZARDFS_ERROR_ENOATTR;
				}
				value = aclConverter::aclObjectToXattr(posix_acl.second);
				valueLength = value.size();
				return LIZARDFS_STATUS_OK;
			} else {
				return LIZARDFS_ERROR_ENOATTR;
			}
		} catch (AclAcquisitionException &e) {
			sassert((e.status() != LIZARDFS_STATUS_OK) && (e.status() != LIZARDFS_ERROR_ENOATTR));
			return e.status();
		} catch (Exception &) {
			lzfs_pretty_syslog(LOG_WARNING, "Failed to convert ACL to xattr, looks like a bug");
			return LIZARDFS_ERROR_IO;
		}
	}

	uint8_t removexattr(Context& ctx, Inode ino, const char *,
			uint32_t) override {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_deletacl(ino, ctx.uid, ctx.gid, type_));
		eraseAclCache(ino);
		return status;
	}

private:
	AclType type_;
	SteadyClock clock_;
};

class NFSAclXattrHandler : public XattrHandler {
public:
	NFSAclXattrHandler() { }

	uint8_t setxattr(Context& ctx, Inode ino, const char *,
			uint32_t, const char *value, size_t size, int) override {
		uint8_t status = LIZARDFS_STATUS_OK;
		RichACL acl = richAclConverter::extractObjectFromNFS((uint8_t *)value, size);

		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_setacl(ino, ctx.uid, ctx.gid, acl));
		eraseAclCache(ino);
		gDirEntryCache.lockAndInvalidateInode(ino);
		return status;
	}

	uint8_t getxattr(Context& ctx, Inode ino, const char *,
			uint32_t, int, uint32_t& valueLength, std::vector<uint8_t>& value) override {
		try {
			AclCacheEntry cache_entry = acl_cache->get(clock_.now(), ino, ctx.uid, ctx.gid);
			if (cache_entry) {
				value = richAclConverter::objectToNFSXattr(cache_entry->acl, cache_entry->owner_id);
				valueLength = value.size();
			} else {
				// NOTICE(sarna): This call will most likely use attr cache anyway
				AttrReply attr_reply = LizardClient::getattr(ctx, ino);
				RichACL generated_acl = RichACL::createFromMode(
					attr_reply.attr.st_mode & 0777,
					S_ISDIR(attr_reply.attr.st_mode));
				value = richAclConverter::objectToNFSXattr(generated_acl, attr_reply.attr.st_uid);
				valueLength = value.size();
			}
			return LIZARDFS_STATUS_OK;
		} catch (AclAcquisitionException& e) {
			sassert((e.status() != LIZARDFS_STATUS_OK) && (e.status() != LIZARDFS_ERROR_ENOATTR));
			return e.status();
		} catch (Exception&) {
			lzfs_pretty_syslog(LOG_WARNING, "Failed to convert ACL to xattr, looks like a bug");
			return LIZARDFS_ERROR_IO;
		}
	}

	uint8_t removexattr(Context& ctx, Inode ino, const char *,
			uint32_t) override {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_deletacl(ino, ctx.uid, ctx.gid, AclType::kRichACL));
		eraseAclCache(ino);
		return status;
	}
private:
	SteadyClock clock_;
};

class RichAclXattrHandler : public XattrHandler {
public:
	RichAclXattrHandler() { }

	uint8_t setxattr(Context& ctx, Inode ino, const char *,
			uint32_t, const char *value, size_t size, int) override {
		uint8_t status = LIZARDFS_STATUS_OK;
		RichACL acl = richAclConverter::extractObjectFromRichACL((uint8_t *)value, size);

		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_setacl(ino, ctx.uid, ctx.gid, acl));
		eraseAclCache(ino);
		gDirEntryCache.lockAndInvalidateInode(ino);
		return status;
	}

	uint8_t getxattr(Context& ctx, Inode ino, const char *,
			uint32_t, int, uint32_t& valueLength, std::vector<uint8_t>& value) override {
		try {
			AclCacheEntry cache_entry = acl_cache->get(clock_.now(), ino, ctx.uid, ctx.gid);
			if (cache_entry) {
				value = richAclConverter::objectToRichACLXattr(cache_entry->acl);
				valueLength = value.size();
            }
            else {
                AttrReply attr_reply = LizardClient::getattr(ctx, ino);
                RichACL generated_acl = RichACL::createFromMode(
                                    attr_reply.attr.st_mode & 0777,
                                    S_ISDIR(attr_reply.attr.st_mode));
                value = richAclConverter::objectToRichACLXattr(generated_acl);
                valueLength = value.size();
			}
            return LIZARDFS_STATUS_OK;
		} catch (AclAcquisitionException& e) {
			sassert((e.status() != LIZARDFS_STATUS_OK) && (e.status() != LIZARDFS_ERROR_ENOATTR));
			return e.status();
		} catch (Exception&) {
			lzfs_pretty_syslog(LOG_WARNING, "Failed to convert ACL to xattr, looks like a bug");
			return LIZARDFS_ERROR_IO;
		}
	}

	uint8_t removexattr(Context& ctx, Inode ino, const char *,
			uint32_t) override {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_deletacl(ino, ctx.uid, ctx.gid, AclType::kRichACL));
		eraseAclCache(ino);
		return status;
	}
private:
	SteadyClock clock_;
};

#ifdef __APPLE__
class OsxAclXattrHandler : public XattrHandler {
public:
	OsxAclXattrHandler() {}

	uint8_t setxattr(Context& ctx, Inode ino, const char *,
			uint32_t, const char *value, size_t size, int) override {
		static constexpr size_t kEmptyAclSize = 4;
		if (size <= kEmptyAclSize) {
			return LIZARDFS_ERROR_EINVAL;
		}
		RichACL result;
		try {
			AclCacheEntry cache_entry = acl_cache->get(clock_.now(), ino, ctx.uid, ctx.gid);
			result = osxAclConverter::extractAclObject((const uint8_t*)value, size);
		} catch (RequestException &e) {
			return e.lizardfs_error_code;
		} catch (Exception&) {
			return LIZARDFS_ERROR_EINVAL;
		}
		uint8_t status = LIZARDFS_STATUS_OK;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_setacl(ino, ctx.uid, ctx.gid, result));
		eraseAclCache(ino);
		gDirEntryCache.lockAndInvalidateInode(ino);
		return status;
	}

	uint8_t getxattr(Context& ctx, Inode ino, const char *,
			uint32_t, int /*mode*/, uint32_t& valueLength, std::vector<uint8_t>& value) override {
		try {
			auto ts = clock_.now();
			AclCacheEntry cache_entry = acl_cache->get(ts, ino, ctx.uid, ctx.gid);
			if (cache_entry) {
				value = osxAclConverter::objectToOsxXattr(cache_entry->acl);
				valueLength = value.size();
				return LIZARDFS_STATUS_OK;
			} else {
				return LIZARDFS_ERROR_ENOATTR;
			}
		} catch (AclAcquisitionException& e) {
			sassert((e.status() != LIZARDFS_STATUS_OK) && (e.status() != LIZARDFS_ERROR_ENOATTR));
			return e.status();
		} catch (RequestException &e) {
			return e.lizardfs_error_code;
		} catch (Exception&) {
			lzfs_pretty_syslog(LOG_WARNING, "Failed to convert ACL to xattr, looks like a bug");
			return LIZARDFS_ERROR_IO;
		}
		valueLength = 0;
	}

	uint8_t removexattr(Context& ctx, Inode ino, const char *,
			uint32_t) override {
		uint8_t status;
		RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
			fs_deletacl(ino, ctx.uid, ctx.gid, AclType::kRichACL));
		eraseAclCache(ino);
		return status;
	}

private:
	SteadyClock clock_;
};

#endif

} // anonymous namespace

static PosixAclXattrHandler accessAclXattrHandler(AclType::kAccess);
static PosixAclXattrHandler defaultAclXattrHandler(AclType::kDefault);
static NFSAclXattrHandler nfsAclXattrHandler;
static RichAclXattrHandler richAclXattrHandler;
#ifdef __APPLE__
static OsxAclXattrHandler osxAclXattrHandler;
#endif

static ErrorXattrHandler enotsupXattrHandler(LIZARDFS_ERROR_ENOTSUP);
static PlainXattrHandler plainXattrHandler;

static std::map<std::string, XattrHandler*> xattr_handlers = {
	{"system.posix_acl_access", &accessAclXattrHandler},
	{"system.posix_acl_default", &defaultAclXattrHandler},
	{"system.nfs4_acl", &nfsAclXattrHandler},
	{"system.richacl", &richAclXattrHandler},
	{"security.capability", &enotsupXattrHandler},
#ifdef __APPLE__
	{"com.apple.system.Security", &osxAclXattrHandler},
#endif
};

static XattrHandler* choose_xattr_handler(const char *name) {
	try {
		return xattr_handlers.at(name);
	} catch (std::out_of_range&) {
		return &plainXattrHandler;
	}
}

void setxattr(Context &ctx, Inode ino, const char *name, const char *value,
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
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(LIZARDFS_ERROR_EPERM));
		throw RequestException(LIZARDFS_ERROR_EPERM);
	}
	if (size>MFS_XATTR_SIZE_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns E2BIG here
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(LIZARDFS_ERROR_E2BIG));
		throw RequestException(LIZARDFS_ERROR_E2BIG);
#else
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(LIZARDFS_ERROR_ERANGE));
		throw RequestException(LIZARDFS_ERROR_ERANGE);
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
				lizardfs_error_string(LIZARDFS_ERROR_EPERM));
		throw RequestException(LIZARDFS_ERROR_EPERM);
#else
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(LIZARDFS_ERROR_ERANGE));
		throw RequestException(LIZARDFS_ERROR_ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	if (strcmp(name,"security.capability")==0) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(LIZARDFS_ERROR_ENOTSUP));
		throw RequestException(LIZARDFS_ERROR_ENOTSUP);
	}
#if defined(XATTR_CREATE) && defined(XATTR_REPLACE)
	if ((flags&XATTR_CREATE) && (flags&XATTR_REPLACE)) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	mode = (flags==XATTR_CREATE)?XATTR_SMODE_CREATE_ONLY:(flags==XATTR_REPLACE)?XATTR_SMODE_REPLACE_ONLY:XATTR_SMODE_CREATE_OR_REPLACE;
#else
	mode = 0;
#endif
	(void)position;
	status = choose_xattr_handler(name)->setxattr(ctx, ino, name, nleng, value, size, mode);
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				flags,
				lizardfs_error_string(status));
		throw RequestException(status);
	}
	oplog_printf(ctx, "setxattr (%lu,%s,%" PRIu64 ",%d): OK",
			(unsigned long int)ino,
			name,
			(uint64_t)size,
			flags);
}

XattrReply getxattr(Context &ctx, Inode ino, const char *name, size_t size, uint32_t position) {
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
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				lizardfs_error_string(LIZARDFS_ERROR_ENODATA));
		throw RequestException(LIZARDFS_ERROR_ENODATA);
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				lizardfs_error_string(LIZARDFS_ERROR_EPERM));
		throw RequestException(LIZARDFS_ERROR_EPERM);
#else
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				lizardfs_error_string(LIZARDFS_ERROR_ERANGE));
		throw RequestException(LIZARDFS_ERROR_ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	if (strcmp(name,"security.capability")==0) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				lizardfs_error_string(LIZARDFS_ERROR_ENOTSUP));
		throw RequestException(LIZARDFS_ERROR_ENOTSUP);
	}
	if (size==0) {
		mode = XATTR_GMODE_LENGTH_ONLY;
	} else {
		mode = XATTR_GMODE_GET_DATA;
	}
	(void)position;
	status = choose_xattr_handler(name)->getxattr(ctx, ino, name, nleng, mode, leng, buffer);

	buff = buffer.data();
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "getxattr (%lu,%s,%" PRIu64 "): %s",
				(unsigned long int)ino,
				name,
				(uint64_t)size,
				lizardfs_error_string(status));
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
					lizardfs_error_string(LIZARDFS_ERROR_ERANGE));
			throw RequestException(LIZARDFS_ERROR_ERANGE);
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

XattrReply listxattr(Context &ctx, Inode ino, size_t size) {
	const uint8_t *buff;
	uint32_t leng;
	int status;
	uint8_t mode;

	stats_inc(OP_LISTXATTR);
	if (debug_mode) {
		oplog_printf(ctx, "listxattr (%lu,%" PRIu64 ") ...",
				(unsigned long int)ino,
				(uint64_t)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				lizardfs_error_string(LIZARDFS_ERROR_EPERM));
		throw RequestException(LIZARDFS_ERROR_EPERM);
	}
	if (size==0) {
		mode = XATTR_GMODE_LENGTH_ONLY;
	} else {
		mode = XATTR_GMODE_GET_DATA;
	}
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_listxattr(ino,0,ctx.uid,ctx.gid,mode,&buff,&leng));
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): %s",
				(unsigned long int)ino,
				(uint64_t)size,
				lizardfs_error_string(status));
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
					lizardfs_error_string(LIZARDFS_ERROR_ERANGE));
			throw RequestException(LIZARDFS_ERROR_ERANGE);
		} else {
			oplog_printf(ctx, "listxattr (%lu,%" PRIu64 "): OK (%" PRIu32 ")",
					(unsigned long int)ino,
					(uint64_t)size,
					leng);
			return XattrReply{leng, std::vector<uint8_t>(buff, buff + leng)};
		}
	}
}

void removexattr(Context &ctx, Inode ino, const char *name) {
	uint32_t nleng;
	int status;

	stats_inc(OP_REMOVEXATTR);
	if (debug_mode) {
		oplog_printf(ctx, "removexattr (%lu,%s) ...",
				(unsigned long int)ino,
				name);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_EPERM));
		throw RequestException(LIZARDFS_ERROR_EPERM);
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_EPERM));
		throw RequestException(LIZARDFS_ERROR_EPERM);
#else
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_ERANGE));
		throw RequestException(LIZARDFS_ERROR_ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	status = choose_xattr_handler(name)->removexattr(ctx, ino, name, nleng);
	if (status != LIZARDFS_STATUS_OK) {
		oplog_printf(ctx, "removexattr (%lu,%s): %s",
				(unsigned long int)ino,
				name,
				lizardfs_error_string(status));
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

void getlk(Context &ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock) {
	uint32_t status;

	stats_inc(OP_FLOCK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(ctx, "flock(ctx, %lu, fi): %s", (unsigned long int)ino, lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		}
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}

	if (!fi) {
		if (debug_mode) {
			oplog_printf(ctx,"flock(ctx, %lu, fi): %s",(unsigned long int)ino, lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		}
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}

	// communicate with master
	status = fs_getlk(ino, fi->lock_owner, lock);

	if (status) {
		throw RequestException(status);
	}
}

uint32_t setlk_send(Context &ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock) {
	uint32_t reqid;
	uint32_t status;

	stats_inc(OP_SETLK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(ctx, "flock(ctx, %lu, fi): %s", (unsigned long int)ino, lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		}
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}

	if (!fi) {
		if (debug_mode) {
			oplog_printf(ctx,"flock(ctx, %lu, fi): %s",(unsigned long int)ino, lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		}
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}

	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);

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
		throw RequestException(status);
	}

	return reqid;
}

void setlk_recv() {
	uint32_t status = fs_setlk_recv();

	if (status) {
		throw RequestException(status);
	}
}

uint32_t flock_send(Context &ctx, Inode ino, FileInfo* fi, int op) {
	uint32_t reqid;
	uint32_t status;

	stats_inc(OP_FLOCK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(ctx, "flock(ctx, %lu, fi): %s", (unsigned long int)ino, lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		}
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}

	if (!fi) {
		if (debug_mode) {
			oplog_printf(ctx,"flock(ctx, %lu, fi): %s",(unsigned long int)ino, lizardfs_error_string(LIZARDFS_ERROR_EINVAL));
		}
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}

	finfo *fileinfo = reinterpret_cast<finfo*>(fi->fh);

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
		throw RequestException(status);
	}

	return reqid;
}

void flock_recv() {
	uint32_t status = fs_flock_recv();

	if (status) {
		throw RequestException(status);
	}
}

JobId makesnapshot(Context &ctx, Inode ino, Inode dst_parent, const std::string &dst_name,
	          bool can_overwrite) {
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "makesnapshot (%lu, %lu, %s): %s",
				(unsigned long)ino, (unsigned long)dst_parent, dst_name.c_str(), strerr(EINVAL));
		throw RequestException(EINVAL);
	}

	JobId job_id;
	uint8_t status;
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_makesnapshot(ino, dst_parent, dst_name, ctx.uid, ctx.gid, can_overwrite, job_id));
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}

	return job_id;
}

std::string getgoal(Context &ctx, Inode ino) {
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "getgoal (%lu): %s",
				(unsigned long)ino, strerr(EINVAL));
		throw RequestException(EINVAL);
	}

	std::string goal;
	uint8_t status = fs_getgoal(ino, goal);
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}

	return goal;
}

void setgoal(Context &ctx, Inode ino, const std::string &goal_name, uint8_t smode) {
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "setgoal (%lu, %s): %s",
				(unsigned long)ino, goal_name.c_str(), strerr(EINVAL));
		throw RequestException(EINVAL);
	}

	uint8_t status = fs_setgoal(ino, ctx.uid, goal_name, smode);
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}
}

void statfs(uint64_t *totalspace, uint64_t *availspace, uint64_t *trashspace, uint64_t *reservedspace, uint32_t *inodes) {
	fs_statfs(totalspace, availspace, trashspace, reservedspace, inodes);
}

std::vector<ChunkWithAddressAndLabel> getchunksinfo(Context &ctx, Inode ino,
	                                  uint32_t chunk_index, uint32_t chunk_count) {
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx, "getchunksinfo (%lu, %u, %u): %s",
				(unsigned long)ino, (unsigned)chunk_index, (unsigned)chunk_count, strerr(EINVAL));
		throw RequestException(EINVAL);
	}
	std::vector<ChunkWithAddressAndLabel> chunks;
	uint8_t status;
	RETRY_ON_ERROR_WITH_UPDATED_CREDENTIALS(status, ctx,
		fs_getchunksinfo(ctx.uid, ctx.gid, ino, chunk_index, chunk_count, chunks));
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}
	return chunks;
}

std::vector<ChunkserverListEntry> getchunkservers() {
	std::vector<ChunkserverListEntry> chunkservers;
	uint8_t status = fs_getchunkservers(chunkservers);
	if (status != LIZARDFS_STATUS_OK) {
		throw RequestException(status);
	}
	return chunkservers;
}

void init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_, unsigned direntry_cache_size_,
		double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
		SugidClearMode sugid_clear_mode_, bool use_rwlock_,
		double acl_cache_timeout_, unsigned acl_cache_size_) {
	debug_mode = debug_mode_;
	keep_cache = keep_cache_;
	direntry_cache_timeout = direntry_cache_timeout_;
	entry_cache_timeout = entry_cache_timeout_;
	attr_cache_timeout = attr_cache_timeout_;
	mkdir_copy_sgid = mkdir_copy_sgid_;
	sugid_clear_mode = static_cast<decltype (sugid_clear_mode)>(sugid_clear_mode_);
	use_rwlock = use_rwlock_;
	uint64_t timeout = (uint64_t)(direntry_cache_timeout * 1000000);
	gDirEntryCache.setTimeout(timeout);
	gDirEntryCacheMaxSize = direntry_cache_size_;
	if (debug_mode) {
		lzfs::log_debug("cache parameters: file_keep_cache={} direntry_cache_timeout={:.2f}"
		                " entry_cache_timeout={:.2f} attr_cache_timeout={:.2f}",
		                (keep_cache==1)?"always":(keep_cache==2)?"never":"auto",
		                direntry_cache_timeout, entry_cache_timeout, attr_cache_timeout);
		lzfs::log_debug("mkdir copy sgid={} sugid clear mode={}",
		                mkdir_copy_sgid_, sugidClearModeString(sugid_clear_mode_));
		lzfs::log_debug("RW lock {}", use_rwlock ? "enabled" : "disabled");
		lzfs::log_debug("ACL acl_cache_timeout={:.2f}, acl_cache_size={}\n",
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

void fs_init(FsInitParams &params) {
	socketinit();
	mycrc32_init();
	int connection_ret = fs_init_master_connection(params);
	if (!params.delayed_init && connection_ret < 0) {
		lzfs_pretty_syslog(LOG_ERR, "Can't initialize connection with master server");
		socketrelease();
		throw std::runtime_error("Can't initialize connection with master server");
	}
	symlink_cache_init(params.symlink_cache_timeout_s);
	gGlobalIoLimiter();
	fs_init_threads(params.io_retries);
	masterproxy_init();

	gLocalIoLimiter();
	try {
		IoLimitsConfigLoader loader;
		if (!params.io_limits_config_file.empty()) {
			loader.load(std::ifstream(params.io_limits_config_file.c_str()));
		}
		gMountLimiter().loadConfiguration(loader);
	} catch (Exception &ex) {
		lzfs_pretty_syslog(LOG_ERR, "Can't initialize I/O limiting: %s", ex.what());
		masterproxy_term();
		::fs_term();
		symlink_cache_term();
		socketrelease();
		throw std::runtime_error("Can't initialize I/O limiting");
	}

	read_data_init(params.io_retries,
			params.chunkserver_round_time_ms,
			params.chunkserver_connect_timeout_ms,
			params.chunkserver_wave_read_timeout_ms,
			params.total_read_timeout_ms,
			params.cache_expiration_time_ms,
			params.readahead_max_window_size_kB,
			params.prefetch_xor_stripes,
			std::max(params.bandwidth_overuse, 1.));
	write_data_init(params.write_cache_size, params.io_retries, params.write_workers,
			params.write_window_size, params.chunkserver_write_timeout_ms, params.cache_per_inode_percentage);

	init(params.debug_mode, params.keep_cache, params.direntry_cache_timeout, params.direntry_cache_size,
		params.entry_cache_timeout, params.attr_cache_timeout, params.mkdir_copy_sgid,
		params.sugid_clear_mode, params.use_rw_lock,
		params.acl_cache_timeout, params.acl_cache_size);
}

void fs_term() {
	write_data_term();
	read_data_term();
	masterproxy_term();
	::fs_term();
	symlink_cache_term();
	socketrelease();
}

} // namespace LizardClient
