/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

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

#if defined(__APPLE__)
# if ! defined(__DARWIN_64_BIT_INO_T) && ! defined(_DARWIN_USE_64_BIT_INODE)
#  define __DARWIN_64_BIT_INO_T 0
# endif
#endif

#include "config.h"

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <syslog.h>
#include <inttypes.h>
#include <pthread.h>

#include "stats.h"
#include "oplog.h"
#include "datapack.h"
#include "mastercomm.h"
#include "masterproxy.h"
#include "readdata.h"
#include "writedata.h"
#include "strerr.h"
#include "MFSCommunication.h"

#include "dirattrcache.h"
#include "symlinkcache.h"
// #include "dircache.h"

#if MFS_ROOT_ID != FUSE_ROOT_ID
#error FUSE_ROOT_ID is not equal to MFS_ROOT_ID
#endif

#define READDIR_BUFFSIZE 50000

#define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

#define PKGVERSION ((VERSMAJ)*1000000+(VERSMID)*1000+(VERSMIN))

// #define MASTER_NAME ".master"
// #define MASTER_INODE 0x7FFFFFFF
// 0x01b6 == 0666
// static uint8_t masterattr[35]={'f', 0x01,0xB6, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define MASTERINFO_NAME ".masterinfo"
#define MASTERINFO_INODE 0x7FFFFFFF
// 0x0124 == 0b100100100 == 0444
#ifdef MASTERINFO_WITH_VERSION
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,14};
#else
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10};
#endif

#define STATS_NAME ".stats"
#define STATS_INODE 0x7FFFFFF0
// 0x01A4 == 0b110100100 == 0644
static uint8_t statsattr[35]={'f', 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define OPLOG_NAME ".oplog"
#define OPLOG_INODE 0x7FFFFFF1
#define OPHISTORY_NAME ".ophistory"
#define OPHISTORY_INODE 0x7FFFFFF2
// 0x0100 == 0b100000000 == 0400
static uint8_t oplogattr[35]={'f', 0x01,0x00, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

/* DIRCACHE
#define ATTRCACHE_NAME ".attrcache"
#define ATTRCACHE_INODE 0x7FFFFFF3
// 0x0180 == 0b110000000 == 0600
static uint8_t attrcacheattr[35]={'f', 0x01,0x80, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};
*/

#define MIN_SPECIAL_INODE 0x7FFFFFF0
#define IS_SPECIAL_INODE(ino) ((ino)>=MIN_SPECIAL_INODE)
#define IS_SPECIAL_NAME(name) ((name)[0]=='.' && (strcmp(STATS_NAME,(name))==0 || strcmp(MASTERINFO_NAME,(name))==0 || strcmp(OPLOG_NAME,(name))==0 || strcmp(OPHISTORY_NAME,(name))==0/* || strcmp(ATTRCACHE_NAME,(name))==0*/))

typedef struct _sinfo {
	char *buff;
	uint32_t leng;
	uint8_t reset;
	pthread_mutex_t lock;
} sinfo;

typedef struct _dirbuf {
	int wasread;
	int dataformat;
	uid_t uid;
	gid_t gid;
	const uint8_t *p;
	size_t size;
	void *dcache;
	pthread_mutex_t lock;
} dirbuf;

enum {IO_NONE,IO_READ,IO_WRITE,IO_READONLY,IO_WRITEONLY};

typedef struct _finfo {
	uint8_t mode;
	void *data;
	pthread_mutex_t lock;
} finfo;

static int debug_mode = 0;
static int usedircache = 1;
static int keep_cache = 0;
static double direntry_cache_timeout = 0.1;
static double entry_cache_timeout = 0.0;
static double attr_cache_timeout = 0.1;
static int mkdir_copy_sgid = 0;
static int sugid_clear_mode = 0;

//static int local_mode = 0;
//static int no_attr_cache = 0;

enum {
	OP_STATFS = 0,
	OP_ACCESS,
	OP_LOOKUP,
	OP_LOOKUP_INTERNAL,
	OP_DIRCACHE_LOOKUP,
//	OP_DIRCACHE_LOOKUP_POSITIVE,
//	OP_DIRCACHE_LOOKUP_NEGATIVE,
//	OP_DIRCACHE_LOOKUP_NOATTR,
	OP_GETATTR,
	OP_DIRCACHE_GETATTR,
	OP_SETATTR,
	OP_MKNOD,
	OP_UNLINK,
	OP_MKDIR,
	OP_RMDIR,
	OP_SYMLINK,
	OP_READLINK,
	OP_READLINK_CACHED,
	OP_RENAME,
	OP_LINK,
	OP_OPENDIR,
	OP_READDIR,
	OP_RELEASEDIR,
	OP_CREATE,
	OP_OPEN,
	OP_RELEASE,
	OP_READ,
	OP_WRITE,
	OP_FLUSH,
	OP_FSYNC,
	OP_SETXATTR,
	OP_GETXATTR,
	OP_LISTXATTR,
	OP_REMOVEXATTR,
//	OP_GETDIR_CACHED,
	OP_GETDIR_FULL,
	OP_GETDIR_SMALL,
	STATNODES
};

static uint64_t *statsptr[STATNODES];

void mfs_statsptr_init(void) {
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
}

void mfs_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_lock();
		(*statsptr[id])++;
		stats_unlock();
	}
}

#ifndef EDQUOT
# define EDQUOT ENOSPC
#endif
#ifndef ENOATTR
# ifdef ENODATA
#  define ENOATTR ENODATA
# else
#  define ENOATTR ENOENT
# endif
#endif

static int mfs_errorconv(int status) {
	int ret;
	switch (status) {
	case STATUS_OK:
		ret=0;
		break;
	case ERROR_EPERM:
		ret=EPERM;
		break;
	case ERROR_ENOTDIR:
		ret=ENOTDIR;
		break;
	case ERROR_ENOENT:
		ret=ENOENT;
		break;
	case ERROR_EACCES:
		ret=EACCES;
		break;
	case ERROR_EEXIST:
		ret=EEXIST;
		break;
	case ERROR_EINVAL:
		ret=EINVAL;
		break;
	case ERROR_ENOTEMPTY:
		ret=ENOTEMPTY;
		break;
	case ERROR_IO:
		ret=EIO;
		break;
	case ERROR_EROFS:
		ret=EROFS;
		break;
	case ERROR_QUOTA:
		ret=EDQUOT;
		break;
	case ERROR_ENOATTR:
		ret=ENOATTR;
		break;
	case ERROR_ENOTSUP:
		ret=ENOTSUP;
		break;
	case ERROR_ERANGE:
		ret=ERANGE;
		break;
	default:
		ret=EINVAL;
		break;
	}
	if (debug_mode && ret!=0) {
#ifdef HAVE_STRERROR_R
		char errorbuff[500];
# ifdef STRERROR_R_CHAR_P
		fprintf(stderr,"status: %s\n",strerror_r(ret,errorbuff,500));
# else
		strerror_r(ret,errorbuff,500);
		fprintf(stderr,"status: %s\n",errorbuff);
# endif
#else
# ifdef HAVE_PERROR
		errno=ret;
		perror("status: ");
# else
		fprintf(stderr,"status: %d\n",ret);
# endif
#endif
	}
	return ret;
}

static void mfs_type_to_stat(uint32_t inode,uint8_t type, struct stat *stbuf) {
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

static uint8_t mfs_attr_get_mattr(const uint8_t attr[35]) {
	return (attr[1]>>4);	// higher 4 bits of mode
}

static void mfs_attr_to_stat(uint32_t inode,const uint8_t attr[35], struct stat *stbuf) {
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime,attrnlink,attrrdev;
	uint64_t attrlength;
	const uint8_t *ptr;
	ptr = attr;
	attrtype = get8bit(&ptr);
	attrmode = get16bit(&ptr);
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);
	attratime = get32bit(&ptr);
	attrmtime = get32bit(&ptr);
	attrctime = get32bit(&ptr);
	attrnlink = get32bit(&ptr);
	stbuf->st_ino = inode;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	stbuf->st_blksize = MFSBLOCKSIZE;
#endif
	switch (attrtype) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR | ( attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK | ( attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG | ( attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO | ( attrmode & 07777);
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK | ( attrmode & 07777);
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK | ( attrmode & 07777);
		attrrdev = get32bit(&ptr);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR | ( attrmode & 07777);
		attrrdev = get32bit(&ptr);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
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
#ifdef HAVE_STRUCT_STAT_ST_BIRTHTIME
	stbuf->st_birthtime = attrctime;	// for future use
#endif
	stbuf->st_nlink = attrnlink;
}

static inline void mfs_makemodestr(char modestr[11],uint16_t mode) {
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

static void mfs_makeattrstr(char *buff,uint32_t size,struct stat *stbuf) {
	char modestr[11];
	mfs_makemodestr(modestr,stbuf->st_mode);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
	if (modestr[0]=='b' || modestr[0]=='c') {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%llu,%08lX]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(unsigned long long int)(stbuf->st_size),(unsigned long int)(stbuf->st_rdev));
	} else {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%llu]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(unsigned long long int)(stbuf->st_size));
	}
#else
	snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%llu]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(unsigned long long int)(stbuf->st_size));
#endif
}

#if FUSE_USE_VERSION >= 26
void mfs_statfs(fuse_req_t req,fuse_ino_t ino) {
#else
void mfs_statfs(fuse_req_t req) {
#endif
	uint64_t totalspace,availspace,trashspace,reservedspace;
	uint32_t inodes;
	uint32_t bsize;
	struct statvfs stfsbuf;
	memset(&stfsbuf,0,sizeof(stfsbuf));

	mfs_stats_inc(OP_STATFS);
	if (debug_mode) {
#if FUSE_USE_VERSION >= 26
		oplog_printf(fuse_req_ctx(req),"statfs (%lu)",(unsigned long int)ino);
#else
		oplog_printf(fuse_req_ctx(req),"statfs ()");
#endif
	}
#if FUSE_USE_VERSION >= 26
	(void)ino;
#endif
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
	stfsbuf.f_files = 1000000000+PKGVERSION+inodes;
	stfsbuf.f_ffree = 1000000000+PKGVERSION;
	stfsbuf.f_favail = 1000000000+PKGVERSION;
	//stfsbuf.f_flag = ST_RDONLY;
	fuse_reply_statfs(req,&stfsbuf);
#if FUSE_USE_VERSION >= 26
	oplog_printf(fuse_req_ctx(req),"statfs (%lu): OK (%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",(unsigned long int)ino,totalspace,availspace,trashspace,reservedspace,inodes);
#else
	oplog_printf(fuse_req_ctx(req),"statfs (): OK (%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",totalspace,availspace,trashspace,reservedspace,inodes);
#endif
}

/*
static int mfs_node_access(uint8_t attr[32],uint32_t uid,uint32_t gid,int mask) {
	uint32_t emode,mmode;
	uint32_t attruid,attrgid;
	uint16_t attrmode;
	uint8_t *ptr;
	if (uid == 0) {
		return 1;
	}
	ptr = attr+2;
	attrmode = get16bit(&ptr);
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);
	if (uid == attruid) {
		emode = (attrmode & 0700) >> 6;
	} else if (gid == attrgid) {
		emode = (attrmode & 0070) >> 3;
	} else {
		emode = attrmode & 0007;
	}
	mmode = 0;
	if (mask & R_OK) {
		mmode |= 4;
	}
	if (mask & W_OK) {
		mmode |= 2;
	}
	if (mask & X_OK) {
		mmode |= 1;
	}
	if ((emode & mmode) == mmode) {
		return 1;
	}
	return 0;
}
*/

void mfs_access(fuse_req_t req, fuse_ino_t ino, int mask) {
	int status;
	const struct fuse_ctx *ctx;
	int mmode;

	ctx = fuse_req_ctx(req);
	oplog_printf(ctx,"access (%lu,0x%X)",(unsigned long int)ino,mask);
	mfs_stats_inc(OP_ACCESS);
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
//	if (ino==MASTER_INODE) {
//		fuse_reply_err(req,0);
//		return;
//	}
	if (IS_SPECIAL_INODE(ino)) {
		if (mask & (W_OK | X_OK)) {
			fuse_reply_err(req,EACCES);
		} else {
			fuse_reply_err(req,0);
		}
		return;
	}
	status = fs_access(ino,ctx->uid,ctx->gid,mmode);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req,status);
	} else {
		fuse_reply_err(req,0);
	}
}

void mfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint64_t maxfleng;
	uint32_t inode;
	uint32_t nleng;
	uint8_t attr[35];
	char attrstr[256];
	uint8_t mattr;
	uint8_t icacheflag;
	int status;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	if (debug_mode) {
		oplog_printf(ctx,"lookup (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"lookup (%lu,%s)\n",(unsigned long int)parent,name);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		mfs_stats_inc(OP_LOOKUP);
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"lookup (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		return;
	}
	if (parent==FUSE_ROOT_ID) {
		if (nleng==2 && name[0]=='.' && name[1]=='.') {
			nleng=1;
		}
//		if (strcmp(name,MASTER_NAME)==0) {
//			memset(&e, 0, sizeof(e));
//			e.ino = MASTER_INODE;
//			e.attr_timeout = 3600.0;
//			e.entry_timeout = 3600.0;
//			mfs_attr_to_stat(MASTER_INODE,masterattr,&e.attr);
//			fuse_reply_entry(req, &e);
//			mfs_stats_inc(OP_LOOKUP);
//			return ;
//		}
		if (strcmp(name,MASTERINFO_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = MASTERINFO_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(MASTERINFO_INODE,masterinfoattr,&e.attr);
			fuse_reply_entry(req, &e);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: MASTERINFO): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return ;
		}
		if (strcmp(name,STATS_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = STATS_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(STATS_INODE,statsattr,&e.attr);
			fuse_reply_entry(req, &e);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: STATS): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return ;
		}
		if (strcmp(name,OPLOG_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = OPLOG_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(OPLOG_INODE,oplogattr,&e.attr);
			fuse_reply_entry(req, &e);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: OPLOG): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return ;
		}
		if (strcmp(name,OPHISTORY_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = OPHISTORY_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(OPHISTORY_INODE,oplogattr,&e.attr);
			fuse_reply_entry(req, &e);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: OPHISTORY): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return ;
		}
/*
		if (strcmp(name,ATTRCACHE_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = ATTRCACHE_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(ATTRCACHE_INODE,attrcacheattr,&e.attr);
			fuse_reply_entry(req, &e);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: ATTRCACHE)",(unsigned long int)parent,name);
			return ;
		}
*/
	}
/*
	if (newdircache) {
		const uint8_t *dbuff;
		uint32_t dsize;
		switch (dir_cache_lookup(parent,nleng,(const uint8_t*)name,&inode,attr)) {
			case -1:
				mfs_stats_inc(OP_DIRCACHE_LOOKUP_NEGATIVE);
				fuse_reply_err(req,ENOENT);
				oplog_printf(ctx,"lookup (%lu,%s) (cached answer: %s)",(unsigned long int)parent,name,strerr(ENOENT));
				return;
			case 1:
				mfs_stats_inc(OP_DIRCACHE_LOOKUP_POSITIVE);
				status = 0;
				oplog_printf(ctx,"lookup (%lu,%s) (cached answer: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
				break;
			case -2:
				mfs_stats_inc(OP_LOOKUP);
				status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid,&inode,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					oplog_printf(ctx,"lookup (%lu,%s) (lookup forced by cache: %s)",(unsigned long int)parent,name,strerr(status));
				} else {
					oplog_printf(ctx,"lookup (%lu,%s) (lookup forced by cache: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
				}
				break;
			case -3:
				mfs_stats_inc(OP_DIRCACHE_LOOKUP_NOATTR);
				status = fs_getattr(inode,ctx->uid,ctx->gid,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					oplog_printf(ctx,"lookup (%lu,%s) (getattr forced by cache: %s)",(unsigned long int)parent,name,strerr(status));
				} else {
					oplog_printf(ctx,"lookup (%lu,%s) (getattr forced by cache: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
				}
				break;
			default:
				status = fs_getdir_plus(parent,ctx->uid,ctx->gid,1,&dbuff,&dsize);
				status = mfs_errorconv(status);
				if (status!=0) {
					fuse_reply_err(req, status);
					oplog_printf(ctx,"lookup (%lu,%s) (readdir: %s)",(unsigned long int)parent,name,strerr(status));
					return;
				}
				mfs_stats_inc(OP_GETDIR_FULL);
				dir_cache_newdirdata(parent,dsize,dbuff);
				switch (dir_cache_lookup(parent,nleng,(const uint8_t*)name,&inode,attr)) {
					case -1:
						mfs_stats_inc(OP_DIRCACHE_LOOKUP_NEGATIVE);
						fuse_reply_err(req,ENOENT);
						oplog_printf(ctx,"lookup (%lu,%s) (after readdir cached answer: %s)",(unsigned long int)parent,name,strerr(ENOENT));
						return;
					case 1:
						mfs_stats_inc(OP_DIRCACHE_LOOKUP_POSITIVE);
						oplog_printf(ctx,"lookup (%lu,%s) (after readdir cached answer: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
						break;
					default:
						mfs_stats_inc(OP_LOOKUP);
						status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid,&inode,attr);
						status = mfs_errorconv(status);
						if (status!=0) {
							oplog_printf(ctx,"lookup (%lu,%s) (after readdir lookup forced by cache: %s)",(unsigned long int)parent,name,strerr(status));
						} else {
							oplog_printf(ctx,"lookup (%lu,%s) (after readdir lookup forced by cache: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
						}
				}
		}
	} else 
*/
	if (usedircache && dcache_lookup(ctx,parent,nleng,(const uint8_t*)name,&inode,attr)) {
		if (debug_mode) {
			fprintf(stderr,"lookup: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_LOOKUP);
		status = 0;
		icacheflag = 1;
//		oplog_printf(ctx,"lookup (%lu,%s) (using open dir cache): OK (%lu)",(unsigned long int)parent,name,(unsigned long int)inode);
	} else {
		mfs_stats_inc(OP_LOOKUP);
		status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid,&inode,attr);
		status = mfs_errorconv(status);
		icacheflag = 0;
	}
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"lookup (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		return;
	}
	if (attr[0]==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(inode);
	} else {
		maxfleng = 0;
	}
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	mattr = mfs_attr_get_mattr(attr);
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:((attr[0]==TYPE_DIRECTORY)?direntry_cache_timeout:entry_cache_timeout);
	mfs_attr_to_stat(inode,attr,&e.attr);
	if (maxfleng>(uint64_t)(e.attr.st_size)) {
		e.attr.st_size=maxfleng;
	}
//	if (attr[0]==TYPE_FILE && debug_mode) {
//		fprintf(stderr,"lookup inode %lu - file size: %llu\n",(unsigned long int)inode,(unsigned long long int)e.attr.st_size);
//	}
	fuse_reply_entry(req, &e);
	mfs_makeattrstr(attrstr,256,&e.attr);
	oplog_printf(ctx,"lookup (%lu,%s)%s: OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,icacheflag?" (using open dir cache)":"",e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
}

void mfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint64_t maxfleng;
	double attr_timeout;
	struct stat o_stbuf;
	uint8_t attr[35];
	char attrstr[256];
	int status;
	uint8_t icacheflag;
	const struct fuse_ctx *ctx;
	(void)fi;

	ctx = fuse_req_ctx(req);
//	mfs_stats_inc(OP_GETATTR);
	if (debug_mode) {
		oplog_printf(ctx,"getattr (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"getattr (%lu)\n",(unsigned long int)ino);
	}
//	if (ino==MASTER_INODE) {
//		memset(&o_stbuf, 0, sizeof(struct stat));
//		mfs_attr_to_stat(ino,masterattr,&o_stbuf);
//		fuse_reply_attr(req, &o_stbuf, 3600.0);
//		mfs_stats_inc(OP_GETATTR);
//		return;
//	}
	if (ino==MASTERINFO_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,masterinfoattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"getattr (%lu) (internal node: MASTERINFO): OK (3600,%s)",(unsigned long int)ino,attrstr);
		return;
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"getattr (%lu) (internal node: STATS): OK (3600,%s)",(unsigned long int)ino,attrstr);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,oplogattr,&o_stbuf);
//		if (fi && fi->fh) {
//			uint64_t *posptr = (uint64_t*)(unsigned long)(fi->fh);
//			o_stbuf.st_size = (*posptr)+oplog_getpos();
//		}
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"getattr (%lu) (internal node: %s): OK (3600,%s)",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",attrstr);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,attrcacheattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		mfs_stats_inc(OP_GETATTR);
		oplog_printf(ctx,"getattr (%lu) (internal node ATTRCACHE)",(unsigned long int)ino);
		return;
	}
*/
//	if (write_data_flush_inode(ino)) {
//		mfs_stats_inc(OP_GETATTR);
//		status = fs_getattr(ino,ctx->uid,ctx->gid,attr);
//		status = mfs_errorconv(status);
/*
	if (newdircache) {
		if (dir_cache_getattr(ino,attr)) {
			mfs_stats_inc(OP_DIRCACHE_GETATTR);
			status = 0;
			oplog_printf(ctx,"getattr (%lu) (data found in cache)",(unsigned long int)ino);
		} else {
			mfs_stats_inc(OP_GETATTR);
			status = fs_getattr(ino,ctx->uid,ctx->gid,attr);
			status = mfs_errorconv(status);
			if (status!=0) {
				oplog_printf(ctx,"getattr (%lu) (data not found in cache: %s)",(unsigned long int)ino,strerr(status));
			} else {
				oplog_printf(ctx,"getattr (%lu) (data not found in cache)",(unsigned long int)ino);
			}
		}
	} else 
*/
	if (usedircache && dcache_getattr(ctx,ino,attr)) {
		if (debug_mode) {
			fprintf(stderr,"getattr: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_GETATTR);
		status = 0;
		icacheflag = 1;
	} else {
		mfs_stats_inc(OP_GETATTR);
		status = fs_getattr(ino,ctx->uid,ctx->gid,attr);
		status = mfs_errorconv(status);
		icacheflag = 0;
	}
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"getattr (%lu): %s",(unsigned long int)ino,strerr(status));
		return;
	}
	if (attr[0]==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	attr_timeout = (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	fuse_reply_attr(req, &o_stbuf, attr_timeout);
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(ctx,"getattr (%lu): OK (%.1lf,%s)",(unsigned long int)ino,attr_timeout,attrstr);
}

void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	uint64_t maxfleng;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	double attr_timeout;
	int status;
	const struct fuse_ctx *ctx;
	uint8_t setmask = 0;

	ctx = fuse_req_ctx(req);
	mfs_makemodestr(modestr,stbuf->st_mode);
	mfs_stats_inc(OP_SETATTR);
	if (debug_mode) {
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]) ...",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size));
		fprintf(stderr,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu])\n",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size));
	}
	if (/*ino==MASTER_INODE || */ino==MASTERINFO_INODE) {
		fuse_reply_err(req, EPERM);
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(EPERM));
		return;
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]) (internal node: STATS): OK (3600,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),attrstr);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,oplogattr,&o_stbuf);
//		if (fi && fi->fh) {
//			uint64_t *posptr = (uint64_t*)(unsigned long)(fi->fh);
//			o_stbuf.st_size = (*posptr)+oplog_getpos();
//		}
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]) (internal node: %s): OK (3600,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",attrstr);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,attrcacheattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
*/
	status = EINVAL;
	if ((to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME|FUSE_SET_ATTR_SIZE)) == 0) {	// change other flags or change nothing
//		status = fs_getattr(ino,ctx->uid,ctx->gid,attr);
		status = fs_setattr(ino,ctx->uid,ctx->gid,0,0,0,0,0,0,0,attr);	// ext3 compatibility - change ctime during this operation (usually chown(-1,-1))
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
			return;
		}
	}
	if (to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME)) {
		setmask = 0;
		if (to_set & FUSE_SET_ATTR_MODE) {
			setmask |= SET_MODE_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_UID) {
			setmask |= SET_UID_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_GID) {
			setmask |= SET_GID_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_ATIME) {
			setmask |= SET_ATIME_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_MTIME) {
			setmask |= SET_MTIME_FLAG;
			write_data_flush_inode(ino);	// in this case we want flush all pending writes because they could overwrite mtime
		}
		status = fs_setattr(ino,ctx->uid,ctx->gid,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,sugid_clear_mode,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
			return;
		}
	}
	if (to_set & FUSE_SET_ATTR_SIZE) {
		if (stbuf->st_size<0) {
			fuse_reply_err(req, EINVAL);
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(EINVAL));
			return;
		}
		if (stbuf->st_size>=MAX_FILE_SIZE) {
			fuse_reply_err(req, EFBIG);
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(EFBIG));
			return;
		}
		write_data_flush_inode(ino);
		status = fs_truncate(ino,(fi!=NULL)?1:0,ctx->uid,ctx->gid,stbuf->st_size,attr);
		while (status==ERROR_LOCKED) {
			sleep(1);
			status = fs_truncate(ino,(fi!=NULL)?1:0,ctx->uid,ctx->gid,stbuf->st_size,attr);
		}
		status = mfs_errorconv(status);
		read_inode_ops(ino);
		if (status!=0) {
			fuse_reply_err(req, status);
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
			return;
		}
	}
	if (status!=0) {	// should never happend but better check than sorry
		fuse_reply_err(req, status);
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
		return;
	}
	if (attr[0]==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	attr_timeout = (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	fuse_reply_attr(req, &o_stbuf, attr_timeout);
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): OK (%.1lf,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),attr_timeout,attrstr);
}

void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	uint8_t type;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_MKNOD);
	if (debug_mode) {
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX) ...",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev);
		fprintf(stderr,"mknod (%lu,%s,%s:0%04o,0x%08lX)\n",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(ENAMETOOLONG));
		return;
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
		fuse_reply_err(req, EPERM);
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(EPERM));
		return;
	}

	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(EACCES));
			return;
		}
	}

	status = fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,ctx->uid,ctx->gid,rdev,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(status));
	} else {
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
	}
}

void mfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_UNLINK);
	if (debug_mode) {
		oplog_printf(ctx,"unlink (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"unlink (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(EACCES));
			return;
		}
	}

	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		return;
	}

	status = fs_unlink(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
	} else {
//		if (newdircache) {
//			dir_cache_unlink(parent,nleng,(const uint8_t*)name);
//		}
		fuse_reply_err(req, 0);
		oplog_printf(ctx,"unlink (%lu,%s): OK",(unsigned long int)parent,name);
	}
}

void mfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_MKDIR);
	if (debug_mode) {
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o) ...",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
		fprintf(stderr,"mkdir (%lu,%s,d%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EACCES));
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(ENAMETOOLONG));
		return;
	}

	status = fs_mkdir(parent,nleng,(const uint8_t*)name,mode,ctx->uid,ctx->gid,mkdir_copy_sgid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
	} else {
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:direntry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
	}
}

void mfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_RMDIR);
	if (debug_mode) {
		oplog_printf(ctx,"rmdir (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"rmdir (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(EACCES));
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		return;
	}

	status = fs_rmdir(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
	} else {
//		if (newdircache) {
//			dir_cache_unlink(parent,nleng,(const uint8_t*)name);
//		}
		fuse_reply_err(req, 0);
		oplog_printf(ctx,"rmdir (%lu,%s): OK",(unsigned long int)parent,name);
	}
}

void mfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_SYMLINK);
	if (debug_mode) {
		oplog_printf(ctx,"symlink (%s,%lu,%s) ...",path,(unsigned long int)parent,name);
		fprintf(stderr,"symlink (%s,%lu,%s)\n",path,(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(EACCES));
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		return;
	}

	ctx = fuse_req_ctx(req);
	status = fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)path,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(status));
	} else {
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"symlink (%s,%lu,%s): OK (%.1lf,%lu,%.1lf,%s)",path,(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
	}
}

void mfs_readlink(fuse_req_t req, fuse_ino_t ino) {
	int status;
	const uint8_t *path;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	if (debug_mode) {
		oplog_printf(ctx,"readlink (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"readlink (%lu)\n",(unsigned long int)ino);
	}
	if (symlink_cache_search(ino,&path)) {
		mfs_stats_inc(OP_READLINK_CACHED);
		fuse_reply_readlink(req, (char*)path);
		oplog_printf(ctx,"readlink (%lu) (using cache): OK (%s)",(unsigned long int)ino,(char*)path);
		return;
	}
	mfs_stats_inc(OP_READLINK);
	status = fs_readlink(ino,&path);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"readlink (%lu): %s",(unsigned long int)ino,strerr(status));
	} else {
		symlink_cache_insert(ino,path);
		fuse_reply_readlink(req, (char*)path);
		oplog_printf(ctx,"readlink (%lu): OK (%s)",(unsigned long int)ino,(char*)path);
	}
}

void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
	uint32_t nleng,newnleng;
	int status;
	uint32_t inode;
	uint8_t attr[35];
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_RENAME);
	if (debug_mode) {
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s) ...",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
		fprintf(stderr,"rename (%lu,%s,%lu,%s)\n",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(EACCES));
			return;
		}
	}
	if (newparent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(newname)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(EACCES));
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		return;
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		return;
	}

	status = fs_rename(parent,nleng,(const uint8_t*)name,newparent,newnleng,(const uint8_t*)newname,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(status));
	} else {
//		if (newdircache) {
//			dir_cache_unlink(parent,nleng,(const uint8_t*)name);
//			dir_cache_link(newparent,newnleng,(const uint8_t*)newname,inode,attr);
//		}
		fuse_reply_err(req, 0);
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): OK",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
	}
}

void mfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
	uint32_t newnleng;
	int status;
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	char attrstr[256];
	uint8_t mattr;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_LINK);
	if (debug_mode) {
		oplog_printf(ctx,"link (%lu,%lu,%s) ...",(unsigned long int)ino,(unsigned long int)newparent,newname);
		fprintf(stderr,"link (%lu,%lu,%s)\n",(unsigned long int)ino,(unsigned long int)newparent,newname);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req, EACCES);
		oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(EACCES));
		return;
	}
	if (newparent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(newname)) {
			fuse_reply_err(req, EACCES);
			oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(EACCES));
			return;
		}
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		return;
	}

//	write_data_flush_inode(ino);
	status = fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(status));
	} else {
//		if (newdircache) {
//			dir_cache_link(newparent,newnleng,(const uint8_t*)newname,inode,attr);
//		}
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"link (%lu,%lu,%s): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)ino,(unsigned long int)newparent,newname,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
	}
}

void mfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	dirbuf *dirinfo;
	int status;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_OPENDIR);
	if (debug_mode) {
		oplog_printf(ctx,"opendir (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"opendir (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req, ENOTDIR);
		oplog_printf(ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(ENOTDIR));
	}
	status = fs_access(ino,ctx->uid,ctx->gid,MODE_MASK_R);	// at least test rights
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(status));
	} else {
		dirinfo = malloc(sizeof(dirbuf));
		pthread_mutex_init(&(dirinfo->lock),NULL);
		pthread_mutex_lock(&(dirinfo->lock));	// make valgrind happy
		dirinfo->p = NULL;
		dirinfo->size = 0;
		dirinfo->dcache = NULL;
		dirinfo->wasread = 0;
		pthread_mutex_unlock(&(dirinfo->lock));	// make valgrind happy
		fi->fh = (unsigned long)dirinfo;
		if (fuse_reply_open(req,fi) == -ENOENT) {
			fi->fh = 0;
			pthread_mutex_destroy(&(dirinfo->lock));
			free(dirinfo);
		}
		oplog_printf(ctx,"opendir (%lu): OK",(unsigned long int)ino);
	}
}

void mfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	int status;
        dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	char buffer[READDIR_BUFFSIZE];
	char name[MFS_NAME_MAX+1];
	const uint8_t *ptr,*eptr;
	uint8_t end;
	size_t opos,oleng;
	uint8_t nleng;
	uint32_t inode;
	uint8_t type;
	struct stat stbuf;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_READDIR);
	if (debug_mode) {
		oplog_printf(ctx,"readdir (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		fprintf(stderr,"readdir (%lu,%llu,%llu)\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (off<0) {
		fuse_reply_err(req,EINVAL);
		oplog_printf(ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EINVAL));
		return;
	}
	pthread_mutex_lock(&(dirinfo->lock));
	if (dirinfo->wasread==0 || (dirinfo->wasread==1 && off==0)) {
		const uint8_t *dbuff;
		uint32_t dsize;
		uint8_t needscopy;
/*
		if (newdircache) {
			status = dir_cache_getdirdata(ino,&dsize,&dbuff);
			if (status==1) {	// got dir from new cache
				mfs_stats_inc(OP_GETDIR_CACHED);
				needscopy = 0;
				dirinfo->dataformat = 0;
				status = 0;
			} else {
				status = fs_getdir_plus(ino,ctx->uid,ctx->gid,1,&dbuff,&dsize);
				if (status==0) {
					mfs_stats_inc(OP_GETDIR_FULL);
					dir_cache_newdirdata(ino,dsize,dbuff);
				}
				needscopy = 1;
				dirinfo->dataformat = 1;
			}
		} else 
*/
		if (usedircache) {
			status = fs_getdir_plus(ino,ctx->uid,ctx->gid,0,&dbuff,&dsize);
			if (status==0) {
				mfs_stats_inc(OP_GETDIR_FULL);
			}
			needscopy = 1;
			dirinfo->dataformat = 1;
		} else {
			status = fs_getdir(ino,ctx->uid,ctx->gid,&dbuff,&dsize);
			if (status==0) {
				mfs_stats_inc(OP_GETDIR_SMALL);
			}
			needscopy = 1;
			dirinfo->dataformat = 0;
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			pthread_mutex_unlock(&(dirinfo->lock));
			oplog_printf(ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(status));
			return;
		}
		if (dirinfo->dcache) {
			dcache_release(dirinfo->dcache);
			dirinfo->dcache = NULL;
		}
		if (dirinfo->p) {
			free((uint8_t*)(dirinfo->p));
			dirinfo->p = NULL;
		}
		if (needscopy) {
			dirinfo->p = malloc(dsize);
			if (dirinfo->p == NULL) {
				fuse_reply_err(req,EINVAL);
				pthread_mutex_unlock(&(dirinfo->lock));
				oplog_printf(ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EINVAL));
				return;
			}
			memcpy((uint8_t*)(dirinfo->p),dbuff,dsize);
		} else {
			dirinfo->p = dbuff;
		}
		dirinfo->size = dsize;
		if (usedircache && dirinfo->dataformat==1) {
			dirinfo->dcache = dcache_new(ctx,ino,dirinfo->p,dirinfo->size);
		}
	}
	dirinfo->wasread=1;

	if (off>=(off_t)(dirinfo->size)) {
		fuse_reply_buf(req, NULL, 0);
		oplog_printf(ctx,"readdir (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	} else {
		if (size>READDIR_BUFFSIZE) {
			size=READDIR_BUFFSIZE;
		}
		ptr = dirinfo->p+off;
		eptr = dirinfo->p+dirinfo->size;
		opos = 0;
		end = 0;

		while (ptr<eptr && end==0) {
			nleng = ptr[0];
			ptr++;
			memcpy(name,ptr,nleng);
			name[nleng]=0;
			ptr+=nleng;
			off+=nleng+((dirinfo->dataformat)?40:6);
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				if (dirinfo->dataformat) {
					mfs_attr_to_stat(inode,ptr,&stbuf);
					ptr+=35;
				} else {
					type = get8bit(&ptr);
					mfs_type_to_stat(inode,type,&stbuf);
				}
				oleng = fuse_add_direntry(req, buffer + opos, size - opos, name, &stbuf, off);
				if (opos+oleng>size) {
					end=1;
				} else {
					opos+=oleng;
				}
			}
		}

		fuse_reply_buf(req,buffer,opos);
		oplog_printf(ctx,"readdir (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)opos);
	}
	pthread_mutex_unlock(&(dirinfo->lock));
}

void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	(void)ino;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_RELEASEDIR);
	if (debug_mode) {
		oplog_printf(ctx,"releasedir (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"releasedir (%lu)\n",(unsigned long int)ino);
	}
	pthread_mutex_lock(&(dirinfo->lock));
	pthread_mutex_unlock(&(dirinfo->lock));
	pthread_mutex_destroy(&(dirinfo->lock));
	if (dirinfo->dcache) {
		dcache_release(dirinfo->dcache);
	}
	if (dirinfo->p) {
		free((uint8_t*)(dirinfo->p));
	}
	free(dirinfo);
	fi->fh = 0;
	fuse_reply_err(req,0);
	oplog_printf(ctx,"releasedir (%lu): OK",(unsigned long int)ino);
}


static finfo* mfs_newfileinfo(uint8_t accmode,uint32_t inode) {
	finfo *fileinfo;
	fileinfo = malloc(sizeof(finfo));
	pthread_mutex_init(&(fileinfo->lock),NULL);
	pthread_mutex_lock(&(fileinfo->lock)); // make helgrind happy
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
	pthread_mutex_unlock(&(fileinfo->lock)); // make helgrind happy
	return fileinfo;
}

static void mfs_removefileinfo(finfo* fileinfo) {
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode == IO_READONLY || fileinfo->mode == IO_READ) {
		read_data_end(fileinfo->data);
	} else if (fileinfo->mode == IO_WRITEONLY || fileinfo->mode == IO_WRITE) {
//		write_data_flush(fileinfo->data);
		write_data_end(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	pthread_mutex_destroy(&(fileinfo->lock));
	free(fileinfo);
}

void mfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t oflags;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;
	finfo *fileinfo;

	ctx = fuse_req_ctx(req);
	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_CREATE);
	if (debug_mode) {
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
		fprintf(stderr,"create (%lu,%s,-%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			fuse_reply_err(req,EACCES);
			oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EACCES));
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(ENAMETOOLONG));
		return;
	}

	oflags = AFTER_CREATE;
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	} else {
		fuse_reply_err(req, EINVAL);
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EINVAL));
		return;
	}

	status = fs_mknod(parent,nleng,(const uint8_t*)name,TYPE_FILE,mode&07777,ctx->uid,ctx->gid,0,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o) (mknod): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
		return;
	}
//	if (newdircache) {
//		dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//	}
	status = fs_opencheck(inode,ctx->uid,ctx->gid,oflags,NULL);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o) (open): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
		return;
	}

	mattr = mfs_attr_get_mattr(attr);
	fileinfo = mfs_newfileinfo(fi->flags & O_ACCMODE,inode);
	fi->fh = (unsigned long)fileinfo;
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"create (%lu) ok -> keep cache: %lu\n",(unsigned long int)inode,(unsigned long int)fi->keep_cache);
	}
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
	mfs_attr_to_stat(inode,attr,&e.attr);
	if (fuse_reply_create(req, &e, fi) == -ENOENT) {
		mfs_removefileinfo(fileinfo);
	}
	mfs_makeattrstr(attrstr,256,&e.attr);
	oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): OK (%.1lf,%lu,%.1lf,%s,%lu)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr,(unsigned long int)fi->keep_cache);
}

void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint8_t oflags;
	uint8_t attr[35];
	uint8_t mattr;
	int status;
	const struct fuse_ctx *ctx;
	finfo *fileinfo;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_OPEN);
	if (debug_mode) {
		oplog_printf(ctx,"open (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"open (%lu)\n",(unsigned long int)ino);
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo;
//		status = fs_direct_connect();
//		if (status<0) {
//			fuse_reply_err(req,EIO);
//			return;
//		}
//		masterinfo = malloc(sizeof(minfo));
//		if (masterinfo==NULL) {
//			fuse_reply_err(req,ENOMEM);
//			return;
//		}
//		masterinfo->sd = status;
//		masterinfo->sent = 0;
//		fi->direct_io = 1;
//		fi->fh = (unsigned long)masterinfo;
//		fuse_reply_open(req, fi);
//		return;
//	}

	if (ino==MASTERINFO_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			fuse_reply_err(req,EACCES);
			oplog_printf(ctx,"open (%lu) (internal node: MASTERINFO): %s",(unsigned long int)ino,strerr(EACCES));
			return;
		}
		fi->fh = 0;
		fi->direct_io = 0;
		fi->keep_cache = 1;
		fuse_reply_open(req, fi);
		oplog_printf(ctx,"open (%lu) (internal node: MASTERINFO): OK (0,1)",(unsigned long int)ino);
		return;
	}

	if (ino==STATS_INODE) {
		sinfo *statsinfo;
//		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
//			stats_reset_all();
//			fuse_reply_err(req,EACCES);
//			return;
//		}
		statsinfo = malloc(sizeof(sinfo));
		if (statsinfo==NULL) {
			fuse_reply_err(req,ENOMEM);
			oplog_printf(ctx,"open (%lu) (internal node: STATS): %s",(unsigned long int)ino,strerr(ENOMEM));
			return;
		}
		pthread_mutex_init(&(statsinfo->lock),NULL);	// make helgrind happy
		pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
		stats_show_all(&(statsinfo->buff),&(statsinfo->leng));
		statsinfo->reset = 0;
		pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
		fi->fh = (unsigned long)statsinfo;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		fuse_reply_open(req, fi);
		oplog_printf(ctx,"open (%lu) (internal node: STATS): OK (1,0)",(unsigned long int)ino);
		return;
	}

	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			fuse_reply_err(req,EACCES);
			oplog_printf(ctx,"open (%lu) (internal node: %s): %s",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",strerr(EACCES));
			return;
		}
		fi->fh = oplog_newhandle((ino==OPHISTORY_INODE)?1:0);
		fi->direct_io = 1;
		fi->keep_cache = 0;
		fuse_reply_open(req, fi);
		oplog_printf(ctx,"open (%lu) (internal node: %s): OK (1,0)",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY");
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		fi->fh = 0;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		fuse_reply_open(req, fi);
		return;
	}
*/
	oflags = 0;
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	}
	status = fs_opencheck(ino,ctx->uid,ctx->gid,oflags,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		oplog_printf(ctx,"open (%lu): %s",(unsigned long int)ino,strerr(status));
		return ;
	}

	mattr = mfs_attr_get_mattr(attr);
	fileinfo = mfs_newfileinfo(fi->flags & O_ACCMODE,ino);
	fi->fh = (unsigned long)fileinfo;
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"open (%lu) ok -> keep cache: %lu\n",(unsigned long int)ino,(unsigned long int)fi->keep_cache);
	}
	fi->direct_io = 0;
	if (fuse_reply_open(req, fi) == -ENOENT) {
		mfs_removefileinfo(fileinfo);
	}
	oplog_printf(ctx,"open (%lu): OK (%lu,%lu)",(unsigned long int)ino,(unsigned long int)fi->direct_io,(unsigned long int)fi->keep_cache);
}

void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_RELEASE);
	if (debug_mode) {
		oplog_printf(ctx,"release (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"release (%lu)\n",(unsigned long int)ino);
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		if (masterinfo!=NULL) {
//			fs_direct_close(masterinfo->sd);
//			free(masterinfo);
//		}
//		fuse_reply_err(req,0);
//		return;
//	}
	if (ino==MASTERINFO_INODE/* || ino==ATTRCACHE_INODE*/) {
		fuse_reply_err(req,0);
		oplog_printf(ctx,"release (%lu) (internal node: MASTERINFO): OK",(unsigned long int)ino);
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
			if (statsinfo->buff!=NULL) {
				free(statsinfo->buff);
			}
			if (statsinfo->reset) {
				stats_reset_all();
			}
			pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
			pthread_mutex_destroy(&(statsinfo->lock));	// make helgrind happy
			free(statsinfo);
		}
		fuse_reply_err(req,0);
		oplog_printf(ctx,"release (%lu) (internal node: STATS): OK",(unsigned long int)ino);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_releasehandle(fi->fh);
		fuse_reply_err(req,0);
		oplog_printf(ctx,"release (%lu) (internal node: %s): OK",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY");
		return;
	}
	if (fileinfo!=NULL) {
		mfs_removefileinfo(fileinfo);
	}
	fs_release(ino);
	fuse_reply_err(req,0);
	oplog_printf(ctx,"release (%lu): OK",(unsigned long int)ino);
}

void mfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	uint8_t *buff;
	uint32_t ssize;
	int err;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_READ);
	if (debug_mode) {
		if (ino!=OPLOG_INODE && ino!=OPHISTORY_INODE) {
			oplog_printf(ctx,"read (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		}
		fprintf(stderr,"read from inode %lu up to %llu bytes from position %llu\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (ino==MASTERINFO_INODE) {
		uint8_t masterinfo[14];
		fs_getmasterlocation(masterinfo);
		masterproxy_getlocation(masterinfo);
#ifdef MASTERINFO_WITH_VERSION
		if (off>=14) {
			fuse_reply_buf(req,NULL,0);
			oplog_printf(ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		} else if (off+size>14) {
			fuse_reply_buf(req,(char*)(masterinfo+off),14-off);
			oplog_printf(ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)(14-off));
#else
		if (off>=10) {
			fuse_reply_buf(req,NULL,0);
			oplog_printf(ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		} else if (off+size>10) {
			fuse_reply_buf(req,(char*)(masterinfo+off),10-off);
			oplog_printf(ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)(10-off));
#endif
		} else {
			fuse_reply_buf(req,(char*)(masterinfo+off),size);
			oplog_printf(ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
		}
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
			if (off>=statsinfo->leng) {
				fuse_reply_buf(req,NULL,0);
				oplog_printf(ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
			} else if ((uint64_t)(off+size)>(uint64_t)(statsinfo->leng)) {
				fuse_reply_buf(req,statsinfo->buff+off,statsinfo->leng-off);
				oplog_printf(ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)(statsinfo->leng-off));
			} else {
				fuse_reply_buf(req,statsinfo->buff+off,size);
				oplog_printf(ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
			}
			pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
		} else {
			fuse_reply_buf(req,NULL,0);
			oplog_printf(ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		}
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_getdata(fi->fh,&buff,&ssize,size);
		fuse_reply_buf(req,(char*)buff,ssize);
		oplog_releasedata(fi->fh);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		uint8_t info[2];
		info[0]=dir_cache_ison()+'0';
		if (info[0]!='0' && info[0]!='1') {
			info[0]='X';
		}
		info[1]='\n';
		if (off>2) {
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>2) {
			fuse_reply_buf(req,(char*)(info+off),2-off);
		} else {
			fuse_reply_buf(req,(char*)(info+off),size);
		}
		return;
	}
*/
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		oplog_printf(ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		return;
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		if (masterinfo->sent) {
//			int rsize;
//			buff = malloc(size);
//			rsize = fs_direct_read(masterinfo->sd,buff,size);
//			fuse_reply_buf(req,(char*)buff,rsize);
//			//syslog(LOG_WARNING,"master received: %d/%llu",rsize,(unsigned long long int)size);
//			free(buff);
//		} else {
//			syslog(LOG_WARNING,"master: read before write");
//			fuse_reply_buf(req,NULL,0);
//		}
//		return;
//	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		fuse_reply_err(req,EFBIG);
		oplog_printf(ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EFBIG));
		return;
	}
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITEONLY) {
		pthread_mutex_unlock(&(fileinfo->lock));
		fuse_reply_err(req,EACCES);
		oplog_printf(ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		return;
	}
	if (fileinfo->mode==IO_WRITE) {
		err = write_data_flush(fileinfo->data);
		if (err!=0) {
			pthread_mutex_unlock(&(fileinfo->lock));
			fuse_reply_err(req,err);
			if (debug_mode) {
				fprintf(stderr,"IO error occured while writting inode %lu\n",(unsigned long int)ino);
			}
			oplog_printf(ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
			return;
		}
		write_data_end(fileinfo->data);
	}
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_READ;
		fileinfo->data = read_data_new(ino);
	}
	write_data_flush_inode(ino);
	ssize = size;
	buff = NULL;	// use internal 'readdata' buffer
	err = read_data(fileinfo->data,off,&ssize,&buff);
	if (err!=0) {
		fuse_reply_err(req,err);
		if (debug_mode) {
			fprintf(stderr,"IO error occured while reading inode %lu\n",(unsigned long int)ino);
		}
		oplog_printf(ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
	} else {
		fuse_reply_buf(req,(char*)buff,ssize);
		if (debug_mode) {
			fprintf(stderr,"%"PRIu32" bytes have been read from inode %lu\n",ssize,(unsigned long int)ino);
		}
		oplog_printf(ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)ssize);
	}
	read_data_freebuff(fileinfo->data);
	pthread_mutex_unlock(&(fileinfo->lock));
}

void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_WRITE);
	if (debug_mode) {
		oplog_printf(ctx,"write (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		fprintf(stderr,"write to inode %lu %llu bytes at position %llu\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (ino==MASTERINFO_INODE || ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		fuse_reply_err(req,EACCES);
		oplog_printf(ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
			statsinfo->reset=1;
			pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
		}
		fuse_reply_write(req,size);
		oplog_printf(ctx,"write (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		if (off==0 && size>0 && buf[0]>='0' && buf[0]<='1') {
			dir_cache_user_switch(buf[0]-'0');
			newdircache = buf[0]-'0';
		}
		fuse_reply_write(req,size);
		return;
	}
*/
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		oplog_printf(ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		return;
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		int wsize;
//		masterinfo->sent=1;
//		wsize = fs_direct_write(masterinfo->sd,(const uint8_t*)buf,size);
//		//syslog(LOG_WARNING,"master sent: %d/%llu",wsize,(unsigned long long int)size);
//		fuse_reply_write(req,wsize);
//		return;
//	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		fuse_reply_err(req, EFBIG);
		oplog_printf(ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EFBIG));
		return;
	}
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_READONLY) {
		pthread_mutex_unlock(&(fileinfo->lock));
		fuse_reply_err(req,EACCES);
		oplog_printf(ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		return;
	}
	if (fileinfo->mode==IO_READ) {
		read_data_end(fileinfo->data);
	}
	if (fileinfo->mode==IO_READ || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_WRITE;
		fileinfo->data = write_data_new(ino);
	}
	err = write_data(fileinfo->data,off,size,(const uint8_t*)buf);
	if (err!=0) {
		pthread_mutex_unlock(&(fileinfo->lock));
		fuse_reply_err(req,err);
		if (debug_mode) {
			fprintf(stderr,"IO error occured while writting inode %lu\n",(unsigned long int)ino);
		}
		oplog_printf(ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
	} else {
		pthread_mutex_unlock(&(fileinfo->lock));
		fuse_reply_write(req,size);
		if (debug_mode) {
			fprintf(stderr,"%llu bytes have been written to inode %lu\n",(unsigned long long int)size,(unsigned long int)ino);
		}
		oplog_printf(ctx,"write (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
	}
}

void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_FLUSH);
	if (debug_mode) {
		oplog_printf(ctx,"flush (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"flush (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,0);
		oplog_printf(ctx,"flush (%lu): OK",(unsigned long int)ino);
		return;
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		oplog_printf(ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		return;
	}
//	syslog(LOG_NOTICE,"remove_locks inode:%lu owner:%llu",(unsigned long int)ino,(unsigned long long int)fi->lock_owner);
	err = 0;
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	fuse_reply_err(req,err);
	if (err!=0) {
		oplog_printf(ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(err));
	} else {
		oplog_printf(ctx,"flush (%lu): OK",(unsigned long int)ino);
	}
}

void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_FSYNC);
	if (debug_mode) {
		oplog_printf(ctx,"fsync (%lu,%d) ...",(unsigned long int)ino,datasync);
		fprintf(stderr,"fsync (%lu,%d)\n",(unsigned long int)ino,datasync);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,0);
		oplog_printf(ctx,"fsync (%lu,%d): OK",(unsigned long int)ino,datasync);
		return;
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		oplog_printf(ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(EBADF));
		return;
	}
	err = 0;
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	fuse_reply_err(req,err);
	if (err!=0) {
		oplog_printf(ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(err));
	} else {
		oplog_printf(ctx,"fsync (%lu,%d): OK",(unsigned long int)ino,datasync);
	}
}

#if defined(__APPLE__)
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position) {
#else
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags) {
	uint32_t position=0;
#endif
	uint32_t nleng;
	int status;
	uint8_t mode;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_SETXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d) ...",(unsigned long int)ino,name,(unsigned long long int)size,flags);
		fprintf(stderr,"setxattr (%lu,%s,%llu,%d)",(unsigned long int)ino,name,(unsigned long long int)size,flags);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,EPERM);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EPERM));
		return;
	}
	if (size>MFS_XATTR_SIZE_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns E2BIG here
		fuse_reply_err(req,E2BIG);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(E2BIG));
#else
		fuse_reply_err(req,ERANGE);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(ERANGE));
#endif
		return;
	}
	// ACL's - will be implemented separately
	if (strcmp(name,"system.posix_acl_default")==0 || strcmp(name,"system.posix_acl_access")==0) {
		fuse_reply_err(req,ENOTSUP);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(ENOTSUP));
		return;
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		fuse_reply_err(req,EPERM);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EPERM));
#else
		fuse_reply_err(req,ERANGE);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(ERANGE));
#endif
		return;
	}
	if (nleng==0) {
		fuse_reply_err(req,EINVAL);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EINVAL));
		return;
	}
#if defined(XATTR_CREATE) && defined(XATTR_REPLACE)
	if ((flags&XATTR_CREATE) && (flags&XATTR_REPLACE)) {
		fuse_reply_err(req,EINVAL);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EINVAL));
		return;
	}
	mode = (flags==XATTR_CREATE)?MFS_XATTR_CREATE_ONLY:(flags==XATTR_REPLACE)?MFS_XATTR_REPLACE_ONLY:MFS_XATTR_CREATE_OR_REPLACE;
#else
	mode = 0;
#endif
	(void)position;
	status = fs_setxattr(ino,0,ctx->uid,ctx->gid,nleng,(const uint8_t*)name,(uint32_t)size,(const uint8_t*)value,mode);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req,status);
		oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(status));
		return;
	}
	fuse_reply_err(req,0);
	oplog_printf(ctx,"setxattr (%lu,%s,%llu,%d): OK",(unsigned long int)ino,name,(unsigned long long int)size,flags);
}

#if defined(__APPLE__)
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position) {
#else
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) {
	uint32_t position=0;
#endif /* __APPLE__ */
	uint32_t nleng;
	int status;
	uint8_t mode;
	const uint8_t *buff;
	uint32_t leng;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_GETXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"getxattr (%lu,%s,%llu) ...",(unsigned long int)ino,name,(unsigned long long int)size);
		fprintf(stderr,"getxattr (%lu,%s,%llu)",(unsigned long int)ino,name,(unsigned long long int)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,EPERM);
		oplog_printf(ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(EPERM));
		return;
	}
	// ACL's - will be implemented separately
	if (strcmp(name,"system.posix_acl_default")==0 || strcmp(name,"system.posix_acl_access")==0) {
		fuse_reply_err(req,ENOTSUP);
		oplog_printf(ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(ENOTSUP));
		return;
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		fuse_reply_err(req,EPERM);
		oplog_printf(ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(EPERM));
#else
		fuse_reply_err(req,ERANGE);
		oplog_printf(ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(ERANGE));
#endif
		return;
	}
	if (nleng==0) {
		fuse_reply_err(req,EINVAL);
		oplog_printf(ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(EINVAL));
		return;
	}
	if (size==0) {
		mode = MFS_XATTR_LENGTH_ONLY;
	} else {
		mode = MFS_XATTR_GETA_DATA;
	}
	(void)position;
	status = fs_getxattr(ino,0,ctx->uid,ctx->gid,nleng,(const uint8_t*)name,mode,&buff,&leng);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req,status);
		oplog_printf(ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(status));
		return;
	}
	if (size==0) {
		fuse_reply_xattr(req,leng);
		oplog_printf(ctx,"getxattr (%lu,%s,%llu): OK (%"PRIu32")",(unsigned long int)ino,name,(unsigned long long int)size,leng);
	} else {
		if (leng>size) {
			fuse_reply_err(req,ERANGE);
			oplog_printf(ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(ERANGE));
		} else {
			fuse_reply_buf(req,(const char*)buff,leng);
			oplog_printf(ctx,"getxattr (%lu,%s,%llu): OK (%"PRIu32")",(unsigned long int)ino,name,(unsigned long long int)size,leng);
		}
	}
}

void mfs_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size) {
	const uint8_t *buff;
	uint32_t leng;
	int status;
	uint8_t mode;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_LISTXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"listxattr (%lu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size);
		fprintf(stderr,"listxattr (%lu,%llu)",(unsigned long int)ino,(unsigned long long int)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,EPERM);
		oplog_printf(ctx,"listxattr (%lu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,strerr(EPERM));
		return;
	}
	if (size==0) {
		mode = MFS_XATTR_LENGTH_ONLY;
	} else {
		mode = MFS_XATTR_GETA_DATA;
	}
	status = fs_listxattr(ino,0,ctx->uid,ctx->gid,mode,&buff,&leng);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req,status);
		oplog_printf(ctx,"listxattr (%lu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,strerr(status));
		return;
	}
	if (size==0) {
		fuse_reply_xattr(req,leng);
		oplog_printf(ctx,"listxattr (%lu,%llu): OK (%"PRIu32")",(unsigned long int)ino,(unsigned long long int)size,leng);
	} else {
		if (leng>size) {
			fuse_reply_err(req,ERANGE);
			oplog_printf(ctx,"listxattr (%lu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,strerr(ERANGE));
		} else {
			fuse_reply_buf(req,(const char*)buff,leng);
			oplog_printf(ctx,"listxattr (%lu,%llu): OK (%"PRIu32")",(unsigned long int)ino,(unsigned long long int)size,leng);
		}
	}
}

void mfs_removexattr (fuse_req_t req, fuse_ino_t ino, const char *name) {
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	mfs_stats_inc(OP_REMOVEXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"removexattr (%lu,%s) ...",(unsigned long int)ino,name);
		fprintf(stderr,"removexattr (%lu,%s)",(unsigned long int)ino,name);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,EPERM);
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EPERM));
		return;
	}
	// ACL's - will be implemented separately
	if (strcmp(name,"system.posix_acl_default")==0 || strcmp(name,"system.posix_acl_access")==0) {
		fuse_reply_err(req,ENOTSUP);
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(ENOTSUP));
		return;
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		fuse_reply_err(req,EPERM);
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EPERM));
#else
		fuse_reply_err(req,ERANGE);
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(ERANGE));
#endif
		return;
	}
	if (nleng==0) {
		fuse_reply_err(req,EINVAL);
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EINVAL));
		return;
	}
	status = fs_removexattr(ino,0,ctx->uid,ctx->gid,nleng,(const uint8_t*)name);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req,status);
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(status));
	} else {
		fuse_reply_err(req,0);
		oplog_printf(ctx,"removexattr (%lu,%s): OK",(unsigned long int)ino,name);
	}
}

#if FUSE_USE_VERSION >= 26
/*
void mfs_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock) {
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	oplog_printf(ctx,"getlk (inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u)\n",(unsigned long int)ino,(unsigned long long int)fi->lock_owner,(unsigned long long int)lock->l_start,(unsigned long long int)lock->l_len,(unsigned int)lock->l_whence,(unsigned int)lock->l_type);
	if (debug_mode) {
		fprintf(stderr,"getlk (inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u)\n",(unsigned long int)ino,(unsigned long long int)fi->lock_owner,(unsigned long long int)lock->l_start,(unsigned long long int)lock->l_len,(unsigned int)lock->l_whence,(unsigned int)lock->l_type);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,EPERM);
		return;
	}
//	syslog(LOG_NOTICE,"get lock inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u",(unsigned long int)ino,fi->lock_owner,lock->l_start,lock->l_len,lock->l_whence,lock->l_type);
	lock->l_type = F_UNLCK;
	fuse_reply_lock(req,lock);
}

typedef struct _setlk_interrupt {
	fuse_ino_t inode;
	uint64_t ownerid;
	uint64_t start;
	uint64_t len;
	uint8_t whence;
	uint8_t type;
	uint8_t fired;
} setlk_interrupt;

void mfs_setlk_interrupt(fuse_req_t req,void *data) {
	const struct fuse_ctx *ctx;
	setlk_interrupt *d = (setlk_interrupt*)data;

	ctx = fuse_req_ctx(req);
	oplog_printf(ctx,"setlk interrupt (inode:%lu owner:%"PRIu64" lstart:%"PRIu64" llen:%"PRIu64" lwhence:%"PRIu8" ltype:%"PRIu8")\n",(unsigned long int)d->inode,d->ownerid,d->start,d->len,d->whence,d->type);
	if (debug_mode) {
		fprintf(stderr,"setlk interrupt (inode:%lu owner:%"PRIu64" lstart:%"PRIu64" llen:%"PRIu64" lwhence:%"PRIu8" ltype:%"PRIu8")\n",(unsigned long int)d->inode,d->ownerid,d->start,d->len,d->whence,d->type);
	}
	d->fired = 1;
}

void mfs_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sl) {
	const struct fuse_ctx *ctx;

	ctx = fuse_req_ctx(req);
	oplog_printf(ctx,"setlk (inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u sleep:%u)\n",(unsigned long int)ino,(unsigned long long int)fi->lock_owner,(unsigned long long int)lock->l_start,(unsigned long long int)lock->l_len,(unsigned int)lock->l_whence,(unsigned int)lock->l_type,(unsigned int)sl);
	if (debug_mode) {
		fprintf(stderr,"setlk (inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u sleep:%u)\n",(unsigned long int)ino,(unsigned long long int)fi->lock_owner,(unsigned long long int)lock->l_start,(unsigned long long int)lock->l_len,(unsigned int)lock->l_whence,(unsigned int)lock->l_type,(unsigned int)sl);
	}
	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req,EPERM);
		return;
	}
//	syslog(LOG_NOTICE,"set lock inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u sleep:%u",(unsigned long int)ino,fi->lock_owner,lock->l_start,lock->l_len,lock->l_whence,lock->l_type,sl);
	if (sl) {
		uint32_t i;
		setlk_interrupt data;
		data.inode = ino;
		data.ownerid = fi->lock_owner;
		data.start = lock->l_start;
		data.len = lock->l_len;
		data.whence = lock->l_whence;
		data.type = lock->l_type;
		data.fired = 0;
		fuse_req_interrupt_func(req,mfs_setlk_interrupt,&data);
		for (i=0 ; i<20000 ; i++) {
			usleep(1000);
			if (data.fired) {
				break;
			}
		}
		fuse_req_interrupt_func(req,NULL,NULL);
	}
	fuse_reply_err(req,0);
}
*/
#endif

void mfs_init(int debug_mode_in,int keep_cache_in,double direntry_cache_timeout_in,double entry_cache_timeout_in,double attr_cache_timeout_in,int mkdir_copy_sgid_in,int sugid_clear_mode_in) {
	const char* sugid_clear_mode_strings[] = {SUGID_CLEAR_MODE_STRINGS};
	debug_mode = debug_mode_in;
	keep_cache = keep_cache_in;
	direntry_cache_timeout = direntry_cache_timeout_in;
	entry_cache_timeout = entry_cache_timeout_in;
	attr_cache_timeout = attr_cache_timeout_in;
	mkdir_copy_sgid = mkdir_copy_sgid_in;
	sugid_clear_mode = sugid_clear_mode_in;
	if (debug_mode) {
		fprintf(stderr,"cache parameters: file_keep_cache=%s direntry_cache_timeout=%.2lf entry_cache_timeout=%.2lf attr_cache_timeout=%.2lf\n",(keep_cache==1)?"always":(keep_cache==2)?"never":"auto",direntry_cache_timeout,entry_cache_timeout,attr_cache_timeout);
		fprintf(stderr,"mkdir copy sgid=%d\nsugid clear mode=%s\n",mkdir_copy_sgid_in,(sugid_clear_mode_in<SUGID_CLEAR_MODE_OPTIONS)?sugid_clear_mode_strings[sugid_clear_mode_in]:"???");
	}
	mfs_statsptr_init();
}
