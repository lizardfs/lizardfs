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
#include "datapack.h"
#include "mastercomm.h"
#include "readdata.h"
#include "writedata.h"
#include "MFSCommunication.h"

#include "dirattrcache.h"

#if MFS_ROOT_ID != FUSE_ROOT_ID
#error FUSE_ROOT_ID is not equal to MFS_ROOT_ID
#endif

#define READDIR_BUFFSIZE 50000

#define MAX_FILE_SIZE MFS_MAX_FILE_SIZE

#define PKGVERSION ((VERSMAJ)*1000000+(VERSMID)*1000+(VERSMIN))

// #define MASTER_NAME ".master"
// #define MASTER_INODE 0x7FFFFFFF
// 0x01b6 == 0666
// static uint8_t masterattr[35]={'f', 0x01,0xB6, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define MASTERINFO_NAME ".masterinfo"
#define MASTERINFO_INODE 0x7FFFFFFF
// 0x0124 == 0b100100100 == 0444
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10};

#define STATS_NAME ".stats"
#define STATS_INODE 0x7FFFFFF0
// 0x01A4 == 0b110100100 == 0644
static uint8_t statsattr[35]={'f', 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

typedef struct _sinfo {
	char *buff;
	uint32_t leng;
	uint8_t reset;
} sinfo;

typedef struct _dirbuf {
	int wasread;
	uid_t uid;
	gid_t gid;
	uint8_t *p;
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

//static int local_mode = 0;
//static int no_attr_cache = 0;

enum {
	OP_STATFS = 0,
	OP_ACCESS,
	OP_LOOKUP,
	OP_DIRCACHE_LOOKUP,
	OP_GETATTR,
	OP_DIRCACHE_GETATTR,
	OP_SETATTR,
	OP_MKNOD,
	OP_UNLINK,
	OP_MKDIR,
	OP_RMDIR,
	OP_SYMLINK,
	OP_READLINK,
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
	STATNODES
};

static uint64_t *statsptr[STATNODES];
static pthread_mutex_t statsptrlock = PTHREAD_MUTEX_INITIALIZER;

void mfs_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"fuse_ops");
	statsptr[OP_FSYNC] = stats_get_counterptr(stats_get_subnode(s,"fsync"));
	statsptr[OP_FLUSH] = stats_get_counterptr(stats_get_subnode(s,"flush"));
	statsptr[OP_WRITE] = stats_get_counterptr(stats_get_subnode(s,"write"));
	statsptr[OP_READ] = stats_get_counterptr(stats_get_subnode(s,"read"));
	statsptr[OP_RELEASE] = stats_get_counterptr(stats_get_subnode(s,"release"));
	statsptr[OP_OPEN] = stats_get_counterptr(stats_get_subnode(s,"open"));
	statsptr[OP_CREATE] = stats_get_counterptr(stats_get_subnode(s,"create"));
	statsptr[OP_RELEASEDIR] = stats_get_counterptr(stats_get_subnode(s,"releasedir"));
	statsptr[OP_READDIR] = stats_get_counterptr(stats_get_subnode(s,"readdir"));
	statsptr[OP_OPENDIR] = stats_get_counterptr(stats_get_subnode(s,"opendir"));
	statsptr[OP_LINK] = stats_get_counterptr(stats_get_subnode(s,"link"));
	statsptr[OP_RENAME] = stats_get_counterptr(stats_get_subnode(s,"rename"));
	statsptr[OP_READLINK] = stats_get_counterptr(stats_get_subnode(s,"readlink"));
	statsptr[OP_SYMLINK] = stats_get_counterptr(stats_get_subnode(s,"symlink"));
	statsptr[OP_RMDIR] = stats_get_counterptr(stats_get_subnode(s,"rmdir"));
	statsptr[OP_MKDIR] = stats_get_counterptr(stats_get_subnode(s,"mkdir"));
	statsptr[OP_UNLINK] = stats_get_counterptr(stats_get_subnode(s,"unlink"));
	statsptr[OP_MKNOD] = stats_get_counterptr(stats_get_subnode(s,"mknod"));
	statsptr[OP_SETATTR] = stats_get_counterptr(stats_get_subnode(s,"setattr"));
	statsptr[OP_GETATTR] = stats_get_counterptr(stats_get_subnode(s,"getattr"));
	if (usedircache) {
		statsptr[OP_DIRCACHE_GETATTR] = stats_get_counterptr(stats_get_subnode(s,"getattr-cached"));
	}
	statsptr[OP_LOOKUP] = stats_get_counterptr(stats_get_subnode(s,"lookup"));
	if (usedircache) {
		statsptr[OP_DIRCACHE_LOOKUP] = stats_get_counterptr(stats_get_subnode(s,"lookup-cached"));
	}
	statsptr[OP_ACCESS] = stats_get_counterptr(stats_get_subnode(s,"access"));
	statsptr[OP_STATFS] = stats_get_counterptr(stats_get_subnode(s,"statfs"));
}

void mfs_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		pthread_mutex_lock(&statsptrlock);
		(*statsptr[id])++;
		pthread_mutex_unlock(&statsptrlock);
	}
}

#ifndef EDQUOT
#define EDQUOT ENOSPC
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
	switch (attrtype) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR | ( attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
		stbuf->st_blocks = (attrlength+511)/512;
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK | ( attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
		stbuf->st_blocks = (attrlength+511)/512;
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG | ( attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
		stbuf->st_blocks = (attrlength+511)/512;
		break;
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO | ( attrmode & 07777);
		stbuf->st_size = 0;
		stbuf->st_blocks = 0;
		break;
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK | ( attrmode & 07777);
		stbuf->st_size = 0;
		stbuf->st_blocks = 0;
		break;
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK | ( attrmode & 07777);
		attrrdev = get32bit(&ptr);
		stbuf->st_rdev = attrrdev;
		stbuf->st_size = 0;
		stbuf->st_blocks = 0;
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR | ( attrmode & 07777);
		attrrdev = get32bit(&ptr);
		stbuf->st_rdev = attrrdev;
		stbuf->st_size = 0;
		stbuf->st_blocks = 0;
		break;
	default:
		stbuf->st_mode = 0;
	}
	stbuf->st_uid = attruid;
	stbuf->st_gid = attrgid;
	stbuf->st_atime = attratime;
	stbuf->st_mtime = attrmtime;
	stbuf->st_ctime = attrctime;
	stbuf->st_nlink = attrnlink;
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
	ctx = fuse_req_ctx(req);
//	if (ino==MASTER_INODE) {
//		fuse_reply_err(req,0);
//		return;
//	}
	if (ino==MASTERINFO_INODE || ino==STATS_INODE) {
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
		fuse_reply_err(req, status);
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
	uint8_t mattr;
	int status;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"lookup (%lu,%s)\n",(unsigned long int)parent,name);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		mfs_stats_inc(OP_LOOKUP);
		fuse_reply_err(req, ENAMETOOLONG);
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
			mfs_stats_inc(OP_LOOKUP);
			return ;
		}
		if (strcmp(name,STATS_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = STATS_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(STATS_INODE,statsattr,&e.attr);
			fuse_reply_entry(req, &e);
			mfs_stats_inc(OP_LOOKUP);
			return ;
		}
	}
	ctx = fuse_req_ctx(req);
	if (usedircache && dcache_lookup(ctx,parent,nleng,(const uint8_t*)name,&inode,attr)) {
		if (debug_mode) {
			fprintf(stderr,"lookup: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_LOOKUP);
		status = 0;
	} else {
		mfs_stats_inc(OP_LOOKUP);
		status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid,&inode,attr);
		status = mfs_errorconv(status);
	}
	if (status!=0) {
		fuse_reply_err(req, status);
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
}

void mfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint64_t maxfleng;
	struct stat o_stbuf;
	uint8_t attr[35];
	int status;
	const struct fuse_ctx *ctx;
	(void)fi;

//	mfs_stats_inc(OP_GETATTR);
	if (debug_mode) {
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
		return;
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		mfs_stats_inc(OP_GETATTR);
		return;
	}
	ctx = fuse_req_ctx(req);
//	if (write_data_flush_inode(ino)) {
//		mfs_stats_inc(OP_GETATTR);
//		status = fs_getattr(ino,ctx->uid,ctx->gid,attr);
//		status = mfs_errorconv(status);
	if (usedircache && dcache_getattr(ctx,ino,attr)) {
		if (debug_mode) {
			fprintf(stderr,"getattr: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_GETATTR);
		status = 0;
	} else {
		mfs_stats_inc(OP_GETATTR);
		status = fs_getattr(ino,ctx->uid,ctx->gid,attr);
		status = mfs_errorconv(status);
	}
	if (status!=0) {
		fuse_reply_err(req, status);
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
	fuse_reply_attr(req, &o_stbuf, (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout);
}

void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	uint64_t maxfleng;
	uint8_t attr[35];
	int status;
	const struct fuse_ctx *ctx;
	uint8_t setmask = 0;

	mfs_stats_inc(OP_SETATTR);
	if (debug_mode) {
		fprintf(stderr,"setattr (%lu,%u,(%04o,%ld,%ld,%lu,%lu,%llu))\n",(unsigned long int)ino,to_set,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size));
	}
	if (/*ino==MASTER_INODE || */ino==MASTERINFO_INODE) {
		fuse_reply_err(req, EPERM);
		return;
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	status = EINVAL;
	ctx = fuse_req_ctx(req);
	if ((to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME|FUSE_SET_ATTR_SIZE)) == 0) {	// change other flags or change nothing
//		status = fs_getattr(ino,ctx->uid,ctx->gid,attr);
		status = fs_setattr(ino,ctx->uid,ctx->gid,0,0,0,0,0,0,attr);	// ext3 compatibility - change ctime during this operation (usually chown(-1,-1))
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
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
		status = fs_setattr(ino,ctx->uid,ctx->gid,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			return;
		}
	}
	if (to_set & FUSE_SET_ATTR_SIZE) {
		if (stbuf->st_size<0) {
			fuse_reply_err(req, EINVAL);
			return;
		}
		if (stbuf->st_size>=MAX_FILE_SIZE) {
			fuse_reply_err(req, EFBIG);
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
			return;
		}
	}
	if (status!=0) {	// should never happend but better check than sorry
		fuse_reply_err(req, status);
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
	fuse_reply_attr(req, &o_stbuf, (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout);
}

void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	uint8_t type;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_MKNOD);
	if (debug_mode) {
		fprintf(stderr,"mknod (%lu,%s,%04o,%08lX)\n",(unsigned long int)parent,name,(unsigned int)mode,(unsigned long int)rdev);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
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
		return;
	}

	if (parent==FUSE_ROOT_ID) {
		if (/*strcmp(name,MASTER_NAME)==0 || */strcmp(name,MASTERINFO_NAME)==0 || strcmp(name,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}

	ctx = fuse_req_ctx(req);
	status = fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,ctx->uid,ctx->gid,rdev,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_UNLINK);
	if (debug_mode) {
		fprintf(stderr,"unlink (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (/*strcmp(name,MASTER_NAME)==0 || */strcmp(name,MASTERINFO_NAME)==0 || strcmp(name,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}

	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	ctx = fuse_req_ctx(req);
	status = fs_unlink(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		fuse_reply_err(req, 0);
	}
}

void mfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_MKDIR);
	if (debug_mode) {
		fprintf(stderr,"mkdir (%lu,%s,%04o)\n",(unsigned long int)parent,name,(unsigned int)mode);
	}
	if (parent==FUSE_ROOT_ID) {
		if (/*strcmp(name,MASTER_NAME)==0 || */strcmp(name,MASTERINFO_NAME)==0 || strcmp(name,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	ctx = fuse_req_ctx(req);
	status = fs_mkdir(parent,nleng,(const uint8_t*)name,mode,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:direntry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_RMDIR);
	if (debug_mode) {
		fprintf(stderr,"rmdir (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (/*strcmp(name,MASTER_NAME)==0 || */strcmp(name,MASTERINFO_NAME)==0 || strcmp(name,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	ctx = fuse_req_ctx(req);
	status = fs_rmdir(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		fuse_reply_err(req, 0);
	}
}

void mfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_SYMLINK);
	if (debug_mode) {
		fprintf(stderr,"symlink (%s,%lu,%s)\n",path,(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (/*strcmp(name,MASTER_NAME)==0 || */strcmp(name,MASTERINFO_NAME)==0 || strcmp(name,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	ctx = fuse_req_ctx(req);
	status = fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)path,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_readlink(fuse_req_t req, fuse_ino_t ino) {
	int status;
	const uint8_t *path;

	mfs_stats_inc(OP_READLINK);
	if (debug_mode) {
		fprintf(stderr,"readlink (%lu)\n",(unsigned long int)ino);
	}
	status = fs_readlink(ino,&path);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		fuse_reply_readlink(req, (char*)path);
	}
}

void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
	uint32_t nleng,newnleng;
	int status;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_RENAME);
	if (debug_mode) {
		fprintf(stderr,"rename (%lu,%s,%lu,%s)\n",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
	}
	if (parent==FUSE_ROOT_ID) {
		if (/*strcmp(name,MASTER_NAME)==0 || */strcmp(name,MASTERINFO_NAME)==0 || strcmp(name,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	if (newparent==FUSE_ROOT_ID) {
		if (/*strcmp(newname,MASTER_NAME)==0 || */strcmp(newname,MASTERINFO_NAME)==0 || strcmp(newname,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	ctx = fuse_req_ctx(req);
	status = fs_rename(parent,nleng,(const uint8_t*)name,newparent,newnleng,(const uint8_t*)newname,ctx->uid,ctx->gid);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		fuse_reply_err(req, 0);
	}
}

void mfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
	uint32_t newnleng;
	int status;
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	uint8_t mattr;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_LINK);
	if (debug_mode) {
		fprintf(stderr,"link (%lu,%lu,%s)\n",(unsigned long int)ino,(unsigned long int)newparent,newname);
	}
	if (/*ino==MASTER_INODE || */ino==MASTERINFO_INODE || ino==STATS_INODE) {
		fuse_reply_err(req, EACCES);
		return;
	}
	if (newparent==FUSE_ROOT_ID) {
		if (/*strcmp(newname,MASTER_NAME)==0 || */strcmp(newname,MASTERINFO_NAME)==0 || strcmp(newname,STATS_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

//	write_data_flush_inode(ino);
	ctx = fuse_req_ctx(req);
	status = fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	dirbuf *dirinfo;
	int status;
	const struct fuse_ctx *ctx;

	mfs_stats_inc(OP_OPENDIR);
	if (debug_mode) {
		fprintf(stderr,"opendir (%lu)\n",(unsigned long int)ino);
	}
	if (/*ino==MASTER_INODE || */ino==MASTERINFO_INODE || ino==STATS_INODE) {
		fuse_reply_err(req, ENOTDIR);
	}
	ctx = fuse_req_ctx(req);
	status = fs_access(ino,ctx->uid,ctx->gid,MODE_MASK_R);	// at least test rights
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		dirinfo = malloc(sizeof(dirbuf));
		pthread_mutex_init(&(dirinfo->lock),NULL);
		dirinfo->p = NULL;
		dirinfo->size = 0;
		dirinfo->dcache = NULL;
		dirinfo->wasread = 0;
		fi->fh = (unsigned long)dirinfo;
		if (fuse_reply_open(req,fi) == -ENOENT) {
			fi->fh = 0;
			pthread_mutex_destroy(&(dirinfo->lock));
			free(dirinfo);
		}
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

	mfs_stats_inc(OP_READDIR);
	if (debug_mode) {
		fprintf(stderr,"readdir (%lu,%llu,%llu)\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (off<0) {
		fuse_reply_err(req,EINVAL);
		return;
	}
	pthread_mutex_lock(&(dirinfo->lock));
	if (dirinfo->wasread==0 || (dirinfo->wasread==1 && off==0)) {
		const uint8_t *dbuff;
		uint32_t dsize;
		ctx = fuse_req_ctx(req);
		if (usedircache) {
			status = fs_getdir_plus(ino,ctx->uid,ctx->gid,&dbuff,&dsize);
		} else {
			status = fs_getdir(ino,ctx->uid,ctx->gid,&dbuff,&dsize);
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			pthread_mutex_unlock(&(dirinfo->lock));
			return;
		}
		if (dirinfo->dcache) {
			dcache_release(dirinfo->dcache);
		}
		if (dirinfo->p) {
			free(dirinfo->p);
		}
		dirinfo->p = malloc(dsize);
		if (dirinfo->p == NULL) {
			fuse_reply_err(req,EINVAL);
			pthread_mutex_unlock(&(dirinfo->lock));
			return;
		}
		memcpy(dirinfo->p,dbuff,dsize);
		dirinfo->size = dsize;
		if (usedircache) {
			dirinfo->dcache = dcache_new(ctx,ino,dirinfo->p,dirinfo->size);
		}
	}
	dirinfo->wasread=1;

	if (off>=(off_t)(dirinfo->size)) {
		fuse_reply_buf(req, NULL, 0);
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
			off+=nleng+(usedircache?40:6);
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				if (usedircache) {
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
	}
	pthread_mutex_unlock(&(dirinfo->lock));
}

void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	(void)ino;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));

	mfs_stats_inc(OP_RELEASEDIR);
	if (debug_mode) {
		fprintf(stderr,"releasedir (%lu)\n",(unsigned long int)ino);
	}
	pthread_mutex_lock(&(dirinfo->lock));
	pthread_mutex_unlock(&(dirinfo->lock));
	pthread_mutex_destroy(&(dirinfo->lock));
	if (dirinfo->dcache) {
		dcache_release(dirinfo->dcache);
	}
	if (dirinfo->p) {
		free(dirinfo->p);
	}
	free(dirinfo);
	fi->fh = 0;
	fuse_reply_err(req,0);
}


static finfo* mfs_newfileinfo(uint8_t accmode,uint32_t inode) {
	finfo *fileinfo;
	fileinfo = malloc(sizeof(finfo));
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
	pthread_mutex_init(&(fileinfo->lock),NULL);
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
	uint8_t mattr;
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;
	finfo *fileinfo;

	mfs_stats_inc(OP_CREATE);
	if (debug_mode) {
		fprintf(stderr,"create (%lu,%s,%04o)\n",(unsigned long int)parent,name,(unsigned int)mode);
	}
	if (parent==FUSE_ROOT_ID) {
		if (/*strcmp(name,MASTER_NAME)==0 || */strcmp(name,MASTERINFO_NAME)==0 || strcmp(name,STATS_NAME)==0) {
			fuse_reply_err(req,EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
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
	}

	ctx = fuse_req_ctx(req);
	status = fs_mknod(parent,nleng,(const uint8_t*)name,TYPE_FILE,mode&07777,ctx->uid,ctx->gid,0,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		return;
	}
	status = fs_opencheck(inode,ctx->uid,ctx->gid,oflags,NULL);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
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
}

void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint8_t oflags;
	uint8_t attr[35];
	uint8_t mattr;
	int status;
	const struct fuse_ctx *ctx;
	finfo *fileinfo;

	mfs_stats_inc(OP_OPEN);
	if (debug_mode) {
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
			return;
		}
		fi->fh = 0;
		fi->direct_io = 0;
		fi->keep_cache = 1;
		fuse_reply_open(req, fi);
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
			return;
		}
		stats_show_all(&(statsinfo->buff),&(statsinfo->leng));
		statsinfo->reset = 0;
		fi->direct_io = 1;
		fi->fh = (unsigned long)statsinfo;
		fuse_reply_open(req, fi);
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
	ctx = fuse_req_ctx(req);
	status = fs_opencheck(ino,ctx->uid,ctx->gid,oflags,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
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
//	fi->direct_io = 1;
	if (fuse_reply_open(req, fi) == -ENOENT) {
		mfs_removefileinfo(fileinfo);
	}
}

void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);

	mfs_stats_inc(OP_RELEASE);
	if (debug_mode) {
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
	if (ino==MASTERINFO_INODE) {
		fuse_reply_err(req,0);
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			if (statsinfo->buff!=NULL) {
				free(statsinfo->buff);
			}
			if (statsinfo->reset) {
				stats_reset_all();
			}
			free(statsinfo);
		}
		fuse_reply_err(req,0);
		return;
	}
	if (fileinfo!=NULL) {
		mfs_removefileinfo(fileinfo);
	}
	fs_release(ino);
	fuse_reply_err(req,0);
}


void mfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	uint8_t *buff;
	uint32_t ssize;
	int err;

	mfs_stats_inc(OP_READ);
	if (debug_mode) {
		fprintf(stderr,"read from inode %lu up to %llu bytes from position %llu\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (ino==MASTERINFO_INODE) {
		uint8_t masterinfo[10];
		fs_getmasterlocation(masterinfo);
		if (off>=10) {
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>10) {
			fuse_reply_buf(req,(char*)(masterinfo+off),10-off);
		} else {
			fuse_reply_buf(req,(char*)(masterinfo+off),size);
		}
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (off>=statsinfo->leng) {
			fuse_reply_buf(req,NULL,0);
		} else if ((uint64_t)(off+size)>(uint64_t)(statsinfo->leng)) {
			fuse_reply_buf(req,statsinfo->buff+off,statsinfo->leng-off);
		} else {
			fuse_reply_buf(req,statsinfo->buff+off,size);
		}
		return;
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
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
	if (fileinfo->mode==IO_WRITEONLY) {
		fuse_reply_err(req,EACCES);
		return;
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		fuse_reply_err(req,EFBIG);
		return;
	}
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE) {
		err = write_data_flush(fileinfo->data);
		if (err!=0) {
			pthread_mutex_unlock(&(fileinfo->lock));
			fuse_reply_err(req,err);
			if (debug_mode) {
				fprintf(stderr,"IO error occured while writting inode %lu\n",(unsigned long int)ino);
			}
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
	} else {
		fuse_reply_buf(req,(char*)buff,ssize);
		if (debug_mode) {
			fprintf(stderr,"%"PRIu32" bytes have been read from inode %lu\n",ssize,(unsigned long int)ino);
		}
	}
	read_data_freebuff(fileinfo->data);
	pthread_mutex_unlock(&(fileinfo->lock));
}

void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;

	mfs_stats_inc(OP_WRITE);
	if (debug_mode) {
		fprintf(stderr,"write to inode %lu %llu bytes at position %llu\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (ino==MASTERINFO_INODE) {
		fuse_reply_err(req,EACCES);
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			statsinfo->reset=1;
		}
		fuse_reply_write(req,size);
		return;
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
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
	if (fileinfo->mode==IO_READONLY) {
		fuse_reply_err(req,EACCES);
		return;
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		fuse_reply_err(req, EFBIG);
		return;
	}
	pthread_mutex_lock(&(fileinfo->lock));     // to trzeba zrobiæ lepiej - wielu czytelnikow i wielu pisarzy
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
	} else {
		pthread_mutex_unlock(&(fileinfo->lock));
		fuse_reply_write(req,size);
		if (debug_mode) {
			fprintf(stderr,"%llu bytes have been written to inode %lu\n",(unsigned long long int)size,(unsigned long int)ino);
		}
	}
}

void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;

	mfs_stats_inc(OP_FLUSH);
	if (debug_mode) {
		fprintf(stderr,"flush (%lu)\n",(unsigned long int)ino);
	}
	if (/*ino==MASTER_INODE || */ino==MASTERINFO_INODE || ino==STATS_INODE) {
		fuse_reply_err(req,0);
		return;
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
//	syslog(LOG_NOTICE,"remove_locks inode:%u owner:%llu",ino,fi->lock_owner);
	err = 0;
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	fuse_reply_err(req,err);
}

void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	(void)datasync;

	mfs_stats_inc(OP_FSYNC);
	if (debug_mode) {
		fprintf(stderr,"fsync (%lu)\n",(unsigned long int)ino);
	}
	if (/*ino==MASTER_INODE || */ino==MASTERINFO_INODE || ino==STATS_INODE) {
		fuse_reply_err(req,0);
		return;
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
	err = 0;
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	fuse_reply_err(req,err);
}

#if FUSE_USE_VERSION >= 26
/*
void mfs_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock) {
	if (debug_mode) {
		fprintf(stderr,"getlk (inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u)\n",(unsigned long int)ino,fi->lock_owner,lock->l_start,lock->l_len,lock->l_whence,lock->l_type);
	}
	syslog(LOG_NOTICE,"get lock inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u",(unsigned long int)ino,fi->lock_owner,lock->l_start,lock->l_len,lock->l_whence,lock->l_type);
	lock->l_type = F_UNLCK;
	fuse_reply_lock(req,lock);
}

void mfs_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sl) {
	if (debug_mode) {
		fprintf(stderr,"setlk (inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u sleep:%u)\n",(unsigned long int)ino,fi->lock_owner,lock->l_start,lock->l_len,lock->l_whence,lock->l_type,sl);
	}
	syslog(LOG_NOTICE,"set lock inode:%lu owner:%llu lstart:%llu llen:%llu lwhence:%u ltype:%u sleep:%u",(unsigned long int)ino,fi->lock_owner,lock->l_start,lock->l_len,lock->l_whence,lock->l_type,sl);
	fuse_reply_err(req,0);
}
*/
#endif

void mfs_init(int debug_mode_in,int keep_cache_in,double direntry_cache_timeout_in,double entry_cache_timeout_in,double attr_cache_timeout_in) {
	debug_mode = debug_mode_in;
	keep_cache = keep_cache_in;
	direntry_cache_timeout = direntry_cache_timeout_in;
	entry_cache_timeout = entry_cache_timeout_in;
	attr_cache_timeout = attr_cache_timeout_in;
	if (debug_mode) {
		fprintf(stderr,"cache parameters: file_keep_cache=%s direntry_cache_timeout=%.2lf entry_cache_timeout=%.2lf attr_cache_timeout=%.2lf\n",(keep_cache==1)?"always":(keep_cache==2)?"never":"auto",direntry_cache_timeout,entry_cache_timeout,attr_cache_timeout);
	}
	mfs_statsptr_init();
}
