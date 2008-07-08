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

#include "config.h"
#include "datapack.h"
#include "mastercomm.h"
#include "readdata.h"
#include "writedata.h"
#include "MFSCommunication.h"

#define LOCAL_MODE_INODE_TIMEOUT 60.0
#define LOCAL_MODE_ATTR_TIMEOUT 60.0

#define NAME_MAX 255
#define MAX_FILE_SIZE 0x20000000000LL

// version 1.3.2 (x.y.z -> x*1000000+y*1000+z)
//#define VERSMAJ 1
//#define VERSMID 3
//#define VERSMIN 2
#define VERSION ((VERSMAJ)*1000000+(VERSMID)*1000+(VERSMIN))

#define MASTER_NAME ".master"
#define MASTER_INODE 0x7FFFFFFF
static uint8_t masterattr[35]={'f', 0x01,0xB6, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

typedef struct _minfo {
	int sd;
	int sent;
} minfo;

typedef struct _dirbuf {
	int wasread;
	char *p;
	size_t size;
	pthread_mutex_t lock;
} dirbuf;

enum {IO_NONE,IO_READ,IO_WRITE,IO_READONLY,IO_WRITEONLY};

typedef struct _finfo {
	uint8_t mode;
	void *data;
	pthread_mutex_t lock;
} finfo;

static int debug_mode = 0;
static int local_mode = 0;

static uint32_t root_inode;

static void mfs_rootinode_lookup(const char *name) {
	uint32_t inode;
	uint32_t nleng;
	uint8_t attr[35];
	int status;
	int cnt;

	if (root_inode==0) {
		return;
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
		root_inode=0;
		return;
	}
	// this trick with cnt should be unnecessary after rebuild masterconn.h
	for (cnt=0 ; cnt<40 ; cnt++) {
		status = fs_lookup(root_inode,nleng,(const uint8_t *)name,0,0,&inode,attr);
		if (status==0) {
			if (attr[0]==TYPE_DIRECTORY) {
				root_inode=inode;
			} else {
				root_inode=0;
			}
			return;
		} else if (status==ERROR_IO) {
			sleep(1);
		} else {
			root_inode=0;
			return;
		}
	}
	root_inode=0;
}

int mfs_rootnode_setup(char *path) {
	root_inode = 1;
	while (path) {
		char *n;
		while (*path=='/') {
			path++;
		}
		n = path;
		path = strchr(n,'/');
		if (path!=NULL) {
			*path++=0;
		}
		if (*n) {
			mfs_rootinode_lookup(n);
		}
	}
	if (root_inode==0) {
		return -1;
	}
	return 0;
}

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
	default:
		ret=EINVAL;
		break;
	}
	if (debug_mode) {
		fprintf(stderr,"status conv: %u->%d\n",status,ret);
	}
	return ret;
}

static void mfs_type_to_stat(uint32_t inode,uint8_t type, struct stat *stbuf) {
	if (inode==root_inode) {
		inode=FUSE_ROOT_ID;
	}
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

static void mfs_attr_to_stat(uint32_t inode,uint8_t attr[35], struct stat *stbuf) {
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime,attrnlink,attrrdev;
	uint64_t attrlength;
	uint8_t *ptr;
	ptr = attr;
	GET8BIT(attrtype,ptr);
	GET16BIT(attrmode,ptr);
	GET32BIT(attruid,ptr);
	GET32BIT(attrgid,ptr);
	GET32BIT(attratime,ptr);
	GET32BIT(attrmtime,ptr);
	GET32BIT(attrctime,ptr);
	GET32BIT(attrnlink,ptr);
	if (inode==root_inode) {
		inode=FUSE_ROOT_ID;
	}
	stbuf->st_ino = inode;
	switch (attrtype) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR | ( attrmode & 07777);
		GET64BIT(attrlength,ptr);
		stbuf->st_size = attrlength;
		stbuf->st_blocks = (attrlength+511)/512;
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK | ( attrmode & 07777);
		GET64BIT(attrlength,ptr);
		stbuf->st_size = attrlength;
		stbuf->st_blocks = (attrlength+511)/512;
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG | ( attrmode & 07777);
		GET64BIT(attrlength,ptr);
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
		GET32BIT(attrrdev,ptr);
		stbuf->st_rdev = attrrdev;
		stbuf->st_size = 0;
		stbuf->st_blocks = 0;
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR | ( attrmode & 07777);
		GET32BIT(attrrdev,ptr);
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

void mfs_statfs(fuse_req_t req) {
	uint64_t totalspace,availspace,trashspace,reservedspace;
	uint32_t inodes;
	uint32_t bsize;
	struct statvfs stfsbuf;
	memset(&stfsbuf,0,sizeof(stfsbuf));

	fs_statfs(&totalspace,&availspace,&trashspace,&reservedspace,&inodes);

	if (totalspace<0x0000800000000000ULL) {
		bsize = 0x10000;
	} else {
		bsize = 0x4000000;
	}
	stfsbuf.f_namemax = NAME_MAX;
	stfsbuf.f_frsize = bsize;
	stfsbuf.f_bsize = bsize;
	stfsbuf.f_blocks = totalspace/bsize;
	stfsbuf.f_bfree = availspace/bsize;
	stfsbuf.f_bavail = availspace/bsize;
	stfsbuf.f_files = 1000000000+VERSION+inodes;
	stfsbuf.f_ffree = 1000000000+VERSION;
	stfsbuf.f_favail = 1000000000+VERSION;
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
	GET16BIT(attrmode,ptr);
	GET32BIT(attruid,ptr);
	GET32BIT(attrgid,ptr);
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
	if (ino==FUSE_ROOT_ID) {
		ino=root_inode;
	}
	if (ino==MASTER_INODE) {
		fuse_reply_err(req,0);
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
	uint32_t inode;
	uint32_t nleng;
	uint8_t attr[35];
	int status;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"lookup (%lu,%s)\n",(unsigned long int)parent,name);
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}
	if (parent==FUSE_ROOT_ID) {
		parent = root_inode;
		if (nleng==2 && name[0]=='.' && name[1]=='.') {
			nleng=1;
		}
		if (strcmp(name,MASTER_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = MASTER_INODE;
			if (local_mode) {
				e.attr_timeout = LOCAL_MODE_ATTR_TIMEOUT;
				e.entry_timeout = LOCAL_MODE_INODE_TIMEOUT;
			} else {
				e.attr_timeout = 0.0;
				e.entry_timeout = 0.0;
			}
			mfs_attr_to_stat(MASTER_INODE,masterattr,&e.attr);
			fuse_reply_entry(req, &e);
			return ;
		}
	}
	ctx = fuse_req_ctx(req);
	status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		return;
	}
	if (inode==root_inode) {
		inode=FUSE_ROOT_ID;
		status = fs_getattr(inode,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			return;
		}
	}
	if (attr[0]==TYPE_FILE) {
		write_data_flush_inode(inode);
		status = fs_getattr(inode,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			return;
		}
	}
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	if (local_mode) {
		e.attr_timeout = LOCAL_MODE_ATTR_TIMEOUT;
		e.entry_timeout = LOCAL_MODE_INODE_TIMEOUT;
	} else {
		e.attr_timeout = 0.0;
		e.entry_timeout = 0.0;
	}
	mfs_attr_to_stat(inode,attr,&e.attr);
//	if (attr[0]==TYPE_FILE && debug_mode) {
//		fprintf(stderr,"lookup inode %lu - file size: %llu\n",(unsigned long int)inode,(unsigned long long int)e.attr.st_size);
//	}
	fuse_reply_entry(req, &e);
}

void mfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	uint8_t attr[35];
	int status;
	(void)fi;

	if (debug_mode) {
		fprintf(stderr,"getattr (%lu)\n",(unsigned long int)ino);
	}
	if (ino==FUSE_ROOT_ID) {
		ino = root_inode;
	}
	if (ino==MASTER_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,masterattr,&o_stbuf);
		if (local_mode) {
			fuse_reply_attr(req, &o_stbuf, LOCAL_MODE_ATTR_TIMEOUT);
		} else {
			fuse_reply_attr(req, &o_stbuf, 0.0);
		}
		return;
	}
	write_data_flush_inode(ino);
	status = fs_getattr(ino,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		if (ino==root_inode) {
			ino=FUSE_ROOT_ID;
		}
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,attr,&o_stbuf);
//		if (attr[0]==TYPE_FILE && debug_mode) {
//			fprintf(stderr,"getattr inode %lu - file size: %llu\n",(unsigned long int)ino,(unsigned long long int)o_stbuf.st_size);
//		}
		if (local_mode) {
			fuse_reply_attr(req, &o_stbuf, LOCAL_MODE_ATTR_TIMEOUT);
		} else {
			fuse_reply_attr(req, &o_stbuf, 0.0);
		}
	}
}

void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	uint8_t attr[35];
	int status;
	const struct fuse_ctx *ctx;
	uint8_t setmask = 0;

	if (debug_mode) {
		fprintf(stderr,"setattr (%lu,%u,(%04o,%u,%u,%lu,%lu,%llu))\n",(unsigned long int)ino,to_set,stbuf->st_mode & 07777,stbuf->st_uid,stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),stbuf->st_size);
	}
	if (ino==FUSE_ROOT_ID) {
		ino = root_inode;
	}
	if (ino==MASTER_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,masterattr,&o_stbuf);
		if (local_mode) {
			fuse_reply_attr(req, &o_stbuf, LOCAL_MODE_ATTR_TIMEOUT);
		} else {
			fuse_reply_attr(req, &o_stbuf, 0.0);
		}
		return;
	}
	status = EINVAL;
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
		}
		ctx = fuse_req_ctx(req);
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
		ctx = fuse_req_ctx(req);
		status = fs_truncate(ino,(fi!=NULL)?0:ctx->uid,ctx->gid,stbuf->st_size,attr);
		while (status==ERROR_LOCKED) {
			sleep(1);
			status = fs_truncate(ino,(fi!=NULL)?0:ctx->uid,ctx->gid,stbuf->st_size,attr);
		}
		status = mfs_errorconv(status);
		read_inode_ops(ino);
		if (status!=0) {
			fuse_reply_err(req, status);
			return;
		}
	}
	if (status!=0) {	// other flags or no flags - return EINVAL
		fuse_reply_err(req, status);
	} else {
		if (ino==root_inode) {
			ino = FUSE_ROOT_ID;
		}
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,attr,&o_stbuf);
		if (local_mode) {
			fuse_reply_attr(req, &o_stbuf, LOCAL_MODE_ATTR_TIMEOUT);
		} else {
			fuse_reply_attr(req, &o_stbuf, 0.0);
		}
	}
}

void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	uint32_t nleng;
	int status;
	uint8_t type;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"mknod (%lu,%s,%04o,%08lX)\n",(unsigned long int)parent,name,mode,(unsigned long int)rdev);
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
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
		parent = root_inode;
		if (strcmp(name,MASTER_NAME)==0) {
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
		if (local_mode) {
			e.attr_timeout = LOCAL_MODE_ATTR_TIMEOUT;
			e.entry_timeout = LOCAL_MODE_INODE_TIMEOUT;
		} else {
			e.attr_timeout = 0.0;
			e.entry_timeout = 0.0;
		}
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"unlink (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		parent = root_inode;
		if (strcmp(name,MASTER_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}

	nleng = strlen(name);
	if (nleng>NAME_MAX) {
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
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"mkdir (%lu,%s,%04o)\n",(unsigned long int)parent,name,mode);
	}
	if (parent==FUSE_ROOT_ID) {
		parent = root_inode;
		if (strcmp(name,MASTER_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
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
		if (local_mode) {
			e.attr_timeout = LOCAL_MODE_ATTR_TIMEOUT;
			e.entry_timeout = LOCAL_MODE_INODE_TIMEOUT;
		} else {
			e.attr_timeout = 0.0;
			e.entry_timeout = 0.0;
		}
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"rmdir (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		parent = root_inode;
		if (strcmp(name,MASTER_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
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
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"symlink (%s,%lu,%s)\n",path,(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		parent = root_inode;
		if (strcmp(name,MASTER_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
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
		if (local_mode) {
			e.attr_timeout = LOCAL_MODE_ATTR_TIMEOUT;
			e.entry_timeout = LOCAL_MODE_INODE_TIMEOUT;
		} else {
			e.attr_timeout = 0.0;
			e.entry_timeout = 0.0;
		}
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_readlink(fuse_req_t req, fuse_ino_t ino) {
	int status;
	uint8_t *path;

	if (debug_mode) {
		fprintf(stderr,"readlink (%lu)\n",(unsigned long int)ino);
	}
	if (ino==FUSE_ROOT_ID) {
		ino = root_inode;
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

	if (debug_mode) {
		fprintf(stderr,"rename (%lu,%s,%lu,%s)\n",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
	}
	if (parent==FUSE_ROOT_ID) {
		parent = root_inode;
		if (strcmp(name,MASTER_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	if (newparent==FUSE_ROOT_ID) {
		newparent = root_inode;
		if (strcmp(newname,MASTER_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}
	newnleng = strlen(newname);
	if (newnleng>NAME_MAX) {
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
	const struct fuse_ctx *ctx;


	if (debug_mode) {
		fprintf(stderr,"link (%lu,%lu,%s)\n",(unsigned long int)ino,(unsigned long int)newparent,newname);
	}
	if (ino==FUSE_ROOT_ID) {
		ino = root_inode;
	}
	if (ino==MASTER_INODE) {
		fuse_reply_err(req, EACCES);
		return;
	}
	if (newparent==FUSE_ROOT_ID) {
		newparent = root_inode;
		if (strcmp(newname,MASTER_NAME)==0) {
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	newnleng = strlen(newname);
	if (newnleng>NAME_MAX) {
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	write_data_flush_inode(ino);
	ctx = fuse_req_ctx(req);
	status = fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx->uid,ctx->gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		if (local_mode) {
			e.attr_timeout = LOCAL_MODE_ATTR_TIMEOUT;
			e.entry_timeout = LOCAL_MODE_INODE_TIMEOUT;
		} else {
			e.attr_timeout = 0.0;
			e.entry_timeout = 0.0;
		}
		mfs_attr_to_stat(inode,attr,&e.attr);
		fuse_reply_entry(req, &e);
	}
}

static int dirbuf_fill(dirbuf *b, uint32_t ino,uint8_t *buffer, uint32_t size) {
	uint8_t *ptr,*eptr;
	uint8_t nleng;
	char *name;
	uint32_t inode;
	uint8_t type;
	uint32_t off,next;
	struct stat stbuf;

	b->p = NULL;
	b->size = 0;
	ptr = buffer;
	eptr = buffer+size;
	while (ptr<eptr) {
		nleng = ptr[0];
		b->size += fuse_dirent_size(nleng);
		ptr += 6+nleng;
	}
	if (ptr!=eptr) {	// data misaligned
		return -1;
	}
//	if (ino==root_inode) {
//		b->size += fuse_dirent_size(strlen(MASTER_NAME));
//	}
	b->p = malloc(b->size);
	if (b->p==NULL) {
		return -1;	// out of memory
	}
	ptr = buffer;
	off = 0;
	while (ptr<eptr) {
		nleng = ptr[0];
		ptr++;
		name = (char*)ptr;
		ptr+=nleng;
		if (ptr+5<=eptr) {
			GET32BIT(inode,ptr);
			if (ino==root_inode && name[0]=='.' && name[1]=='.' && name[2]==0) {
				inode = FUSE_ROOT_ID;
			}
			if (inode==root_inode) {
				inode = FUSE_ROOT_ID;
			}
			GET8BIT(type,ptr);
			mfs_type_to_stat(inode,type,&stbuf);
			name[nleng]=0;	// ugly hack
			next = off+fuse_dirent_size(nleng);
			fuse_add_dirent(b->p + off, name, &stbuf, next);
			off = next;
		}
	}
//	if (ino==root_inode) {
//		mfs_attr_to_stat(MASTER_INODE,masterattr,&stbuf);
//		fuse_add_dirent(b->p + off,MASTER_NAME, &stbuf, off + fuse_dirent_size(strlen(MASTER_NAME)));
//	}
	return 0;
}

void mfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	dirbuf *dirinfo;
	int status;
	uint8_t *dbuff;
	uint32_t dsize;
	const struct fuse_ctx *ctx;

	if (debug_mode) {
		fprintf(stderr,"opendir (%lu)\n",(unsigned long int)ino);
	}
	if (ino==FUSE_ROOT_ID) {
		ino = root_inode;
	}
	if (ino==MASTER_INODE) {
		fuse_reply_err(req, ENOTDIR);
	}
	ctx = fuse_req_ctx(req);
	status = fs_getdir(ino,ctx->uid,ctx->gid,&dbuff,&dsize);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
	} else {
		dirinfo = malloc(sizeof(dirbuf));
		pthread_mutex_init(&(dirinfo->lock),NULL);
		dirbuf_fill(dirinfo,ino,dbuff,dsize);
		fi->fh = (unsigned long)dirinfo;
		if (fuse_reply_open(req,fi) == -ENOENT) {
			fi->fh = 0;
			pthread_mutex_destroy(&(dirinfo->lock));
			free(dirinfo->p);
			free(dirinfo);
		}
	}
}

void mfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	int status;
        dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	if (debug_mode) {
		fprintf(stderr,"readdir (%lu,%llu,%llu)\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (off<0) {
		fuse_reply_err(req,EINVAL);
		return;
	}
	pthread_mutex_lock(&(dirinfo->lock));
	if (dirinfo->wasread==1 && off==0) {
		uint8_t *dbuff;
		uint32_t dsize;
		if (ino==FUSE_ROOT_ID) {
			ino = root_inode;
		}
		status = fs_getdir(ino,0,0,&dbuff,&dsize);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
			return;
		}
		free(dirinfo->p);
		dirbuf_fill(dirinfo,ino,dbuff,dsize);
	}
	if ((size_t)off < dirinfo->size) {
		if (off + size > dirinfo->size) {
			fuse_reply_buf(req, dirinfo->p + off, dirinfo->size - off);
		} else {
			fuse_reply_buf(req, dirinfo->p + off, size);
		}
	} else {
		fuse_reply_buf(req, NULL, 0);
	}
	dirinfo->wasread=1;
	pthread_mutex_unlock(&(dirinfo->lock));
}

void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	(void)ino;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	if (debug_mode) {
		fprintf(stderr,"releasedir (%lu)\n",(unsigned long int)ino);
	}
	pthread_mutex_lock(&(dirinfo->lock));
	pthread_mutex_unlock(&(dirinfo->lock));
	pthread_mutex_destroy(&(dirinfo->lock));
	free(dirinfo->p);
	free(dirinfo);
	fi->fh = 0;
	fuse_reply_err(req,0);
}


static finfo* mfs_newfileinfo(uint8_t accmode,uint32_t inode) {
	finfo *fileinfo;
	fileinfo = malloc(sizeof(finfo));
#ifdef __FreeBSD__
	/* FreeBSD fuse used to read whole file when opening with O_WRONLY|O_APPEND,
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
		write_data_flush(fileinfo->data);
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
	uint32_t nleng;
	int status;
	const struct fuse_ctx *ctx;
	finfo *fileinfo;

	if (debug_mode) {
		fprintf(stderr,"create (%lu,%s,%04o)\n",(unsigned long int)parent,name,mode);
	}
	if (parent==FUSE_ROOT_ID) {
		parent = root_inode;
		if (strcmp(name,MASTER_NAME)==0) {
			fuse_reply_err(req,EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>NAME_MAX) {
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
	status = fs_opencheck(inode,ctx->uid,ctx->gid,oflags);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		return;
	}

	fileinfo = mfs_newfileinfo(fi->flags & O_ACCMODE,inode);
	fi->fh = (unsigned long)fileinfo;
	fi->keep_cache = local_mode;
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	if (local_mode) {
		e.attr_timeout = LOCAL_MODE_ATTR_TIMEOUT;
		e.entry_timeout = LOCAL_MODE_INODE_TIMEOUT;
	} else {
		e.attr_timeout = 0.0;
		e.entry_timeout = 0.0;
	}
	mfs_attr_to_stat(inode,attr,&e.attr);
	if (fuse_reply_create(req, &e, fi) == -ENOENT) {
		mfs_removefileinfo(fileinfo);
	}
}

void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint8_t oflags;
	int status;
	const struct fuse_ctx *ctx;
	finfo *fileinfo;

	if (debug_mode) {
		fprintf(stderr,"open (%lu)\n",(unsigned long int)ino);
	}
	if (ino==FUSE_ROOT_ID) {
		ino = root_inode;
	}

	if (ino==MASTER_INODE) {
		minfo *masterinfo;
		status = fs_direct_connect();
		if (status<0) {
			fuse_reply_err(req,EIO);
			return;
		}
		masterinfo = malloc(sizeof(minfo));
		if (masterinfo==NULL) {
			fuse_reply_err(req,ENOMEM);
			return;
		}
		masterinfo->sd = status;
		masterinfo->sent = 0;
		fi->direct_io = 1;
		fi->fh = (unsigned long)masterinfo;
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
	status = fs_opencheck(ino,ctx->uid,ctx->gid,oflags);
	status = mfs_errorconv(status);
	if (status!=0) {
		fuse_reply_err(req, status);
		return ;
	}

	fileinfo = mfs_newfileinfo(fi->flags & O_ACCMODE,ino);
	fi->fh = (unsigned long)fileinfo;
	fi->keep_cache = local_mode;
//	fi->direct_io = 1;
	if (fuse_reply_open(req, fi) == -ENOENT) {
		mfs_removefileinfo(fileinfo);
	}
}

void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);

	if (debug_mode) {
		fprintf(stderr,"release (%lu)\n",(unsigned long int)ino);
	}
	if (ino==MASTER_INODE) {
		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
		if (masterinfo!=NULL) {
			fs_direct_close(masterinfo->sd);
			free(masterinfo);
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

	if (debug_mode) {
		fprintf(stderr,"read from inode %lu up to %lu bytes from position %llu\n",ino,(unsigned long int)size,(unsigned long long int)off);
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
	if (ino==MASTER_INODE) {
		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
		if (masterinfo->sent) {
			int rsize;
			buff = malloc(size);
			rsize = fs_direct_read(masterinfo->sd,buff,size);
			fuse_reply_buf(req,(char*)buff,rsize);
			//syslog(LOG_WARNING,"master received: %d/%d",rsize,size);
			free(buff);
		} else {
			syslog(LOG_WARNING,"master: read before write");
			fuse_reply_buf(req,NULL,0);
		}
		return;
	}
	if (fileinfo->mode==IO_WRITEONLY) {
		fuse_reply_err(req,EACCES);
		return;
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		fuse_reply_err(req, EFBIG);
		return;
	}
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE) {
		err = write_data_flush(fileinfo->data);
		if (err<0) {
			pthread_mutex_unlock(&(fileinfo->lock));
			if (err==-2) {	// stale descriptor
				fuse_reply_err(req,EBADF);
			} else if (err==-3) {	// chunk missing
				fuse_reply_err(req,ENXIO);
			} else if (err==-4) {	// out of memory
				fuse_reply_err(req,ENOMEM);
			} else {
				fuse_reply_err(req,EIO);
			}
			if (debug_mode) {
				fprintf(stderr,"IO error occured while writting inode %lu\n",ino);
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
	err = read_data(fileinfo->data,off,&ssize,&buff);
	if (err<0) {
		if (err==-2) {	// stale descriptor
			fuse_reply_err(req,EBADF);
		} else if (err==-3) {	// chunk missing
			fuse_reply_err(req,ENXIO);
		} else if (err==-4) {	// out of memory
			fuse_reply_err(req,ENOMEM);
		} else {
			fuse_reply_err(req,EIO);
		}
		if (debug_mode) {
			fprintf(stderr,"IO error occured while reading inode %lu\n",ino);
		}
	} else {
		fuse_reply_buf(req,(char*)buff,ssize);
		if (debug_mode) {
			fprintf(stderr,"%lu bytes have been read from inode %lu\n",(unsigned long int)ssize,ino);
		}
	}
	read_data_freebuff(fileinfo->data);
	pthread_mutex_unlock(&(fileinfo->lock));
}

void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;

	if (debug_mode) {
		fprintf(stderr,"write to inode %lu %lu bytes at position %llu\n",ino,(unsigned long int)size,(unsigned long long int)off);
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
	if (ino==MASTER_INODE) {
		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
		int wsize;
		masterinfo->sent=1;
		wsize = fs_direct_write(masterinfo->sd,(const uint8_t*)buf,size);
		//syslog(LOG_WARNING,"master sent: %d/%d",wsize,size);
		fuse_reply_write(req,wsize);
		return;
	}
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
	if (err<0) {
		pthread_mutex_unlock(&(fileinfo->lock));
		if (err==-2) {	// stale descriptor
			fuse_reply_err(req,EBADF);
		} else if (err==-3) {	// chunk missing
			fuse_reply_err(req,ENXIO);
		} else if (err==-4) {	// out of memory
			fuse_reply_err(req,ENOMEM);
		} else {
			fuse_reply_err(req,EIO);
		}
		if (debug_mode) {
			fprintf(stderr,"IO error occured while writting inode %lu\n",ino);
		}
	} else {
		pthread_mutex_unlock(&(fileinfo->lock));
		fuse_reply_write(req,size);
		if (debug_mode) {
			fprintf(stderr,"%lu bytes have been written to inode %lu\n",(unsigned long int)size,ino);
		}
	}
}

void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;

	if (debug_mode) {
		fprintf(stderr,"flush (%lu)\n",(unsigned long int)ino);
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
	if (ino==MASTER_INODE) {
		fuse_reply_err(req,0);
		return;
	}
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
		if (err<0) {
			if (err==-2) {	// stale descriptor
				fuse_reply_err(req,EBADF);
			} else if (err==-3) {	// chunk missing
				fuse_reply_err(req,ENXIO);
			} else if (err==-4) {	// out of memory
				fuse_reply_err(req,ENOMEM);
			} else {
				fuse_reply_err(req,EIO);
			}
			return ;
		}
	}
	fuse_reply_err(req,0);
}

void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	(void)datasync;
	
	if (debug_mode) {
		fprintf(stderr,"fsync (%lu)\n",(unsigned long int)ino);
	}
	if (fileinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
	if (ino==MASTER_INODE) {
		fuse_reply_err(req,0);
		return;
	}
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
		if (err<0) {
			if (err==-2) {	// stale descriptor
				fuse_reply_err(req,EBADF);
			} else if (err==-3) {	// chunk missing
				fuse_reply_err(req,ENXIO);
			} else if (err==-4) {	// out of memory
				fuse_reply_err(req,ENOMEM);
			} else {
				fuse_reply_err(req,EIO);
			}
			return ;
		}
	}
	fuse_reply_err(req,0);
}

void mfs_init(int debug_mode_in,int local_mode_in) {
	debug_mode = debug_mode_in;
	local_mode = local_mode_in;
}
