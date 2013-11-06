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
# ifndef __DARWIN_64_BIT_INO_T
#  define __DARWIN_64_BIT_INO_T 0
# endif
#endif

#include "config.h"

#include <fuse/fuse_lowlevel.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <syslog.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>

#include "common/datapack.h"
#include "mastercomm.h"
#include "masterproxy.h"
#include "common/MFSCommunication.h"

#define READDIR_BUFFSIZE 50000

typedef struct _dirbuf {
	int wasread;
	uint8_t *p;
	size_t size;
	pthread_mutex_t lock;
} dirbuf;

typedef struct _pathbuf {
	int changed;
	char *p;
	size_t size;
	pthread_mutex_t lock;
} pathbuf;

#define NAME_MAX 255
#define PATH_SIZE_LIMIT 1024

#define META_ROOT_INODE FUSE_ROOT_ID
#define META_ROOT_MODE 0555

#define META_TRASH_INODE (FUSE_ROOT_ID+1)
#define META_TRASH_MODE 0700
#define META_TRASH_NAME "trash"
#define META_UNDEL_INODE (FUSE_ROOT_ID+2)
#define META_UNDEL_MODE 0200
#define META_UNDEL_NAME "undel"
#define META_RESERVED_INODE (FUSE_ROOT_ID+3)
#define META_RESERVED_MODE 0500
#define META_RESERVED_NAME "reserved"

#define META_INODE_MIN META_ROOT_INODE
#define META_INODE_MAX META_RESERVED_INODE

#define INODE_VALUE_MASK 0x1FFFFFFF
#define INODE_TYPE_MASK 0x60000000
#define INODE_TYPE_TRASH 0x20000000
#define INODE_TYPE_RESERVED 0x40000000
#define INODE_TYPE_SPECIAL 0x00000000

#define MASTERINFO_NAME ".masterinfo"
#define MASTERINFO_INODE 0x7FFFFFFE
// 0x0124 = 0444
#ifdef MASTERINFO_WITH_VERSION
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,14};
#else
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10};
#endif

#define PKGVERSION ((PACKAGE_VERSION_MAJOR)*1000000+(PACKAGE_VERSION_MINOR)*1000+(PACKAGE_VERSION_MICRO))

static int debug_mode = 0;
static double entry_cache_timeout = 0.0;
static double attr_cache_timeout = 1.0;

uint32_t mfs_meta_name_to_inode(const char *name) {
	uint32_t inode=0;
	char *end;
	inode = strtoul(name,&end,16);
	if (*end=='|' && end[1]!=0) {
		return inode;
	} else {
		return 0;
	}
}

static void mfs_meta_type_to_stat(uint32_t inode,uint8_t type, struct stat *stbuf) {
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

static int mfs_errorconv(int status) {
	switch (status) {
		case STATUS_OK:
			return 0;
		case ERROR_EPERM:
			return EPERM;
		case ERROR_ENOTDIR:
			return ENOTDIR;
		case ERROR_ENOENT:
			return ENOENT;
		case ERROR_EACCES:
			return EACCES;
		case ERROR_EEXIST:
			return EEXIST;
		case ERROR_EINVAL:
			return EINVAL;
		case ERROR_ENOTEMPTY:
			return ENOTEMPTY;
		case ERROR_IO:
			return EIO;
		case ERROR_EROFS:
			return EROFS;
		case ERROR_QUOTA:
#ifdef EDQUOT
			return EDQUOT;
#else
			return ENOSPC;
#endif
		default:
			return EINVAL;
	}
}


static void mfs_meta_stat(uint32_t inode, struct stat *stbuf) {
	int now;
	stbuf->st_ino = inode;
	stbuf->st_size = 0;
	stbuf->st_blocks = 0;
	switch (inode) {
	case META_ROOT_INODE:
		stbuf->st_nlink = 4;
		stbuf->st_mode = S_IFDIR | META_ROOT_MODE ;
		break;
	case META_TRASH_INODE:
		stbuf->st_nlink = 3;
		stbuf->st_mode = S_IFDIR | META_TRASH_MODE ;
		break;
	case META_UNDEL_INODE:
		stbuf->st_nlink = 2;
		stbuf->st_mode = S_IFDIR | META_UNDEL_MODE ;
		break;
	case META_RESERVED_INODE:
		stbuf->st_nlink = 2;
		stbuf->st_mode = S_IFDIR | META_RESERVED_MODE ;
		break;
	}
	stbuf->st_uid = 0;
	stbuf->st_gid = 0;
	now = time(NULL);
	stbuf->st_atime = now;
	stbuf->st_mtime = now;
	stbuf->st_ctime = now;
}

static void mfs_attr_to_stat(uint32_t inode,uint8_t attr[35], struct stat *stbuf) {
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime,attrnlink;
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
	attrlength = get64bit(&ptr);
	stbuf->st_ino = inode;
	if (attrtype==TYPE_FILE || attrtype==TYPE_TRASH || attrtype==TYPE_RESERVED) {
		stbuf->st_mode = S_IFREG | ( attrmode & 07777);
	} else {
		stbuf->st_mode = 0;
	}
	stbuf->st_size = attrlength;
	stbuf->st_blocks = (attrlength+511)/512;
	stbuf->st_uid = attruid;
	stbuf->st_gid = attrgid;
	stbuf->st_atime = attratime;
	stbuf->st_mtime = attrmtime;
	stbuf->st_ctime = attrctime;
	stbuf->st_nlink = attrnlink;
}

#if FUSE_USE_VERSION >= 26
void mfs_meta_statfs(fuse_req_t req, fuse_ino_t ino) {
#else
void mfs_meta_statfs(fuse_req_t req) {
#endif
	uint64_t totalspace,availspace,trashspace,reservedspace;
	uint32_t inodes;
	struct statvfs stfsbuf;
	memset(&stfsbuf,0,sizeof(stfsbuf));

#if FUSE_USE_VERSION >= 26
	(void)ino;
#endif
	fs_statfs(&totalspace,&availspace,&trashspace,&reservedspace,&inodes);

	stfsbuf.f_namemax = NAME_MAX;
	stfsbuf.f_frsize = 512;
	stfsbuf.f_bsize = 512;
	stfsbuf.f_blocks = trashspace/512+reservedspace/512;
	stfsbuf.f_bfree = reservedspace/512;
	stfsbuf.f_bavail = reservedspace/512;
	stfsbuf.f_files = 1000000000+PKGVERSION;
	stfsbuf.f_ffree = 1000000000+PKGVERSION;
	stfsbuf.f_favail = 1000000000+PKGVERSION;

	fuse_reply_statfs(req,&stfsbuf);
}

void mfs_meta_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint32_t inode;
	memset(&e, 0, sizeof(e));
	inode = 0;
	switch (parent) {
	case META_ROOT_INODE:
		if (strcmp(name,".")==0 || strcmp(name,"..")==0) {
			inode = META_ROOT_INODE;
		} else if (strcmp(name,META_TRASH_NAME)==0) {
			inode = META_TRASH_INODE;
		} else if (strcmp(name,META_RESERVED_NAME)==0) {
			inode = META_RESERVED_INODE;
		} else if (strcmp(name,MASTERINFO_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = MASTERINFO_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(MASTERINFO_INODE,masterinfoattr,&e.attr);
			fuse_reply_entry(req, &e);
			return ;
		}
		break;
	case META_TRASH_INODE:
		if (strcmp(name,".")==0) {
			inode = META_TRASH_INODE;
		} else if (strcmp(name,"..")==0) {
			inode = META_ROOT_INODE;
		} else if (strcmp(name,META_UNDEL_NAME)==0) {
			inode = META_UNDEL_INODE;
		} else {
			inode = mfs_meta_name_to_inode(name);
			if (inode>0) {
				int status;
				uint8_t attr[35];
				status = fs_getdetachedattr(inode,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					fuse_reply_err(req, status);
				} else {
					e.ino = inode | INODE_TYPE_TRASH;
					e.attr_timeout = attr_cache_timeout;
					e.entry_timeout = entry_cache_timeout;
					mfs_attr_to_stat(inode  | INODE_TYPE_TRASH,attr,&e.attr);
					fuse_reply_entry(req,&e);
				}
				return;
			}
		}
		break;
	case META_UNDEL_INODE:
		if (strcmp(name,".")==0) {
			inode = META_UNDEL_INODE;
		} else if (strcmp(name,"..")==0) {
			inode = META_TRASH_INODE;
		}
		break;
	case META_RESERVED_INODE:
		if (strcmp(name,".")==0) {
			inode = META_RESERVED_INODE;
		} else if (strcmp(name,"..")==0) {
			inode = META_ROOT_INODE;
		} else {
			inode = mfs_meta_name_to_inode(name);
			if (inode>0) {
				int status;
				uint8_t attr[35];
				status = fs_getdetachedattr(inode,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					fuse_reply_err(req, status);
				} else {
					e.ino = inode | INODE_TYPE_RESERVED;
					e.attr_timeout = attr_cache_timeout;
					e.entry_timeout = entry_cache_timeout;
					mfs_attr_to_stat(inode  | INODE_TYPE_RESERVED,attr,&e.attr);
					fuse_reply_entry(req,&e);
				}
				return;
			}
		}
		break;
	}
	if (inode==0) {
		fuse_reply_err(req,ENOENT);
	} else {
		e.ino = inode;
		e.attr_timeout = attr_cache_timeout;
		e.entry_timeout = entry_cache_timeout;
		mfs_meta_stat(inode,&e.attr);
		fuse_reply_entry(req,&e);
	}
}

void mfs_meta_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	(void)fi;
	if (ino==MASTERINFO_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,masterinfoattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
	} else if (ino>=META_INODE_MIN && ino<=META_INODE_MAX) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_meta_stat(ino,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, attr_cache_timeout);
	} else if ((ino & INODE_TYPE_MASK) == INODE_TYPE_TRASH) {
		int status;
		uint8_t attr[35];
		status = fs_getdetachedattr(ino & INODE_VALUE_MASK,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
		} else {
			memset(&o_stbuf, 0, sizeof(struct stat));
			mfs_attr_to_stat(ino,attr,&o_stbuf);
			fuse_reply_attr(req, &o_stbuf, attr_cache_timeout);
		}
	} else if ((ino & INODE_TYPE_MASK) == INODE_TYPE_RESERVED) {
		int status;
		uint8_t attr[35];
		status = fs_getdetachedattr(ino & INODE_VALUE_MASK,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
		} else {
			memset(&o_stbuf, 0, sizeof(struct stat));
			mfs_attr_to_stat(ino,attr,&o_stbuf);
			fuse_reply_attr(req, &o_stbuf, attr_cache_timeout);
		}
	} else {
		fuse_reply_err(req, ENOENT);
	}
}

void mfs_meta_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	(void)to_set;
	(void)stbuf;
	mfs_meta_getattr(req,ino,fi);
}

void mfs_meta_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	int status;
	uint32_t inode;
	if (parent!=META_TRASH_INODE) {
		fuse_reply_err(req,EACCES);
		return;
	}
	inode = mfs_meta_name_to_inode(name);
	if (inode==0) {
		fuse_reply_err(req,ENOENT);
		return;
	}
	status = fs_purge(inode);
	status = mfs_errorconv(status);
	fuse_reply_err(req, status);
}

void mfs_meta_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
	int status;
	uint32_t inode;
	(void)newname;
	if (parent!=META_TRASH_INODE && newparent!=META_UNDEL_INODE) {
		fuse_reply_err(req,EACCES);
		return;
	}
	inode = mfs_meta_name_to_inode(name);
	if (inode==0) {
		fuse_reply_err(req,ENOENT);
		return;
	}
	status = fs_undel(inode);
	status = mfs_errorconv(status);
	fuse_reply_err(req, status);
}

static uint32_t dir_metaentries_size(uint32_t ino) {
	switch (ino) {
	case META_ROOT_INODE:
		return 4*6+1+2+strlen(META_TRASH_NAME)+strlen(META_RESERVED_NAME);
	case META_TRASH_INODE:
		return 3*6+1+2+strlen(META_UNDEL_NAME);
	case META_UNDEL_INODE:
		return 2*6+1+2;
	case META_RESERVED_INODE:
		return 2*6+1+2;
	}
	return 0;
}

static void dir_metaentries_fill(uint8_t *buff,uint32_t ino) {
	uint8_t l;
	switch (ino) {
	case META_ROOT_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// trash
		l = strlen(META_TRASH_NAME);
		put8bit(&buff,l);
		memcpy(buff,META_TRASH_NAME,l);
		buff+=l;
		put32bit(&buff,META_TRASH_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// reserved
		l = strlen(META_RESERVED_NAME);
		put8bit(&buff,l);
		memcpy(buff,META_RESERVED_NAME,l);
		buff+=l;
		put32bit(&buff,META_RESERVED_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		return;
	case META_TRASH_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_TRASH_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// undel
		l = strlen(META_UNDEL_NAME);
		put8bit(&buff,l);
		memcpy(buff,META_UNDEL_NAME,l);
		buff+=l;
		put32bit(&buff,META_UNDEL_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		return;
	case META_UNDEL_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_UNDEL_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_TRASH_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		return;
	case META_RESERVED_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_RESERVED_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		return;
	}
}

static uint32_t dir_dataentries_size(const uint8_t *dbuff,uint32_t dsize) {
	uint8_t nleng;
	uint32_t eleng;
	const uint8_t *eptr;
	eleng=0;
	if (dbuff==NULL || dsize==0) {
		return 0;
	}
	eptr = dbuff+dsize;
	while (dbuff<eptr) {
		nleng = dbuff[0];
		dbuff+=5+nleng;
		if (nleng>255-9) {
			eleng+=6+255;
		} else {
			eleng+=6+nleng+9;
		}
	}
	return eleng;
}

static void dir_dataentries_convert(uint8_t *buff,const uint8_t *dbuff,uint32_t dsize,uint32_t inodemask) {
	const char *name;
	uint32_t inode;
	uint8_t nleng;
	uint8_t inoleng;
	const uint8_t *eptr;
	eptr = dbuff+dsize;
	while (dbuff<eptr) {
		nleng = dbuff[0];
		if (dbuff+nleng+5<=eptr) {
			dbuff++;
			if (nleng>255-9) {
				inoleng = 255;
			} else {
				inoleng = nleng+9;
			}
			put8bit(&buff,inoleng);
			name = (const char*)dbuff;
			dbuff+=nleng;
			inode = get32bit(&dbuff);
			sprintf((char*)buff,"%08" PRIX32 "|",inode);
			if (nleng>255-9) {
				memcpy(buff+9,name,255-9);
				buff+=255;
			} else {
				memcpy(buff+9,name,nleng);
				buff+=9+nleng;
			}
			inode|=inodemask;
			put32bit(&buff,inode);
			put8bit(&buff,TYPE_FILE);
		} else {
			syslog(LOG_WARNING,"dir data malformed (trash)");
			dbuff=eptr;
		}
	}
}


static void dirbuf_meta_fill(dirbuf *b, uint32_t ino) {
	int status;
	uint32_t msize,dcsize,imask;
	const uint8_t *dbuff;
	uint32_t dsize;

	b->p = NULL;
	b->size = 0;
	msize = dir_metaentries_size(ino);
	if (ino==META_TRASH_INODE) {
		status = fs_gettrash(&dbuff,&dsize);
		if (status!=STATUS_OK) {
			return;
		}
		dcsize = dir_dataentries_size(dbuff,dsize);
		imask = INODE_TYPE_TRASH;
	} else if (ino==META_RESERVED_INODE) {
		status = fs_getreserved(&dbuff,&dsize);
		if (status!=STATUS_OK) {
			return;
		}
		dcsize = dir_dataentries_size(dbuff,dsize);
		imask = INODE_TYPE_RESERVED;
	} else {
		dcsize = 0;
		imask = 0;
	}
	if (msize+dcsize==0) {
		return;
	}
	b->p = (uint8_t*) malloc(msize+dcsize);
	if (b->p==NULL) {
		syslog(LOG_WARNING,"out of memory");
		return;
	}
	if (msize>0) {
		dir_metaentries_fill(b->p,ino);
	}
	if (dcsize>0) {
		dir_dataentries_convert(b->p+msize,dbuff,dsize,imask);
	}
	b->size = msize+dcsize;
}

void mfs_meta_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	dirbuf *dirinfo;
	if (ino==META_ROOT_INODE || ino==META_TRASH_INODE || ino==META_UNDEL_INODE || ino==META_RESERVED_INODE) {
		dirinfo = (dirbuf*) malloc(sizeof(dirbuf));
		pthread_mutex_init(&(dirinfo->lock),NULL);
		dirinfo->p = NULL;
		dirinfo->size = 0;
		dirinfo->wasread = 0;
		fi->fh = (unsigned long)dirinfo;
		if (fuse_reply_open(req,fi) == -ENOENT) {
			fi->fh = 0;
			pthread_mutex_destroy(&(dirinfo->lock));
			free(dirinfo->p);
			free(dirinfo);
		}
	} else {
		fuse_reply_err(req, ENOTDIR);
	}
}

void mfs_meta_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
        dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	char buffer[READDIR_BUFFSIZE];
	char *name,c;
	const uint8_t *ptr,*eptr;
	uint8_t end;
	size_t opos,oleng;
	uint8_t nleng;
	uint32_t inode;
	uint8_t type;
	struct stat stbuf;

	if (off<0) {
		fuse_reply_err(req,EINVAL);
		return;
	}
	pthread_mutex_lock(&(dirinfo->lock));
	if (dirinfo->wasread==0 || (dirinfo->wasread==1 && off==0)) {
		if (dirinfo->p!=NULL) {
			free(dirinfo->p);
		}
		dirbuf_meta_fill(dirinfo,ino);
//		syslog(LOG_WARNING,"inode: %lu , dirinfo->p: %p , dirinfo->size: %lu",(unsigned long)ino,dirinfo->p,(unsigned long)dirinfo->size);
	}
	dirinfo->wasread=1;

	if (off>=(off_t)(dirinfo->size)) {
		fuse_reply_buf(req, NULL, 0);
	} else {
		if (size>READDIR_BUFFSIZE) {
			size=READDIR_BUFFSIZE;
		}
		ptr = (const uint8_t*)(dirinfo->p)+off;
		eptr = (const uint8_t*)(dirinfo->p)+dirinfo->size;
		opos = 0;
		end = 0;

		while (ptr<eptr && end==0) {
			nleng = ptr[0];
			ptr++;
			name = (char*)ptr;
			ptr+=nleng;
			off+=nleng+6;
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				type = get8bit(&ptr);
				mfs_meta_type_to_stat(inode,type,&stbuf);
				c = name[nleng];
				name[nleng]=0;
				oleng = fuse_add_direntry(req, buffer + opos, size - opos, name, &stbuf, off);
				name[nleng] = c;
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

void mfs_meta_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	(void)ino;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	pthread_mutex_lock(&(dirinfo->lock));
	pthread_mutex_unlock(&(dirinfo->lock));
	pthread_mutex_destroy(&(dirinfo->lock));
	free(dirinfo->p);
	free(dirinfo);
	fi->fh = 0;
	fuse_reply_err(req,0);
}

void mfs_meta_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	pathbuf *pathinfo;
	const uint8_t *path;
	int status;
	if (ino==MASTERINFO_INODE) {
		fi->fh = 0;
		fi->direct_io = 0;
		fi->keep_cache = 1;
		fuse_reply_open(req, fi);
		return;
	}
	if ((ino & INODE_TYPE_MASK) == INODE_TYPE_TRASH) {
		status = fs_gettrashpath((ino&INODE_VALUE_MASK),&path);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
		} else {
			pathinfo = (pathbuf*) malloc(sizeof(pathbuf));
			pthread_mutex_init(&(pathinfo->lock),NULL);
			pathinfo->changed = 0;
			pathinfo->size = strlen((char*)path)+1;
			pathinfo->p = (char*) malloc(pathinfo->size);
			memcpy(pathinfo->p,path,pathinfo->size-1);
			pathinfo->p[pathinfo->size-1]='\n';
			fi->direct_io = 1;
			fi->fh = (unsigned long)pathinfo;
			if (fuse_reply_open(req,fi) == -ENOENT) {
				fi->fh = 0;
				pthread_mutex_destroy(&(pathinfo->lock));
				free(pathinfo->p);
				free(pathinfo);
			}
		}
	} else {
		fuse_reply_err(req, EACCES);
	}
}

void mfs_meta_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	if (ino==MASTERINFO_INODE) {
		fuse_reply_err(req,0);
		return;
	}
	pathbuf *pathinfo = (pathbuf *)((unsigned long)(fi->fh));
	pthread_mutex_lock(&(pathinfo->lock));
	if (pathinfo->changed) {
		if (pathinfo->p[pathinfo->size-1]=='\n') {
			pathinfo->p[pathinfo->size-1]=0;
		} else {
			pathinfo->p = (char*) realloc(pathinfo->p,pathinfo->size+1);
			pathinfo->p[pathinfo->size]=0;
		}
		fs_settrashpath((ino&INODE_VALUE_MASK),(uint8_t*)pathinfo->p);
	}
	pthread_mutex_unlock(&(pathinfo->lock));
	pthread_mutex_destroy(&(pathinfo->lock));
	free(pathinfo->p);
	free(pathinfo);
	fi->fh = 0;
	fuse_reply_err(req,0);
}

void mfs_meta_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	pathbuf *pathinfo = (pathbuf *)((unsigned long)(fi->fh));
	if (ino==MASTERINFO_INODE) {
		uint8_t masterinfo[14];
		fs_getmasterlocation(masterinfo);
		masterproxy_getlocation(masterinfo);
#ifdef MASTERINFO_WITH_VERSION
		if (off>=14) {
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>14) {
			fuse_reply_buf(req,(char*)(masterinfo+off),14-off);
#else
		if (off>=10) {
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>10) {
			fuse_reply_buf(req,(char*)(masterinfo+off),10-off);
#endif
		} else {
			fuse_reply_buf(req,(char*)(masterinfo+off),size);
		}
		return;
	}
	if (pathinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
	pthread_mutex_lock(&(pathinfo->lock));
	if (off<0) {
		pthread_mutex_unlock(&(pathinfo->lock));
		fuse_reply_err(req,EINVAL);
		return;
	}
	if ((size_t)off>pathinfo->size) {
		fuse_reply_buf(req, NULL, 0);
	} else if (off + size > pathinfo->size) {
		fuse_reply_buf(req, (pathinfo->p)+off,(pathinfo->size)-off);
	} else {
		fuse_reply_buf(req, (pathinfo->p)+off,size);
	}
	pthread_mutex_unlock(&(pathinfo->lock));
}

void mfs_meta_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	pathbuf *pathinfo = (pathbuf *)((unsigned long)(fi->fh));
	if (ino==MASTERINFO_INODE) {
		fuse_reply_err(req,EACCES);
		return;
	}
	if (pathinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
	if (off + size > PATH_SIZE_LIMIT) {
		fuse_reply_err(req,EINVAL);
		return;
	}
	pthread_mutex_lock(&(pathinfo->lock));
	if (pathinfo->changed==0) {
		pathinfo->size = 0;
	}
	if (off+size > pathinfo->size) {
		size_t s = pathinfo->size;
		pathinfo->p = (char*) realloc(pathinfo->p,off+size);
		pathinfo->size = off+size;
		memset(pathinfo->p+s,0,off+size-s);
	}
	memcpy((pathinfo->p)+off,buf,size);
	pathinfo->changed = 1;
	pthread_mutex_unlock(&(pathinfo->lock));
	fuse_reply_write(req,size);
}

void mfs_meta_init(int debug_mode_in,double entry_cache_timeout_in,double attr_cache_timeout_in) {
	debug_mode = debug_mode_in;
	entry_cache_timeout = entry_cache_timeout_in;
	attr_cache_timeout = attr_cache_timeout_in;
	if (debug_mode) {
		fprintf(stderr,"cache parameters: entry_cache_timeout=%.2f attr_cache_timeout=%.2f\n",entry_cache_timeout,attr_cache_timeout);
	}
}
