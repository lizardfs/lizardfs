/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2018 Skytechnology sp. z o.o..

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

#include <fuse.h>
#include <fuse_lowlevel.h>

#include "protocol/MFSCommunication.h"

#if FUSE_USE_VERSION >= 26
void mfs_statfs(fuse_req_t req, fuse_ino_t ino);
#else
void mfs_statfs(fuse_req_t req);
#endif
void mfs_access(fuse_req_t req, fuse_ino_t ino, int mask);
void mfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
void mfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi);
void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
void mfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
void mfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
void mfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
void mfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name);
void mfs_readlink(fuse_req_t req, fuse_ino_t ino);
#if FUSE_VERSION >= 30
void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags);
#else
void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);
#endif
void mfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname);
void mfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi);
void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
#if defined(__APPLE__)
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position);
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position);
#else
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags);
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size);
#endif /* __APPLE__ */
void mfs_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size);
void mfs_removexattr (fuse_req_t req, fuse_ino_t ino, const char *name);
#if FUSE_VERSION >= 26
void lzfs_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock);
void lzfs_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sleep) ;
#endif
#if FUSE_VERSION >= 29
void lzfs_flock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, int op);
#endif
