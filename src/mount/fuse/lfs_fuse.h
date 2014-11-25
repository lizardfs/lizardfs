/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of LizardFS and is part of LizardFS.

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

#include <fuse/fuse_lowlevel.h>

#include "common/LFSCommunication.h"

#if FUSE_USE_VERSION >= 26
void lfs_statfs(fuse_req_t req, fuse_ino_t ino);
#else
void lfs_statfs(fuse_req_t req);
#endif
void lfs_access(fuse_req_t req, fuse_ino_t ino, int mask);
void lfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
void lfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void lfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi);
void lfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
void lfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
void lfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
void lfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
void lfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name);
void lfs_readlink(fuse_req_t req, fuse_ino_t ino);
void lfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);
void lfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname);
void lfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void lfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void lfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void lfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi);
void lfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void lfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void lfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void lfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
void lfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void lfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
#if defined(__APPLE__)
void lfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position);
void lfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position);
#else
void lfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags);
void lfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size);
#endif /* __APPLE__ */
void lfs_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size);
void lfs_removexattr (fuse_req_t req, fuse_ino_t ino, const char *name);
void lfs_init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
		double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
		SugidClearMode sugid_clear_mode_, bool acl_enabled_, double acl_cache_timeout_,
		unsigned acl_cache_size_);
