/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

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

#include <fuse/fuse_lowlevel.h>

#if FUSE_USE_VERSION >= 26
void mfs_meta_statfs(fuse_req_t req, fuse_ino_t ino);
#else
void mfs_meta_statfs(fuse_req_t req);
#endif
void mfs_meta_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
void mfs_meta_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi);
void mfs_meta_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
void mfs_meta_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);
void mfs_meta_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_meta_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_meta_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_meta_init(int debug_mode_in,double entry_cache_timeout_in,double attr_cache_timeout_in);
