/*
   Copyright 2017 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <utility>
#include "mount/lizard_client.h"
#include "protocol/lock_info.h"

/*
 * This file exists in order to provide unmangled names that can be easily
 * linked with dlsym() call.
 *
 * Implementations of all functions below should not throw anything,
 * as throwing exceptions in dynamically linked code is a dangerous idea.
 */

extern "C" {

int lizardfs_fs_init(LizardClient::FsInitParams &params);
void lizardfs_fs_term();
int lizardfs_lookup(LizardClient::Context &ctx, LizardClient::Inode parent,
	                                 const char *name, LizardClient::EntryParam &param);
int lizardfs_mknod(LizardClient::Context &ctx, LizardClient::Inode parent, const char *name,
	           mode_t mode, dev_t rdev, LizardClient::EntryParam &param);
int lizardfs_link(LizardClient::Context ctx, LizardClient::Inode inode, LizardClient::Inode parent,
	             const char *name, LizardClient::EntryParam &param);
int lizardfs_symlink(LizardClient::Context ctx, const char *link, LizardClient::Inode parent,
	             const char *name, LizardClient::EntryParam &param);
int lizardfs_mkdir(LizardClient::Context &ctx, LizardClient::Inode parent,
	                                 const char *name, mode_t mode, LizardClient::EntryParam &entry_param);
int lizardfs_rmdir(LizardClient::Context &ctx, LizardClient::Inode parent, const char *name);
int lizardfs_unlink(LizardClient::Context &ctx, LizardClient::Inode parent, const char *name);
int lizardfs_undel(LizardClient::Context &ctx, LizardClient::Inode ino);
int lizardfs_open(LizardClient::Context &ctx, LizardClient::Inode ino, LizardClient::FileInfo* fi);
int lizardfs_opendir(LizardClient::Context &ctx, LizardClient::Inode ino);
int lizardfs_release(LizardClient::Inode ino, LizardClient::FileInfo* fi);
int lizardfs_getattr(LizardClient::Context &ctx, LizardClient::Inode ino, LizardClient::AttrReply &reply);
int lizardfs_releasedir(LizardClient::Inode ino, uint64_t opendirSessionID);
int lizardfs_setattr(LizardClient::Context &ctx, LizardClient::Inode ino,
	             struct stat *stbuf, int to_set, LizardClient::AttrReply &attr_reply);

std::pair<int, ReadCache::Result> lizardfs_read(LizardClient::Context &ctx, LizardClient::Inode ino,
	                                         size_t size, off_t off, LizardClient::FileInfo* fi);

std::pair<int, std::vector<uint8_t>> lizardfs_read_special_inode(LizardClient::Context &ctx,
	                    LizardClient::Inode ino, size_t size, off_t off, LizardClient::FileInfo* fi);

std::pair<int, std::vector<LizardClient::DirEntry>> lizardfs_readdir(LizardClient::Context &ctx,
	                    uint64_t opendirSessionID, LizardClient::Inode ino, off_t off, size_t max_entries);

int lizardfs_readlink(LizardClient::Context &ctx, LizardClient::Inode ino, std::string &link);

std::pair<int, std::vector<NamedInodeEntry>> lizardfs_readreserved(LizardClient::Context &ctx,
	                    LizardClient::NamedInodeOffset off, LizardClient::NamedInodeOffset max_entries);

std::pair<int, std::vector<NamedInodeEntry>> lizardfs_readtrash(LizardClient::Context &ctx,
	                    LizardClient::NamedInodeOffset off, LizardClient::NamedInodeOffset max_entries);

std::pair<int, ssize_t> lizardfs_write(LizardClient::Context &ctx, LizardClient::Inode ino,
	                               const char *buf, size_t size, off_t off, LizardClient::FileInfo* fi);
int lizardfs_flush(LizardClient::Context &ctx, LizardClient::Inode ino, LizardClient::FileInfo* fi);
int lizardfs_fsync(LizardClient::Context &ctx, LizardClient::Inode ino, int datasync, LizardClient::FileInfo* fi);
bool lizardfs_isSpecialInode(LizardClient::Inode ino);
int lizardfs_update_groups(LizardClient::Context &ctx);
std::pair<int, LizardClient::JobId> lizardfs_makesnapshot(LizardClient::Context &ctx, LizardClient::Inode ino,
	                                                  LizardClient::Inode dst_parent,
	                                                  const std::string &dst_name,
	                                                  bool can_overwrite);
int lizardfs_getgoal(LizardClient::Context &ctx, LizardClient::Inode ino, std::string &goal);
int lizardfs_setgoal(LizardClient::Context &ctx, LizardClient::Inode ino,
	             const std::string &goal_name, uint8_t smode);
int lizardfs_rename(LizardClient::Context &ctx, LizardClient::Inode parent, const char *name,
	            LizardClient::Inode newparent, const char *newname);
int lizardfs_statfs(uint64_t *totalspace, uint64_t *availspace, uint64_t *trashspace,
	             uint64_t *reservedspace, uint32_t *inodes);
int lizardfs_setxattr(LizardClient::Context ctx, LizardClient::Inode ino, const char *name,
	              const char *value, size_t size, int flags);
int lizardfs_getxattr(LizardClient::Context ctx, LizardClient::Inode ino, const char *name,
	              size_t size, LizardClient::XattrReply &xattr_reply);
int lizardfs_listxattr(LizardClient::Context ctx, LizardClient::Inode ino, size_t size,
	               LizardClient::XattrReply &xattr_reply);
int lizardfs_removexattr(LizardClient::Context ctx, LizardClient::Inode ino, const char *name);
std::pair<int,std::vector<ChunkWithAddressAndLabel>> lizardfs_getchunksinfo(LizardClient::Context &ctx,
	             LizardClient::Inode ino, uint32_t chunk_index, uint32_t chunk_count);
std::pair<int,std::vector<ChunkserverListEntry>> lizardfs_getchunkservers();

int lizardfs_getlk(LizardClient::Context &ctx, LizardClient::Inode ino, LizardClient::FileInfo *fi,
	  lzfs_locks::FlockWrapper &lock);
std::pair<int, uint32_t> lizardfs_setlk_send(LizardClient::Context &ctx, LizardClient::Inode ino,
	                            LizardClient::FileInfo *fi, lzfs_locks::FlockWrapper &lock);
int lizardfs_setlk_recv();
int lizardfs_setlk_interrupt(const lzfs_locks::InterruptData &data);

} // extern "C"
