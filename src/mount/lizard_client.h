/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "mount/group_cache.h"
#include "mount/lizard_client_context.h"
#include "mount/readdata_cache.h"
#include "mount/stat_defs.h"
#include "protocol/lock_info.h"

namespace LizardClient {

typedef unsigned long int Inode;

/**
 * A class that is used for passing information between subsequent calls to the filesystem.
 * It is created when a file is opened, updated with every use of the file descriptor and
 * removed when a file is closed.
 */
struct FileInfo {
	FileInfo(int flags, unsigned int direct_io, unsigned int keep_cache, uint64_t fh,
		uint64_t lock_owner)
			: flags(flags),
			direct_io(direct_io),
			keep_cache(keep_cache),
			fh(fh),
			lock_owner(lock_owner) {
	}

	int flags;
	unsigned int direct_io : 1;
	unsigned int keep_cache : 1;
	uint64_t fh;
	uint64_t lock_owner;
};

/**
 * Directory entry parameters, a result of some filesystem operations (lookup, mkdir,
 * link etc.).
 */
struct EntryParam {
	EntryParam() : ino(0), generation(0), attr_timeout(0), entry_timeout(0) {
		memset(&attr, 0, sizeof(struct stat));
	}

	Inode ino;
	unsigned long generation;
	struct stat attr;
	double attr_timeout;
	double entry_timeout;
};

/**
 * A result of setattr and getattr operations
 */
struct AttrReply {
	struct stat attr;
	double attrTimeout;
};

/**
 * A result of readdir operation
 */
struct DirEntry {
	std::string name;
	struct stat attr;
	off_t nextEntryOffset;

	DirEntry(const std::string n, const struct stat &s, off_t o) : name(n), attr(s), nextEntryOffset(o) {}
};

/**
 * A result of getxattr, setxattr and listattr operations
 */
struct XattrReply {
	uint32_t valueLength;
	std::vector<uint8_t> valueBuffer;
};

/**
 * An exception that is thrown when a request can't be executed successfully
 */
struct RequestException : public std::exception {
	RequestException(int errNo);

	int errNo;
};

int updateGroups(const GroupCache::Groups &groups);

// TODO what about this one? Will decide when writing non-fuse client
// void fsinit(void *userdata, struct fuse_conn_info *conn);
bool isSpecialInode(LizardClient::Inode ino);

void update_credentials(int index, const GroupCache::Groups &groups);

EntryParam lookup(Context ctx, Inode parent, const char *name, bool whole_path_lookup = false);

AttrReply getattr(Context ctx, Inode ino, FileInfo* fi);

#define LIZARDFS_SET_ATTR_MODE      (1 << 0)
#define LIZARDFS_SET_ATTR_UID       (1 << 1)
#define LIZARDFS_SET_ATTR_GID       (1 << 2)
#define LIZARDFS_SET_ATTR_SIZE      (1 << 3)
#define LIZARDFS_SET_ATTR_ATIME     (1 << 4)
#define LIZARDFS_SET_ATTR_MTIME     (1 << 5)
#define LIZARDFS_SET_ATTR_ATIME_NOW (1 << 7)
#define LIZARDFS_SET_ATTR_MTIME_NOW (1 << 8)
AttrReply setattr(Context ctx, Inode ino, struct stat *stbuf, int to_set, FileInfo* fi);

std::string readlink(Context ctx, Inode ino);

EntryParam mknod(Context ctx, Inode parent, const char *name, mode_t mode, dev_t rdev);

EntryParam mkdir(Context ctx, Inode parent, const char *name, mode_t mode);

void unlink(Context ctx, Inode parent, const char *name);

void rmdir(Context ctx, Inode parent, const char *name);

EntryParam symlink(Context ctx, const char *link, Inode parent, const char *name);

void rename(Context ctx, Inode parent, const char *name, Inode newparent, const char *newname);

EntryParam link(Context ctx, Inode ino, Inode newparent, const char *newname);

void open(Context ctx, Inode ino, FileInfo* fi);

std::vector<uint8_t> read_special_inode(Context ctx, Inode ino, size_t size, off_t off,
				        FileInfo* fi);

ReadCache::Result read(Context ctx, Inode ino, size_t size, off_t off, FileInfo* fi);

typedef size_t BytesWritten;
BytesWritten write(Context ctx, Inode ino, const char *buf, size_t size, off_t off,
		FileInfo* fi);

void flush(Context ctx, Inode ino, FileInfo* fi);

void release(Context ctx, Inode ino, FileInfo* fi);

void fsync(Context ctx, Inode ino, int datasync, FileInfo* fi);

void opendir(Context ctx, Inode ino, FileInfo* fi);

std::vector<DirEntry> readdir(Context ctx, Inode ino, off_t off, size_t maxEntries, FileInfo* fi);

void releasedir(Context ctx, Inode ino, FileInfo* fi);

struct statvfs statfs(Context ctx, Inode ino);

void setxattr(Context ctx, Inode ino, const char *name, const char *value,
		size_t size, int flags, uint32_t position);

XattrReply getxattr(Context ctx, Inode ino, const char *name, size_t size, uint32_t position);

XattrReply listxattr(Context ctx, Inode ino, size_t size);

void removexattr(Context ctx, Inode ino, const char *name);

void access(Context ctx, Inode ino, int mask);

EntryParam create(Context ctx, Inode parent, const char *name,
		mode_t mode, FileInfo* fi);

void getlk(Context ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock);
void setlk(Context ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock, int sleep);
void flock_interrupt(uint32_t reqid);
void setlk_interrupt(uint32_t reqid);
void getlk(Context ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock);
uint32_t setlk_send(Context ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock);
void setlk_recv();
uint32_t flock_send(Context ctx, Inode ino, FileInfo* fi, int op);
void flock_recv();

void flock_interrupt(const lzfs_locks::InterruptData &data);
void setlk_interrupt(const lzfs_locks::InterruptData &data);

void init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
		unsigned direntry_cache_size_, double entry_cache_timeout_, double attr_cache_timeout_,
		int mkdir_copy_sgid_, SugidClearMode sugid_clear_mode_, bool acl_enabled_,
		bool use_rw_lock_, double acl_cache_timeout_, unsigned acl_cache_size_);

void remove_file_info(FileInfo *f);
void remove_dir_info(FileInfo *f);

// TODO what about following fuse_lowlevel_ops functions?
// destroy
// forget
// fsyncdir
// bmap
// ioctl
// poll
// write_buf
// retrieve_reply
// forget_multi
// fallocate
// readdirplus

} // namespace LizardClient
