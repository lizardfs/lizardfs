#pragma once
#include "common/platform.h"

#include "mount/lizard_client_context.h"

#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

#include <common/exception.h>

namespace LizardClient {

typedef unsigned long int Inode;

/**
 * A class that is used for passing information between subsequent calls to the filesystem.
 * It is created when a file is opened, updated with every use of the file descriptor and
 * removed when a file is closed.
 */
struct FileInfo {
	FileInfo(int flags, unsigned int direct_io, unsigned int keep_cache, uint64_t fh)
			: flags(flags), direct_io(direct_io), keep_cache(keep_cache), fh(fh) {
	}

	int flags;
	unsigned int direct_io : 1;
	unsigned int keep_cache : 1;
	uint64_t fh;
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
};

/**
 * A result of getxattr, setxattr and listattr operations
 */
struct XattrReply {
	uint32_t valueLength;
	std::vector<uint8_t> valueBuffer;
};

/**
 * An exception that is thrown when a request can't be executed succesfully
 */
struct RequestException : public std::exception {
	RequestException(int errNo);

	int errNo;
};

// TODO what about this one? Will decide when writing non-fuse client
// void fsinit(void *userdata, struct fuse_conn_info *conn);

EntryParam lookup(Context ctx, Inode parent, const char *name);

AttrReply getattr(Context ctx, Inode ino, FileInfo* fi);

#define LIZARDFS_SET_ATTR_MODE      (1 << 0)
#define LIZARDFS_SET_ATTR_UID       (1 << 1)
#define LIZARDFS_SET_ATTR_GID       (1 << 2)
#define LIZARDFS_SET_ATTR_SIZE      (1 << 3)
#define LIZARDFS_SET_ATTR_ATIME     (1 << 4)
#define LIZARDFS_SET_ATTR_MTIME     (1 << 5)
#define LIZARDFS_SET_ATTR_ATIME_NOW (1 << 7)
#define LIZARDFS_SET_ATTR_MTIME_NOW (1 << 8)
AttrReply setattr(Context ctx, Inode ino, struct stat *attr, int to_set, FileInfo* fi);

std::string readlink(Context ctx, Inode ino);

EntryParam mknod(Context ctx, Inode parent, const char *name, mode_t mode, dev_t rdev);

EntryParam mkdir(Context ctx, Inode parent, const char *name, mode_t mode);

void unlink(Context ctx, Inode parent, const char *name);

void rmdir(Context ctx, Inode parent, const char *name);

EntryParam symlink(Context ctx, const char *link, Inode parent, const char *name);

void rename(Context ctx, Inode parent, const char *name, Inode newparent, const char *newname);

EntryParam link(Context ctx, Inode ino, Inode newparent, const char *newname);

void open(Context ctx, Inode ino, FileInfo* fi);

std::vector<uint8_t> read(Context ctx, Inode ino, size_t size, off_t off, FileInfo* fi);

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

void init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
		double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
		SugidClearMode sugid_clear_mode_, bool acl_enabled_);

void remove_file_info(FileInfo *f);
void remove_dir_info(FileInfo *f);

// TODO what about following fuse_lowlevel_ops functions?
// destroy
// forget
// fsyncdir
// getlk
// setlk
// bmap
// ioctl
// poll
// write_buf
// retrieve_reply
// forget_multi
// flock
// fallocate
// readdirplus

} // namespace LizardClient
