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

#include "common/chunk_with_address_and_label.h"
#include "common/exception.h"
#include "mount/group_cache.h"
#include "mount/lizard_client_context.h"
#include "mount/readdata_cache.h"
#include "mount/stat_defs.h"
#include "protocol/chunkserver_list_entry.h"
#include "protocol/lock_info.h"
#include "protocol/named_inode_entry.h"

namespace LizardClient {

typedef uint32_t Inode;
typedef uint32_t JobId;
typedef uint32_t NamedInodeOffset;

struct FsInitParams {
	static constexpr const char *kDefaultSubfolder = DEFAULT_MOUNTED_SUBFOLDER;
	static constexpr bool     kDefaultDoNotRememberPassword = false;
	static constexpr bool     kDefaultDelayedInit = false;
#ifdef _WIN32
	static constexpr unsigned kDefaultReportReservedPeriod = 60;
#else
	static constexpr unsigned kDefaultReportReservedPeriod = 30;
#endif
	static constexpr unsigned kDefaultIoRetries = 30;
	static constexpr unsigned kDefaultRoundTime = 200;
	static constexpr unsigned kDefaultChunkserverConnectTo = 2000;
	static constexpr unsigned kDefaultChunkserverReadTo = 2000;
	static constexpr unsigned kDefaultChunkserverWaveReadTo = 500;
	static constexpr unsigned kDefaultChunkserverTotalReadTo = 2000;
	static constexpr unsigned kDefaultCacheExpirationTime = 0;
	static constexpr unsigned kDefaultReadaheadMaxWindowSize = 16384;
	static constexpr bool     kDefaultPrefetchXorStripes = false;

	static constexpr float    kDefaultBandwidthOveruse = 1.0;
	static constexpr unsigned kDefaultChunkserverWriteTo = 5000;
#ifdef _WIN32
	static constexpr unsigned kDefaultWriteCacheSize = 50;
#else
	static constexpr unsigned kDefaultWriteCacheSize = 0;
#endif
	static constexpr unsigned kDefaultCachePerInodePercentage = 25;
	static constexpr unsigned kDefaultWriteWorkers = 10;
	static constexpr unsigned kDefaultWriteWindowSize = 15;
	static constexpr unsigned kDefaultSymlinkCacheTimeout = 3600;
#if FUSE_VERSION >= 30
	static constexpr int      kDefaultNonEmptyMounts = 0;
#endif

	static constexpr bool     kDefaultDebugMode = false;
	static constexpr int      kDefaultKeepCache = 0;
	static constexpr double   kDefaultDirentryCacheTimeout = 0.25;
	static constexpr unsigned kDefaultDirentryCacheSize = 100000;
	static constexpr double   kDefaultEntryCacheTimeout = 0.0;
	static constexpr double   kDefaultAttrCacheTimeout = 1.0;
#ifdef __linux__
	static constexpr bool     kDefaultMkdirCopySgid = true;
#else
	static constexpr bool     kDefaultMkdirCopySgid = false;
#endif
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
	static constexpr SugidClearMode kDefaultSugidClearMode = SugidClearMode::kExt;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
	static constexpr SugidClearMode kDefaultSugidClearMode = SugidClearMode::kBsd;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
	static constexpr SugidClearMode kDefaultSugidClearMode = SugidClearMode::kOsx;
#else
	static constexpr SugidClearMode kDefaultSugidClearMode = SugidClearMode::kNever;
#endif
	static constexpr bool     kDefaultUseRwLock = true;
	static constexpr double   kDefaultAclCacheTimeout = 1.0;
	static constexpr unsigned kDefaultAclCacheSize = 1000;
	static constexpr bool     kDefaultVerbose = false;

	// Thank you, GCC 4.6, for no delegating constructors
	FsInitParams()
	             : bind_host(), host(), port(), meta(false), mountpoint(), subfolder(kDefaultSubfolder),
	             do_not_remember_password(kDefaultDoNotRememberPassword), delayed_init(kDefaultDelayedInit),
	             report_reserved_period(kDefaultReportReservedPeriod),
	             io_retries(kDefaultIoRetries),
	             chunkserver_round_time_ms(kDefaultRoundTime),
	             chunkserver_connect_timeout_ms(kDefaultChunkserverConnectTo),
	             chunkserver_wave_read_timeout_ms(kDefaultChunkserverWaveReadTo),
	             total_read_timeout_ms(kDefaultChunkserverTotalReadTo),
	             cache_expiration_time_ms(kDefaultCacheExpirationTime),
	             readahead_max_window_size_kB(kDefaultReadaheadMaxWindowSize),
	             prefetch_xor_stripes(kDefaultPrefetchXorStripes),
	             bandwidth_overuse(kDefaultBandwidthOveruse),
	             write_cache_size(kDefaultWriteCacheSize),
	             write_workers(kDefaultWriteWorkers), write_window_size(kDefaultWriteWindowSize),
	             chunkserver_write_timeout_ms(kDefaultChunkserverWriteTo),
	             cache_per_inode_percentage(kDefaultCachePerInodePercentage),
	             symlink_cache_timeout_s(kDefaultSymlinkCacheTimeout),
	             debug_mode(kDefaultDebugMode), keep_cache(kDefaultKeepCache),
	             direntry_cache_timeout(kDefaultDirentryCacheTimeout), direntry_cache_size(kDefaultDirentryCacheSize),
	             entry_cache_timeout(kDefaultEntryCacheTimeout), attr_cache_timeout(kDefaultAttrCacheTimeout),
	             mkdir_copy_sgid(kDefaultMkdirCopySgid), sugid_clear_mode(kDefaultSugidClearMode),
	             use_rw_lock(kDefaultUseRwLock),
	             acl_cache_timeout(kDefaultAclCacheTimeout), acl_cache_size(kDefaultAclCacheSize),
	             verbose(kDefaultVerbose) {
	}

	FsInitParams(const std::string &bind_host, const std::string &host, const std::string &port, const std::string &mountpoint)
	             : bind_host(bind_host), host(host), port(port), meta(false), mountpoint(mountpoint), subfolder(kDefaultSubfolder),
	             do_not_remember_password(kDefaultDoNotRememberPassword), delayed_init(kDefaultDelayedInit),
	             report_reserved_period(kDefaultReportReservedPeriod),
	             io_retries(kDefaultIoRetries),
	             chunkserver_round_time_ms(kDefaultRoundTime),
	             chunkserver_connect_timeout_ms(kDefaultChunkserverConnectTo),
	             chunkserver_wave_read_timeout_ms(kDefaultChunkserverWaveReadTo),
	             total_read_timeout_ms(kDefaultChunkserverTotalReadTo),
	             cache_expiration_time_ms(kDefaultCacheExpirationTime),
	             readahead_max_window_size_kB(kDefaultReadaheadMaxWindowSize),
	             prefetch_xor_stripes(kDefaultPrefetchXorStripes),
	             bandwidth_overuse(kDefaultBandwidthOveruse),
	             write_cache_size(kDefaultWriteCacheSize),
	             write_workers(kDefaultWriteWorkers), write_window_size(kDefaultWriteWindowSize),
	             chunkserver_write_timeout_ms(kDefaultChunkserverWriteTo),
	             cache_per_inode_percentage(kDefaultCachePerInodePercentage),
	             symlink_cache_timeout_s(kDefaultSymlinkCacheTimeout),
	             debug_mode(kDefaultDebugMode), keep_cache(kDefaultKeepCache),
	             direntry_cache_timeout(kDefaultDirentryCacheTimeout), direntry_cache_size(kDefaultDirentryCacheSize),
	             entry_cache_timeout(kDefaultEntryCacheTimeout), attr_cache_timeout(kDefaultAttrCacheTimeout),
	             mkdir_copy_sgid(kDefaultMkdirCopySgid), sugid_clear_mode(kDefaultSugidClearMode),
	             use_rw_lock(kDefaultUseRwLock),
	             acl_cache_timeout(kDefaultAclCacheTimeout), acl_cache_size(kDefaultAclCacheSize),
	             verbose(kDefaultVerbose) {
	}

	std::string bind_host;
	std::string host;
	std::string port;
	bool meta;
	std::string mountpoint;
	std::string subfolder;
	std::vector<uint8_t> password_digest;
	bool do_not_remember_password;
	bool delayed_init;
	unsigned report_reserved_period;

	unsigned io_retries;
	unsigned chunkserver_round_time_ms;
	unsigned chunkserver_connect_timeout_ms;
	unsigned chunkserver_wave_read_timeout_ms;
	unsigned total_read_timeout_ms;
	unsigned cache_expiration_time_ms;
	unsigned readahead_max_window_size_kB;
	bool prefetch_xor_stripes;
	double bandwidth_overuse;

	unsigned write_cache_size;
	unsigned write_workers;
	unsigned write_window_size;
	unsigned chunkserver_write_timeout_ms;
	unsigned cache_per_inode_percentage;
	unsigned symlink_cache_timeout_s;

	bool debug_mode;
	// NOTICE(sarna): This variable can hold more values than 0-1, don't change it to bool ever.
	int keep_cache;
	double direntry_cache_timeout;
	unsigned direntry_cache_size;
	double entry_cache_timeout;
	double attr_cache_timeout;
	bool mkdir_copy_sgid;
	SugidClearMode sugid_clear_mode;
	bool use_rw_lock;
	double acl_cache_timeout;
	unsigned acl_cache_size;

	bool verbose;

	std::string io_limits_config_file;
};

/**
 * A class that is used for passing information between subsequent calls to the filesystem.
 * It is created when a file is opened, updated with every use of the file descriptor and
 * removed when a file is closed.
 */
struct FileInfo {
	FileInfo() : flags(), direct_io(), keep_cache(), fh(), lock_owner() {}

	FileInfo(int flags, unsigned int direct_io, unsigned int keep_cache, uint64_t fh,
		uint64_t lock_owner)
			: flags(flags),
			direct_io(direct_io),
			keep_cache(keep_cache),
			fh(fh),
			lock_owner(lock_owner) {
	}

	FileInfo(const FileInfo &other) = default;
	FileInfo(FileInfo &&other) = default;

	FileInfo &operator=(const FileInfo &other) = default;
	FileInfo &operator=(FileInfo &&other) = default;

	bool isValid() const {
		return fh;
	}

	void reset() {
		*this = FileInfo();
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
#if FUSE_USE_VERSION >= 30
	uint64_t generation;
#else
	unsigned long generation;
#endif
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
	explicit RequestException(int error_code);

	int system_error_code;
	int lizardfs_error_code;
};

void updateGroups(Context &ctx);

// TODO what about this one? Will decide when writing non-fuse client
// void fsinit(void *userdata, struct fuse_conn_info *conn);
bool isSpecialInode(LizardClient::Inode ino);

EntryParam lookup(const Context &ctx, Inode parent, const char *name);

AttrReply getattr(const Context &ctx, Inode ino);

#define LIZARDFS_SET_ATTR_MODE      (1 << 0)
#define LIZARDFS_SET_ATTR_UID       (1 << 1)
#define LIZARDFS_SET_ATTR_GID       (1 << 2)
#define LIZARDFS_SET_ATTR_SIZE      (1 << 3)
#define LIZARDFS_SET_ATTR_ATIME     (1 << 4)
#define LIZARDFS_SET_ATTR_MTIME     (1 << 5)
#define LIZARDFS_SET_ATTR_ATIME_NOW (1 << 7)
#define LIZARDFS_SET_ATTR_MTIME_NOW (1 << 8)
AttrReply setattr(const Context &ctx, Inode ino, struct stat *stbuf, int to_set);

std::string readlink(const Context &ctx, Inode ino);

EntryParam mknod(const Context &ctx, Inode parent, const char *name, mode_t mode, dev_t rdev);

EntryParam mkdir(const Context &ctx, Inode parent, const char *name, mode_t mode);

void unlink(const Context &ctx, Inode parent, const char *name);

void undel(const Context &ctx, Inode ino);

void rmdir(const Context &ctx, Inode parent, const char *name);

EntryParam symlink(const Context &ctx, const char *link, Inode parent, const char *name);

void rename(const Context &ctx, Inode parent, const char *name, Inode newparent, const char *newname);

EntryParam link(const Context &ctx, Inode ino, Inode newparent, const char *newname);

void open(const Context &ctx, Inode ino, FileInfo* fi);

std::vector<uint8_t> read_special_inode(const Context &ctx, Inode ino, size_t size, off_t off,
				        FileInfo* fi);

ReadCache::Result read(const Context &ctx, Inode ino, size_t size, off_t off, FileInfo* fi);

typedef size_t BytesWritten;
BytesWritten write(const Context &ctx, Inode ino, const char *buf, size_t size, off_t off,
		FileInfo* fi);

void flush(const Context &ctx, Inode ino, FileInfo* fi);

void release(Inode ino, FileInfo* fi);

void fsync(const Context &ctx, Inode ino, int datasync, FileInfo* fi);

void opendir(const Context &ctx, Inode ino);

std::vector<DirEntry> readdir(const Context &ctx, Inode ino, off_t off, size_t max_entries);

std::vector<NamedInodeEntry> readreserved(const Context &ctx, NamedInodeOffset offset, NamedInodeOffset max_entries);

std::vector<NamedInodeEntry> readtrash(const Context &ctx, NamedInodeOffset offset, NamedInodeOffset max_entries);

void releasedir(Inode ino);

struct statvfs statfs(const Context &ctx, Inode ino);

void setxattr(const Context &ctx, Inode ino, const char *name, const char *value,
		size_t size, int flags, uint32_t position);

XattrReply getxattr(const Context &ctx, Inode ino, const char *name, size_t size, uint32_t position);

XattrReply listxattr(const Context &ctx, Inode ino, size_t size);

void removexattr(const Context &ctx, Inode ino, const char *name);

void access(const Context &ctx, Inode ino, int mask);

EntryParam create(const Context &ctx, Inode parent, const char *name,
		mode_t mode, FileInfo* fi);

void getlk(const Context &ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock);
uint32_t setlk_send(const Context &ctx, Inode ino, FileInfo* fi, struct lzfs_locks::FlockWrapper &lock);
void setlk_recv();
uint32_t flock_send(const Context &ctx, Inode ino, FileInfo* fi, int op);
void flock_recv();

void flock_interrupt(const lzfs_locks::InterruptData &data);
void setlk_interrupt(const lzfs_locks::InterruptData &data);

void remove_file_info(FileInfo *f);
void remove_dir_info(FileInfo *f);

JobId makesnapshot(const Context &ctx, Inode ino, Inode dst_parent, const std::string &dst_name,
	          bool can_overwrite);
std::string getgoal(const Context &ctx, Inode ino);
void setgoal(const Context &ctx, Inode ino, const std::string &goal_name, uint8_t smode);

void statfs(uint64_t *totalspace, uint64_t *availspace, uint64_t *trashspace, uint64_t *reservedspace, uint32_t *inodes);

std::vector<ChunkWithAddressAndLabel> getchunksinfo(const Context &ctx, Inode ino,
	                                                uint32_t chunk_index, uint32_t chunk_count);

std::vector<ChunkserverListEntry> getchunkservers();

void fs_init(FsInitParams &params);
void fs_term();

}
