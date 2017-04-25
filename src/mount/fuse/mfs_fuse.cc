/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS

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

#include "common/platform.h"
#include "mount/fuse/mfs_fuse.h"

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/massert.h"
#include "common/small_vector.h"
#include "common/special_inode_defs.h"
#include "mount/fuse/lock_conversion.h"
#include "mount/lizard_client.h"
#include "mount/lizard_client_context.h"
#include "mount/thread_safe_map.h"
#include "protocol/MFSCommunication.h"

#if SPECIAL_INODE_ROOT != FUSE_ROOT_ID
#error FUSE_ROOT_ID is not equal to SPECIAL_INODE_ROOT
#endif

#define READDIR_BUFFSIZE 50000

/**
 * Function checking if types are equal, ignoring constness
 */
template <class A, class B>
void checkTypesEqual(const A& a, const B& b) {
	static_assert(std::is_same<decltype(a), decltype(b)>::value,
			"Types don't match");
}

void updateGroupsForContext(fuse_req_t &req, LizardClient::Context &ctx) {
#if defined(__APPLE__)
	(void)req, (void)ctx;
#else
	static const int kMaxGroups = GroupCache::kDefaultGroupsSize - 1;

	GroupCache::Groups groups(kMaxGroups + 1);
	// First group is always the primary group. It may be duplicated later but it is not a problem.
	groups[0] = ctx.gid;
	int getgroups_ret = fuse_req_getgroups(req, kMaxGroups, groups.data() + 1);
	if (getgroups_ret > kMaxGroups) {
		groups.resize(getgroups_ret + 1);
		getgroups_ret = fuse_req_getgroups(req, groups.size() - 1, groups.data() + 1);
	} else if (getgroups_ret >= 0) {
		groups.resize(getgroups_ret + 1);
	}
	if (getgroups_ret > 0) {
		ctx.gid = LizardClient::updateGroups(groups);
	}
#endif
}

/**
 * A function converting fuse_ctx to LizardClient::Context
 */
LizardClient::Context get_context(fuse_req_t& req) {
	auto fuse_ctx = fuse_req_ctx(req);
#if (FUSE_VERSION >= 28)
	mode_t umask = fuse_ctx->umask;
#else
	mode_t umask = 0000;
#endif
	auto ret = LizardClient::Context(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid, umask);
	checkTypesEqual(ret.uid,   fuse_ctx->uid);
	checkTypesEqual(ret.gid,   fuse_ctx->gid);
	checkTypesEqual(ret.pid,   fuse_ctx->pid);
	checkTypesEqual(ret.umask, umask);
	updateGroupsForContext(req, ret);
	return ret;
}

/**
 * A wrapper that allows one to use fuse_file_info as if it was LizardClient::FileInfo object.
 *  During construction, LizardClient::FileInfo object is initialized with information from the
 *  provided fuse_file_info.
 *  During destruction, the fuse_file_info is updated to reflect any changes made to the
 *  LizardClient::FileInfo object.
 */
class fuse_file_info_wrapper {
public:
	fuse_file_info_wrapper(fuse_file_info* fi)
			: fuse_fi_(fi), fs_fi_(fuse_fi_
					? new LizardClient::FileInfo(fi->flags, fi->direct_io, fi->keep_cache, fi->fh,
					fi->lock_owner)
					: nullptr) {
	}
	operator LizardClient::FileInfo*() {
		return fs_fi_.get();
	}
	~fuse_file_info_wrapper() {
		if (fs_fi_) {
			sassert(fuse_fi_);
			fuse_fi_->direct_io  = fs_fi_->direct_io;
			fuse_fi_->fh         = fs_fi_->fh;
			fuse_fi_->flags      = fs_fi_->flags;
			fuse_fi_->keep_cache = fs_fi_->keep_cache;
			checkTypesEqual(fuse_fi_->direct_io , fs_fi_->direct_io);
			checkTypesEqual(fuse_fi_->fh        , fs_fi_->fh);
			checkTypesEqual(fuse_fi_->flags     , fs_fi_->flags);
			checkTypesEqual(fuse_fi_->keep_cache, fs_fi_->keep_cache);
		} else {
			sassert(!fuse_fi_);
		}
	}

private:
	fuse_file_info* fuse_fi_;
	std::unique_ptr<LizardClient::FileInfo> fs_fi_;
};

/**
 * A function converting LizardFS::EntryParam to fuse_entry_param
 */
fuse_entry_param make_fuse_entry_param(const LizardClient::EntryParam& e) {
	fuse_entry_param ret;
	memset(&ret, 0, sizeof(ret));
	checkTypesEqual(ret.ino,           e.ino);
	checkTypesEqual(ret.generation,    e.generation);
	checkTypesEqual(ret.attr,          e.attr);
	checkTypesEqual(ret.attr_timeout,  e.attr_timeout);
	checkTypesEqual(ret.entry_timeout, e.entry_timeout);
	ret.ino           = e.ino;
	ret.generation    = e.generation;
	ret.attr          = e.attr;
	ret.attr_timeout  = e.attr_timeout;
	ret.entry_timeout = e.entry_timeout;
	return ret;
}

#ifndef EDQUOT
# define EDQUOT ENOSPC
#endif
#ifndef ENOATTR
# ifdef ENODATA
#  define ENOATTR ENODATA
# else
#  define ENOATTR ENOENT
# endif
#endif

static_assert(std::is_same<LizardClient::Inode, fuse_ino_t>::value, "Types don't match");


ThreadSafeMap<std::uintptr_t, lzfs_locks::InterruptData> gLockInterruptData;

#if FUSE_USE_VERSION >= 26
void mfs_statfs(fuse_req_t req,fuse_ino_t ino) {
#else
void mfs_statfs(fuse_req_t req) {
	fuse_ino_t ino = 0;
#endif
	try {
		auto a = LizardClient::statfs(get_context(req), ino);
		fuse_reply_statfs(req, &a);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_access(fuse_req_t req, fuse_ino_t ino, int mask) {
	try {
		LizardClient::access(get_context(req), ino, mask);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::lookup(get_context(req), parent, name));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		auto a = LizardClient::getattr(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_attr(req, &a.attr, a.attrTimeout);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set,
		struct fuse_file_info *fi) {
	try {
		static_assert(LIZARDFS_SET_ATTR_MODE      == FUSE_SET_ATTR_MODE,      "incompatible");
		static_assert(LIZARDFS_SET_ATTR_UID       == FUSE_SET_ATTR_UID,       "incompatible");
		static_assert(LIZARDFS_SET_ATTR_GID       == FUSE_SET_ATTR_GID,       "incompatible");
		static_assert(LIZARDFS_SET_ATTR_SIZE      == FUSE_SET_ATTR_SIZE,      "incompatible");
		static_assert(LIZARDFS_SET_ATTR_ATIME     == FUSE_SET_ATTR_ATIME,     "incompatible");
		static_assert(LIZARDFS_SET_ATTR_MTIME     == FUSE_SET_ATTR_MTIME,     "incompatible");
#if (FUSE_VERSION >= 28)
		static_assert(LIZARDFS_SET_ATTR_ATIME_NOW == FUSE_SET_ATTR_ATIME_NOW, "incompatible");
		static_assert(LIZARDFS_SET_ATTR_MTIME_NOW == FUSE_SET_ATTR_MTIME_NOW, "incompatible");
#endif

		auto a = LizardClient::setattr(
				get_context(req), ino, stbuf, to_set, fuse_file_info_wrapper(fi));
		fuse_reply_attr(req, &a.attr, a.attrTimeout);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::mknod(get_context(req), parent, name, mode, rdev));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		LizardClient::unlink(get_context(req), parent, name);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::mkdir(get_context(req), parent, name, mode));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		LizardClient::rmdir(get_context(req), parent, name);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::symlink(get_context(req), path, parent, name));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_readlink(fuse_req_t req, fuse_ino_t ino) {
	try {
		fuse_reply_readlink(req,
				LizardClient::readlink(get_context(req), ino).c_str());
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent,
		const char *newname) {
	try {
		LizardClient::rename(get_context(req), parent, name, newparent, newname);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::link(get_context(req), ino, newparent, newname));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::opendir(get_context(req), ino, fuse_file_info_wrapper(fi));
		if (fuse_reply_open(req, fi) == -ENOENT) {
			assert(fi->fh == 0);
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		struct fuse_file_info *fi) {
	try {
		char buffer[READDIR_BUFFSIZE];
		if (size > READDIR_BUFFSIZE) {
			size = READDIR_BUFFSIZE;
		}
		size_t bytesInBuffer = 0;
		bool end = false;
		while (!end) {
			// Calculate approximated number of entries which will fit in the buffer. If this
			// number is smaller than the actual value, LizardClient::readdir will be called more
			// than once for a single mfs_readdir (this will eg. generate more oplog entries than
			// one might expect). If it's bigger, the code will be slightly less optimal because
			// superfluous entries will be extracted by LizardClient::readdir and then discarded by
			// us. Using maxEntries=+inf makes the complexity of the getdents syscall O(n^2).
			// The expression below generates some upper bound of the actual number of entries
			// to be returned (because fuse adds 24 bytes of metadata to each file name in
			// fuse_add_direntry and aligns size up to 8 bytes), so LizardClient::readdir
			// should be called only once.
			size_t maxEntries = 1 + size / 32;
			// Now extract some entries and rewrite them into the buffer.
			auto fsDirEntries = LizardClient::readdir(get_context(req),
					ino, off, maxEntries, fuse_file_info_wrapper(fi));
			if (fsDirEntries.empty()) {
				break; // no more entries (we don't need to set 'end = true' here to end the loop)
			}
			for (const auto& e : fsDirEntries) {
				size_t entrySize = fuse_add_direntry(req,
						buffer + bytesInBuffer, size,
						e.name.c_str(), &(e.attr), e.nextEntryOffset);
				if (entrySize > size) {
					end = true; // buffer is full
					break;
				}
				off = e.nextEntryOffset; // update offset of the next call to LizardClient::readdir
				bytesInBuffer += entrySize;
				size -= entrySize; // decrease remaining buffer size
			}
		}
		fuse_reply_buf(req, buffer, bytesInBuffer);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::releasedir(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
		struct fuse_file_info *fi) {
	try {
		auto e = make_fuse_entry_param(LizardClient::create(
				get_context(req), parent, name, mode, fuse_file_info_wrapper(fi)));
		if (fuse_reply_create(req, &e, fi) == -ENOENT) {
			LizardClient::remove_file_info(fuse_file_info_wrapper(fi));
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::open(get_context(req), ino, fuse_file_info_wrapper(fi));
		if (fuse_reply_open(req, fi) == -ENOENT) {
			LizardClient::remove_file_info(fuse_file_info_wrapper(fi));
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::release(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	try {
		if (LizardClient::isSpecialInode(ino)) {
			auto ret = LizardClient::read_special_inode(
				   get_context(req), ino, size, off, fuse_file_info_wrapper(fi));
			fuse_reply_buf(req, (char*) ret.data(), ret.size());
		} else {
			ReadCache::Result ret = LizardClient::read(
					get_context(req), ino, size, off, fuse_file_info_wrapper(fi));

			small_vector<struct iovec, 8> reply;
			ret.toIoVec(reply, off, size);
			fuse_reply_iov(req, reply.data(), reply.size());
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off,
		struct fuse_file_info *fi) {
	try {
		fuse_reply_write(req, LizardClient::write(
				get_context(req), ino, buf, size, off, fuse_file_info_wrapper(fi)));
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::flush(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	try {
		LizardClient::fsync(get_context(req), ino, datasync, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

#if defined(__APPLE__)
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value,
		size_t size, int flags, uint32_t position) {
#else
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value,
		size_t size, int flags) {
	uint32_t position=0;
#endif
	try {
		LizardClient::setxattr(get_context(req), ino, name, value, size, flags, position);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

#if defined(__APPLE__)
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size,
		uint32_t position) {
#else
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) {
	uint32_t position=0;
#endif /* __APPLE__ */
	try {
		auto a = LizardClient::getxattr(get_context(req), ino, name, size, position);
		if (size == 0) {
			fuse_reply_xattr(req, a.valueLength);
		} else {
			fuse_reply_buf(req,(const char*)a.valueBuffer.data(), a.valueLength);
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size) {
	try {
		auto a = LizardClient::listxattr(get_context(req), ino, size);
		if (size == 0) {
			fuse_reply_xattr(req, a.valueLength);
		} else {
			fuse_reply_buf(req,(const char*)a.valueBuffer.data(), a.valueLength);
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_removexattr (fuse_req_t req, fuse_ino_t ino, const char *name) {
	try {
		LizardClient::removexattr(get_context(req), ino, name);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void mfs_init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
		unsigned direntry_cache_size_, double entry_cache_timeout_,
		double attr_cache_timeout_, int mkdir_copy_sgid_, SugidClearMode sugid_clear_mode_,
		bool acl_enabled_, double acl_cache_timeout_, unsigned acl_cache_size_,
		bool use_rwlock_) {
	LizardClient::init(debug_mode_, keep_cache_, direntry_cache_timeout_, direntry_cache_size_,
	                   entry_cache_timeout_, attr_cache_timeout_, mkdir_copy_sgid_,
	                   sugid_clear_mode_, acl_enabled_, use_rwlock_, acl_cache_timeout_,
	                   acl_cache_size_);
}

#if FUSE_VERSION >= 26
void lzfs_flock_interrupt(fuse_req_t req, void *data) {
	auto interrupt_data = gLockInterruptData.take(reinterpret_cast<std::uintptr_t>(data));

	// if there was any data
	if (interrupt_data.first) {
		// handle interrupt
		LizardClient::flock_interrupt(interrupt_data.second);
		fuse_reply_err(req, EINTR);
	}
}

void lzfs_setlk_interrupt(fuse_req_t req, void *data) {
	auto interrupt_data = gLockInterruptData.take(reinterpret_cast<std::uintptr_t>(data));

	// if there was any data
	if (interrupt_data.first) {
		// handle interrupt
		LizardClient::setlk_interrupt(interrupt_data.second);
		fuse_reply_err(req, EINTR);
	}
}

void lzfs_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock) {
	try {
		if (!lzfs_locks::posixOpValid(lock->l_type)) {
			fuse_reply_err(req, EINVAL);
			return;
		}

		lzfs_locks::FlockWrapper lzfslock = lzfs_locks::convertPLock(*lock, 1);
		LizardClient::getlk(get_context(req), ino, fuse_file_info_wrapper(fi), lzfslock);
		struct flock retlock = lzfs_locks::convertToFlock(lzfslock);
		fuse_reply_lock(req, &retlock);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}


void lzfs_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sleep) {
	std::uintptr_t interrupt_data_key = gLockInterruptData.generateKey();
	try {
		if (fuse_req_interrupted(req)) {
			fuse_reply_err(req, EINTR);
			return;
		}

		if (!lzfs_locks::posixOpValid(lock->l_type)) {
			fuse_reply_err(req, EINVAL);
			return;
		}

		lzfs_locks::FlockWrapper lzfslock = lzfs_locks::convertPLock(*lock, sleep);
		uint32_t reqid = LizardClient::setlk_send(get_context(req), ino, fuse_file_info_wrapper(fi),
				lzfslock);

		// save interrupt data in static structure
		gLockInterruptData.put(interrupt_data_key,
				       lzfs_locks::InterruptData(fi->lock_owner, ino, reqid));

		// register interrupt handle
		if (lzfslock.l_type == lzfs_locks::kShared || lzfslock.l_type == lzfs_locks::kExclusive) {
			fuse_req_interrupt_func(req, lzfs_setlk_interrupt,
					reinterpret_cast<void*>(interrupt_data_key));
		}

		// WARNING: csetlk_recv() won't work with polonaise server,
		// since actual code requires setlk_send()
		// to be executed by the same thread.
		LizardClient::setlk_recv();

		// release the memory
		auto interrupt_data = gLockInterruptData.take(interrupt_data_key);
		if (interrupt_data.first) {
			fuse_reply_err(req, 0);
		}
	} catch (LizardClient::RequestException& e) {
		// release the memory
		auto interrupt_data = gLockInterruptData.take(interrupt_data_key);
		if (interrupt_data.first) {
			fuse_reply_err(req, e.errNo);
		}
	}
}
#endif
#if FUSE_VERSION >= 29

void lzfs_flock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, int op) {
	std::uintptr_t interrupt_data_key = gLockInterruptData.generateKey();
	try {
		if (fuse_req_interrupted(req)) {
			fuse_reply_err(req, EINTR);
			return;
		}

		if (!lzfs_locks::flockOpValid(op)) {
			fuse_reply_err(req, EINVAL);
			return;
		}

		uint32_t lzfs_op = lzfs_locks::flockOpConv(op);
		uint32_t reqid = LizardClient::flock_send(get_context(req), ino,
			fuse_file_info_wrapper(fi), lzfs_op);

		// save interrupt data in static structure
		gLockInterruptData.put(interrupt_data_key,
				       lzfs_locks::InterruptData(fi->lock_owner, ino, reqid));
		// register interrupt handle
		if (lzfs_op == lzfs_locks::kShared || lzfs_op == lzfs_locks::kExclusive) {
			fuse_req_interrupt_func(req, lzfs_flock_interrupt,
					reinterpret_cast<void*>(interrupt_data_key));
		}

		LizardClient::flock_recv();

		// release the memory
		auto interrupt_data = gLockInterruptData.take(interrupt_data_key);
		if (interrupt_data.first) {
			fuse_reply_err(req, 0);
		}
	} catch (LizardClient::RequestException& e) {
		// release the memory
		auto interrupt_data = gLockInterruptData.take(interrupt_data_key);
		if (interrupt_data.first) {
			fuse_reply_err(req, e.errNo);
		}
	}
}
#endif
