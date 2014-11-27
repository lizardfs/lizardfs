/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of LizardFS and is part of LizardFS

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
#include "mount/fuse/lfs_fuse.h"

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/massert.h"
#include "common/LFSCommunication.h"
#include "mount/lizard_client.h"
#include "mount/lizard_client_context.h"

#if LFS_ROOT_ID != FUSE_ROOT_ID
#error FUSE_ROOT_ID is not equal to LFS_ROOT_ID
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

/**
 * A function converting fuse_ctx to LizardClient::Context
 */
LizardClient::Context get_context(fuse_req_t& req) {
	auto fuse_ctx = fuse_req_ctx(req);
	auto ret = LizardClient::Context(fuse_ctx->uid, fuse_ctx->gid, fuse_ctx->pid, fuse_ctx->umask);
	checkTypesEqual(ret.uid,   fuse_ctx->uid);
	checkTypesEqual(ret.gid,   fuse_ctx->gid);
	checkTypesEqual(ret.pid,   fuse_ctx->pid);
	checkTypesEqual(ret.umask, fuse_ctx->umask);
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
					? new LizardClient::FileInfo(fi->flags, fi->direct_io, fi->keep_cache, fi->fh)
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

#if FUSE_USE_VERSION >= 26
void lfs_statfs(fuse_req_t req,fuse_ino_t ino) {
#else
void lfs_statfs(fuse_req_t req) {
	fuse_ino_t ino = 0;
#endif
	try {
		auto a = LizardClient::statfs(get_context(req), ino);
		fuse_reply_statfs(req, &a);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_access(fuse_req_t req, fuse_ino_t ino, int mask) {
	try {
		LizardClient::access(get_context(req), ino, mask);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::lookup(get_context(req), parent, name));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		auto a = LizardClient::getattr(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_attr(req, &a.attr, a.attrTimeout);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set,
		struct fuse_file_info *fi) {
	try {
		static_assert(LIZARDFS_SET_ATTR_MODE      == FUSE_SET_ATTR_MODE,      "incompatible");
		static_assert(LIZARDFS_SET_ATTR_UID       == FUSE_SET_ATTR_UID,       "incompatible");
		static_assert(LIZARDFS_SET_ATTR_GID       == FUSE_SET_ATTR_GID,       "incompatible");
		static_assert(LIZARDFS_SET_ATTR_SIZE      == FUSE_SET_ATTR_SIZE,      "incompatible");
		static_assert(LIZARDFS_SET_ATTR_ATIME     == FUSE_SET_ATTR_ATIME,     "incompatible");
		static_assert(LIZARDFS_SET_ATTR_MTIME     == FUSE_SET_ATTR_MTIME,     "incompatible");
		static_assert(LIZARDFS_SET_ATTR_ATIME_NOW == FUSE_SET_ATTR_ATIME_NOW, "incompatible");
		static_assert(LIZARDFS_SET_ATTR_MTIME_NOW == FUSE_SET_ATTR_MTIME_NOW, "incompatible");

		auto a = LizardClient::setattr(
				get_context(req), ino, stbuf, to_set, fuse_file_info_wrapper(fi));
		fuse_reply_attr(req, &a.attr, a.attrTimeout);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::mknod(get_context(req), parent, name, mode, rdev));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		LizardClient::unlink(get_context(req), parent, name);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::mkdir(get_context(req), parent, name, mode));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		LizardClient::rmdir(get_context(req), parent, name);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::symlink(get_context(req), path, parent, name));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_readlink(fuse_req_t req, fuse_ino_t ino) {
	try {
		fuse_reply_readlink(req,
				LizardClient::readlink(get_context(req), ino).c_str());
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent,
		const char *newname) {
	try {
		LizardClient::rename(get_context(req), parent, name, newparent, newname);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
	try {
		auto fuseEntryParam = make_fuse_entry_param(
				LizardClient::link(get_context(req), ino, newparent, newname));
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::opendir(get_context(req), ino, fuse_file_info_wrapper(fi));
		if (fuse_reply_open(req, fi) == -ENOENT) {
			LizardClient::remove_dir_info(fuse_file_info_wrapper(fi));
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
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
			// than once for a single lfs_readdir (this will eg. generate more oplog entries than
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
				end = true; // no more entries
				break;
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

void lfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::releasedir(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
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

void lfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::open(get_context(req), ino, fuse_file_info_wrapper(fi));
		if (fuse_reply_open(req, fi) == -ENOENT) {
			LizardClient::remove_file_info(fuse_file_info_wrapper(fi));
		}
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::release(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	try {
		auto ret = LizardClient::read(
				get_context(req), ino, size, off, fuse_file_info_wrapper(fi));
		fuse_reply_buf(req, (char*) ret.data(), ret.size());
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off,
		struct fuse_file_info *fi) {
	try {
		fuse_reply_write(req, LizardClient::write(
				get_context(req), ino, buf, size, off, fuse_file_info_wrapper(fi)));
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		LizardClient::flush(get_context(req), ino, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	try {
		LizardClient::fsync(get_context(req), ino, datasync, fuse_file_info_wrapper(fi));
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

#if defined(__APPLE__)
void lfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value,
		size_t size, int flags, uint32_t position) {
#else
void lfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value,
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
void lfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size,
		uint32_t position) {
#else
void lfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) {
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

void lfs_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size) {
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

void lfs_removexattr (fuse_req_t req, fuse_ino_t ino, const char *name) {
	try {
		LizardClient::removexattr(get_context(req), ino, name);
		fuse_reply_err(req, 0);
	} catch (LizardClient::RequestException& e) {
		fuse_reply_err(req, e.errNo);
	}
}

void lfs_init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
		double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
		SugidClearMode sugid_clear_mode_, bool acl_enabled_, double acl_cache_timeout_,
		unsigned acl_cache_size_) {
	LizardClient::init(debug_mode_, keep_cache_, direntry_cache_timeout_, entry_cache_timeout_,
			attr_cache_timeout_, mkdir_copy_sgid_, sugid_clear_mode_, acl_enabled_,
			acl_cache_timeout_, acl_cache_size_);
}
