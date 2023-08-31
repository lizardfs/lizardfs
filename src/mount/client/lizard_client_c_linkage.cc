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

#include "common/platform.h"

#include "client/lizard_client_c_linkage.h"

typedef LizardClient::EntryParam EntryParam;
typedef LizardClient::Inode Inode;
typedef LizardClient::Context Context;
typedef LizardClient::AttrReply AttrReply;
typedef LizardClient::FileInfo FileInfo;
typedef LizardClient::BytesWritten BytesWritten;
typedef LizardClient::DirEntry DirEntry;
typedef LizardClient::RequestException RequestException;

int lizardfs_fs_init(LizardClient::FsInitParams &params) {
	try {
		LizardClient::fs_init(params);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_lookup(Context &ctx, Inode parent, const char *name, EntryParam &param) {
	try {
		param = LizardClient::lookup(ctx, parent, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_mknod(Context &ctx, Inode parent, const char *name, mode_t mode, dev_t rdev,
		EntryParam &param) {
	try {
		param = LizardClient::mknod(ctx, parent, name, mode, rdev);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_link(Context ctx, Inode inode, Inode parent, const char *name,
		EntryParam &param) {
	try {
		param = LizardClient::link(ctx, inode, parent, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_symlink(Context ctx, const char *link, Inode parent, const char *name,
		EntryParam &param) {
	try {
		param = LizardClient::symlink(ctx, link, parent, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_unlink(Context &ctx, Inode parent, const char *name) {
	try {
		LizardClient::unlink(ctx, parent, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_undel(Context &ctx, Inode ino) {
	try {
		LizardClient::undel(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_open(Context &ctx, Inode ino, FileInfo *fi) {
	try {
		LizardClient::open(ctx, ino, fi);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_getattr(Context &ctx, Inode ino, AttrReply &reply) {
	try {
		reply = LizardClient::getattr(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

std::pair<int, LizardClient::JobId> lizardfs_makesnapshot(Context &ctx, Inode ino, Inode dst_parent,
	                                       const std::string &dst_name, bool can_overwrite) {
	try {
		LizardClient::JobId job_id = LizardClient::makesnapshot(ctx, ino, dst_parent,
		                                                        dst_name, can_overwrite);
		return {LIZARDFS_STATUS_OK, job_id};
	} catch (RequestException &e) {
		return {e.lizardfs_error_code, 0};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, 0};
	}
}

int lizardfs_getgoal(Context &ctx, Inode ino, std::string &goal) {
	try {
		goal = LizardClient::getgoal(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_setattr(Context &ctx, Inode ino, struct stat *stbuf, int to_set,
	             AttrReply &reply) {
	try {
		reply = LizardClient::setattr(ctx, ino, stbuf, to_set);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_setgoal(Context &ctx, Inode ino, const std::string &goal_name, uint8_t smode) {
	try {
		LizardClient::setgoal(ctx, ino, goal_name, smode);
		return LIZARDFS_STATUS_OK;
	} catch (RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

std::pair<int, ReadCache::Result> lizardfs_read(Context &ctx, Inode ino, size_t size,
	                                        off_t off, FileInfo *fi) {
	try {
		return std::pair<int, ReadCache::Result>(
		        LIZARDFS_STATUS_OK, LizardClient::read(ctx, ino, size, off, fi));
	} catch (const RequestException &e) {
		return std::pair<int, ReadCache::Result>(e.lizardfs_error_code,
		                                         ReadCache::Result());
	} catch (...) {
		return std::pair<int, ReadCache::Result>(LIZARDFS_ERROR_IO,
		                                         ReadCache::Result());
	}
}

std::pair<int, std::vector<uint8_t>> lizardfs_read_special_inode(Context &ctx, Inode ino,
		size_t size, off_t off, FileInfo *fi) {
	try {
		return std::pair<int, std::vector<uint8_t>>(
		        LIZARDFS_STATUS_OK,
		        LizardClient::read_special_inode(ctx, ino, size, off, fi));
	} catch (const RequestException &e) {
		return std::pair<int, std::vector<uint8_t>>(e.lizardfs_error_code,
		                                            std::vector<uint8_t>());
	} catch (...) {
		return std::pair<int, std::vector<uint8_t>>(LIZARDFS_ERROR_IO,
		                                            std::vector<uint8_t>());
	}
}

std::pair<int, ssize_t> lizardfs_write(Context &ctx, Inode ino, const char *buf, size_t size,
		off_t off, FileInfo *fi) {
	try {
		auto write_ret = LizardClient::write(ctx, ino, buf, size, off, fi);
		return {LIZARDFS_STATUS_OK, write_ret};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, 0};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, 0};
	}
}

int lizardfs_release(Inode ino, FileInfo *fi) {
	try {
		LizardClient::release(ino, fi);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_flush(Context &ctx, Inode ino, FileInfo *fi) {
	try {
		LizardClient::flush(ctx, ino, fi);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_fsync(Context &ctx, Inode ino, int datasync, FileInfo* fi) {
	try {
		LizardClient::fsync(ctx, ino, datasync, fi);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_statfs(uint64_t *totalspace, uint64_t *availspace, uint64_t *trashspace,
	             uint64_t *reservedspace, uint32_t *inodes) {
	try {
		LizardClient::statfs(totalspace, availspace, trashspace, reservedspace, inodes);
		return LIZARDFS_STATUS_OK;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

void lizardfs_fs_term() {
	try {
		LizardClient::fs_term();
	} catch (...) {
		// ignore
	}
}

bool lizardfs_isSpecialInode(Inode ino) {
	return LizardClient::isSpecialInode(ino);
}

std::pair<int, std::vector<DirEntry>> lizardfs_readdir(Context &ctx, uint64_t opendirSessionID,
		Inode ino, off_t off, size_t max_entries) {
	try {
        auto fsDirEntries = LizardClient::readdir(ctx, opendirSessionID, ino, off, max_entries);
        uint64_t nextEntryIno = (fsDirEntries.empty()) ? 0 : fsDirEntries.back().attr.st_ino;
		LizardClient::update_readdir_session(opendirSessionID, nextEntryIno);
        return {LIZARDFS_STATUS_OK, fsDirEntries};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, std::vector<DirEntry>()};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, std::vector<DirEntry>()};
	}
}

int lizardfs_readlink(Context &ctx, Inode ino, std::string &link) {
	try {
		link = LizardClient::readlink(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

std::pair<int, std::vector<NamedInodeEntry>> lizardfs_readreserved(Context &ctx,
		LizardClient::NamedInodeOffset off, LizardClient::NamedInodeOffset max_entries) {
	try {
		auto ret = LizardClient::readreserved(ctx, off, max_entries);
		return {LIZARDFS_STATUS_OK, ret};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, std::vector<NamedInodeEntry>()};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, std::vector<NamedInodeEntry>()};
	}
}

std::pair<int, std::vector<NamedInodeEntry>> lizardfs_readtrash(Context &ctx,
		LizardClient::NamedInodeOffset off, LizardClient::NamedInodeOffset max_entries) {
	try {
		auto ret = LizardClient::readtrash(ctx, off, max_entries);
		return {LIZARDFS_STATUS_OK, ret};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, std::vector<NamedInodeEntry>()};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, std::vector<NamedInodeEntry>()};
	}
}

int lizardfs_opendir(Context &ctx, Inode ino) {
	try {
		LizardClient::opendir(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_releasedir(Inode ino, uint64_t opendirSessionID) {
	try {
		LizardClient::releasedir(ino);
		LizardClient::drop_readdir_session(opendirSessionID);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_mkdir(Context &ctx, Inode parent, const char *name, mode_t mode,
		EntryParam &entry_param) {
	try {
		entry_param = LizardClient::mkdir(ctx, parent, name, mode);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_rmdir(Context &ctx, Inode parent, const char *name) {
	try {
		LizardClient::rmdir(ctx, parent, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_rename(Context &ctx, Inode parent, const char *name, Inode new_parent,
	            const char *new_name) {
	try {
		LizardClient::rename(ctx, parent, name, new_parent, new_name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_update_groups(Context &ctx) {
	try {
		LizardClient::updateGroups(ctx);
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch(...) {
		return LIZARDFS_ERROR_IO;
	}
	return LIZARDFS_STATUS_OK;
}

int lizardfs_setxattr(Context ctx, Inode ino, const char *name, const char *value,
		size_t size, int flags) {
	try {
		LizardClient::setxattr(ctx, ino, name, value, size, flags, 0);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

std::pair<int,std::vector<ChunkWithAddressAndLabel>> lizardfs_getchunksinfo(Context &ctx,
	                          Inode ino, uint32_t chunk_index, uint32_t chunk_count) {
	try {
		auto chunks = LizardClient::getchunksinfo(ctx, ino, chunk_index, chunk_count);
		return {LIZARDFS_STATUS_OK, chunks};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, std::vector<ChunkWithAddressAndLabel>()};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, std::vector<ChunkWithAddressAndLabel>()};
	}
}

std::pair<int, std::vector<ChunkserverListEntry>> lizardfs_getchunkservers() {
	try {
		auto chunkservers = LizardClient::getchunkservers();
		return {LIZARDFS_STATUS_OK, chunkservers};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, std::vector<ChunkserverListEntry>()};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, std::vector<ChunkserverListEntry>()};
	}
}


int lizardfs_getlk(Context &ctx, Inode ino,
	           LizardClient::FileInfo *fi, lzfs_locks::FlockWrapper &lock) {
	try {
		LizardClient::getlk(ctx, ino, fi, lock);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

std::pair<int, uint32_t> lizardfs_setlk_send(Context &ctx, Inode ino,
	                            LizardClient::FileInfo *fi, lzfs_locks::FlockWrapper &lock) {
	try {
		uint32_t reqid = LizardClient::setlk_send(ctx, ino, fi, lock);
		return {LIZARDFS_STATUS_OK, reqid};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, 0};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, 0};
	}
}

int lizardfs_setlk_recv() {
	try {
		LizardClient::setlk_recv();
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_setlk_interrupt(const lzfs_locks::InterruptData &data) {
	try {
		LizardClient::setlk_interrupt(data);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_getxattr(Context ctx, Inode ino, const char *name,
	              size_t size, LizardClient::XattrReply &xattr_reply) {
	try {
		xattr_reply = LizardClient::getxattr(ctx, ino, name, size, 0);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}
int lizardfs_listxattr(Context ctx, Inode ino, size_t size,
	               LizardClient::XattrReply &xattr_reply) {
	try {
		xattr_reply = LizardClient::listxattr(ctx, ino, size);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_removexattr(Context ctx, Inode ino, const char *name) {
	try {
		LizardClient::removexattr(ctx, ino, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}
