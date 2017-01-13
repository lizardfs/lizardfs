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

int lizardfs_lookup(Context ctx, Inode parent, const char *name, EntryParam &param) {
	try {
		param = LizardClient::lookup(ctx, parent, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_mknod(Context ctx, Inode parent, const char *name, mode_t mode, dev_t rdev,
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

int lizardfs_open(Context ctx, Inode ino, FileInfo *fi) {
	try {
		LizardClient::open(ctx, ino, fi);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_getattr(Context ctx, Inode ino, AttrReply &reply) {
	try {
		reply = LizardClient::getattr(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

std::pair<int, ReadCache::Result> lizardfs_read(Context ctx, Inode ino, size_t size, off_t off,
		FileInfo *fi) {
	try {
		return std::pair<int, ReadCache::Result>(
		        LIZARDFS_STATUS_OK, LizardClient::read(ctx, ino, size, off, fi));
	} catch (const RequestException &e) {
		return std::pair<int, ReadCache::Result>(e.lizardfs_error_code,
		                                         ReadCache::Result());
	}
}

std::pair<int, std::vector<uint8_t>> lizardfs_read_special_inode(Context ctx, Inode ino,
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

std::pair<int, ssize_t> lizardfs_write(Context ctx, Inode ino, const char *buf, size_t size,
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

int lizardfs_release(Context ctx, Inode ino, FileInfo *fi) {
	try {
		LizardClient::release(ctx, ino, fi);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_flush(Context ctx, Inode ino, FileInfo *fi) {
	try {
		LizardClient::flush(ctx, ino, fi);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
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

std::pair<int, std::vector<DirEntry>> lizardfs_readdir(Context ctx, Inode ino, off_t off,
		size_t max_entries) {
	try {
		auto ret = LizardClient::readdir(ctx, ino, off, max_entries);
		return {LIZARDFS_STATUS_OK, ret};
	} catch (const RequestException &e) {
		return {e.lizardfs_error_code, std::vector<DirEntry>()};
	} catch (...) {
		return {LIZARDFS_ERROR_IO, std::vector<DirEntry>()};
	}
}

int lizardfs_opendir(Context ctx, Inode ino) {
	try {
		LizardClient::opendir(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_releasedir(Context ctx, Inode ino) {
	try {
		LizardClient::releasedir(ctx, ino);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_mkdir(Context ctx, Inode parent, const char *name, mode_t mode,
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

int lizardfs_rmdir(Context ctx, Inode parent, const char *name) {
	try {
		LizardClient::rmdir(ctx, parent, name);
		return LIZARDFS_STATUS_OK;
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch (...) {
		return LIZARDFS_ERROR_IO;
	}
}

int lizardfs_update_groups(LizardClient::Context &ctx) {
	try {
		LizardClient::updateGroups(ctx);
	} catch (const RequestException &e) {
		return e.lizardfs_error_code;
	} catch(...) {
		return LIZARDFS_ERROR_IO;
	}
	return LIZARDFS_STATUS_OK;
}
