/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2016 Skytechnology sp. z o.o.

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

#include "common/attributes.h"
#include "mount/lizard_client.h"

enum {
	OP_STATFS = 0,
	OP_ACCESS,
	OP_LOOKUP,
	OP_LOOKUP_INTERNAL,
	OP_DIRCACHE_LOOKUP,
	OP_GETATTR,
	OP_DIRCACHE_GETATTR,
	OP_SETATTR,
	OP_MKNOD,
	OP_UNLINK,
	OP_MKDIR,
	OP_RMDIR,
	OP_SYMLINK,
	OP_READLINK,
	OP_READLINK_CACHED,
	OP_RENAME,
	OP_LINK,
	OP_OPENDIR,
	OP_READDIR,
	OP_RELEASEDIR,
	OP_CREATE,
	OP_OPEN,
	OP_RELEASE,
	OP_READ,
	OP_WRITE,
	OP_FLUSH,
	OP_FSYNC,
	OP_SETXATTR,
	OP_GETXATTR,
	OP_LISTXATTR,
	OP_REMOVEXATTR,
	OP_GETDIR_FULL,
	OP_GETDIR_SMALL,
	OP_GETLK,
	OP_SETLK,
	OP_FLOCK,
	STATNODES
};

struct MagicFile {
	MagicFile() : wasRead(false), wasWritten(false) {}

	std::mutex mutex;
	std::string value;
	bool wasRead;
	bool wasWritten;
};

/**
 * A wrapper around pthread_mutex, acquiring a lock during construction and releasing it during
 * destruction in case if the lock wasn't released beforehand.
 */
struct PthreadMutexWrapper {
	PthreadMutexWrapper(pthread_mutex_t& mutex) : mutex_(mutex), locked_(true) {
		pthread_mutex_lock(&mutex_);
	}

	~PthreadMutexWrapper() {
		if (locked_) {
			unlock();
		}
	}

	void lock() {
		sassert(!locked_);
		locked_ = true;
		pthread_mutex_lock(&mutex_);
	}
	void unlock() {
		sassert(locked_);
		locked_ = false;
		pthread_mutex_unlock(&mutex_);
	}

private:
	pthread_mutex_t& mutex_;
	bool locked_;
};

namespace LizardClient {
void stats_inc(uint8_t id);

void attr_to_stat(uint32_t inode, const Attributes &attr, struct stat *stbuf);

void makeattrstr(char *buff, uint32_t size, struct stat *stbuf);
} // LizardClient
