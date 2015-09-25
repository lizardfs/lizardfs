/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include "common/serialization_macros.h"

namespace lzfs_locks {
enum {
	kInvalid   = 0,
	kUnlock    = 1,
	kShared    = 2,
	kExclusive = 4,
	kInterrupt = 8,
	kNonblock  = 16,
	kRelease   = 32
};

LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(Type, kFlock, kPosix, kAll)

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(Info,
	uint32_t, version,
	uint32_t, inode,
	uint64_t, owner,
	uint32_t, sessionid,
	uint16_t, type,
	uint64_t, start,
	uint64_t, end
);

struct InterruptData {
	InterruptData() : owner(), ino(), reqid() {}
	InterruptData(uint64_t o, uint32_t i, uint32_t r)
		: owner(o), ino(i), reqid(r) {}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(owner, ino, reqid);

	uint64_t owner;
	uint32_t ino;
	uint32_t reqid;
};

/*! \brief Structure representing basic fields of 'struct flock' from Linux.
 *
 * Field l_whence from struct flock is ignored because it is always equal to
 * SEEK_SET (fuse always converts from other values to SEEK_SET).
 */
struct FlockWrapper {
	FlockWrapper() : l_type(0), l_start(0), l_len(0), l_pid(0) {};

	FlockWrapper(int16_t l_type, int64_t l_start, int64_t l_len, int32_t l_pid) :
		l_type(l_type),
		l_start(l_start),
		l_len(l_len),
		l_pid(l_pid) {
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(l_type, l_start, l_len, l_pid);

	int16_t l_type;   // Type of lock: F_RDLCK, F_WRLCK, or F_UNLCK.
	int64_t l_start;  // Offset where the lock begins.
	int64_t l_len;    // Size of the locked area; zero means until EOF.
	int32_t l_pid;    // Process holding the lock.
};

} // namespace lzfs_locks
