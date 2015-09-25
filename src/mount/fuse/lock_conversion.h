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

#include <cassert>
#include <fcntl.h>

#include "common/serialization_macros.h"
#include "protocol/lock_info.h"

namespace lzfs_locks {

/*!
 * \brief Converts flock operation flags to lzfs_locks flags.
 * \param op Integer containing flags to be converted.
 *
 * \return Returns converted flag (or kInvalid)
 */
inline uint32_t flockOpConv(int op) {
	if ((op & LOCK_UN) == LOCK_UN) {
		return kUnlock;
	} else if ((op & LOCK_EX) == LOCK_EX) {
		return (op & LOCK_NB) == LOCK_NB ? kExclusive | kNonblock : kExclusive;
	} else if ((op & LOCK_SH) == LOCK_SH) {
		return (op & LOCK_NB) == LOCK_NB ? kShared | kNonblock : kShared;
	}

	return kInvalid;
}

/*!
 * \brief Converts posix lock operation flags to lzfs_locks flags.
 * \param op Integer containing flags to be converted.
 * \param sleep Denotes if lock operation can sleep.
 *
 * \return Returns converted flag (or kInvalid)
 */
inline uint32_t posixOpConv(int op, bool sleep) {
	switch (op) {
	case F_UNLCK:
		return kUnlock;
	case F_RDLCK:
		return !sleep ? kShared | kNonblock : kShared;
	case F_WRLCK:
		return !sleep ? kExclusive | kNonblock : kExclusive;
	default:
		return kInvalid;
	}
}

/*!
 * \brief Checks if flock operation flags are valid.
 * \param op Integer containing flags to be tested.
 *
 * \return Returns true if operation is valid, false otherwise.
 */
inline bool flockOpValid(int op) {
	return ((op & LOCK_UN) == LOCK_UN) ||
	       ((op & LOCK_SH) == LOCK_SH) ||
	       ((op & LOCK_EX) == LOCK_EX) ||
	       ((op & (LOCK_SH | LOCK_NB)) == (LOCK_SH | LOCK_NB)) ||
	       ((op & (LOCK_EX | LOCK_NB)) == (LOCK_EX | LOCK_NB));
}

/*!
 * \brief Checks if posix lock operation flags are valid.
 * \param op Integer containing flags to be tested.
 *
 * \return Returns true if operation is valid, false otherwise.
 */
inline bool posixOpValid(int op) {
	return (op == F_UNLCK) || (op == F_RDLCK) || (op == F_WRLCK);
}


inline FlockWrapper convertPLock(struct flock& fl, bool sleep) {
	FlockWrapper ret;

	ret.l_type = posixOpConv(fl.l_type, sleep);
	ret.l_start = fl.l_start;
	ret.l_len = fl.l_len;
	ret.l_pid = fl.l_pid;

	return ret;
}

inline struct flock convertToFlock(FlockWrapper& fl) {
		struct ::flock ret;

		ret.l_len = fl.l_len;
		ret.l_pid = fl.l_pid;
		ret.l_start = fl.l_start;
		ret.l_whence = SEEK_SET;
		ret.l_type = -1;
		switch(fl.l_type) {
			case kShared: ret.l_type = F_RDLCK; break;
			case kExclusive: ret.l_type = F_WRLCK; break;
			case kUnlock: ret.l_type = F_UNLCK; break;
			default: assert(0);
		}
		return ret;
}

} // namespace lzfs_locks
