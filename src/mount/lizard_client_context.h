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

#include <cstdint>
#include <sys/types.h>

#ifdef _WIN32
typedef uint32_t uid_t;
typedef uint32_t gid_t;
#endif

namespace LizardClient {

/**
 * Class containing arguments that are passed with every request to the filesystem
 */
struct Context {
	Context(uid_t uid, gid_t gid, pid_t pid, mode_t umask)
			: uid(uid), gid(gid), pid(pid), umask(umask) {
	}

	uid_t uid;
	gid_t gid;
	pid_t pid;
	mode_t umask;
};

} // namespace LizardClient
