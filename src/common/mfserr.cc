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

#include "common/platform.h"
#include "common/mfserr.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <inttypes.h>

#include <array>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/errno_defs.h"

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

int lizardfs_error_conv(uint8_t status) {
	switch (status) {
		case LIZARDFS_STATUS_OK:
			return 0;
		case LIZARDFS_ERROR_EPERM:
			return EPERM;
		case LIZARDFS_ERROR_ENOTDIR:
			return ENOTDIR;
		case LIZARDFS_ERROR_ENOENT:
			return ENOENT;
		case LIZARDFS_ERROR_EACCES:
			return EACCES;
		case LIZARDFS_ERROR_EEXIST:
			return EEXIST;
		case LIZARDFS_ERROR_EINVAL:
			return EINVAL;
		case LIZARDFS_ERROR_ENOTEMPTY:
			return ENOTEMPTY;
		case LIZARDFS_ERROR_IO:
			return EIO;
		case LIZARDFS_ERROR_EROFS:
			return EROFS;
		case LIZARDFS_ERROR_QUOTA:
			return EDQUOT;
		case LIZARDFS_ERROR_ENOATTR:
			return ENOATTR;
		case LIZARDFS_ERROR_ENOTSUP:
			return ENOTSUP;
		case LIZARDFS_ERROR_ERANGE:
			return ERANGE;
		case LIZARDFS_ERROR_ENAMETOOLONG:
			return ENAMETOOLONG;
		case LIZARDFS_ERROR_EFBIG:
			return EFBIG;
		case LIZARDFS_ERROR_EBADF:
			return EBADF;
		case LIZARDFS_ERROR_ENODATA:
			return ENODATA;
		case LIZARDFS_ERROR_OUTOFMEMORY:
			return ENOMEM;
		default:
			return EINVAL;
	}
}

const char *strerr(int error_code) {
	static std::unordered_map<int, std::string> error_description;
	static std::mutex error_description_mutex;

	std::lock_guard<std::mutex> guard(error_description_mutex);
	auto it = error_description.find(error_code);
	if (it != error_description.end()) {
		return it->second.c_str();
	}

	const char *error_string = strerror(error_code);
	auto insert_it = error_description.insert({error_code, std::string(error_string)}).first;
	return insert_it->second.c_str();
}
