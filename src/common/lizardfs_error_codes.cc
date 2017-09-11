/*
   Copyright 2017 Skytechnology sp. z o.o..

   This file is part of LizardFS.

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

#include "lizardfs_error_codes.h"

const char *lizardfs_error_string(uint8_t status) {
	static const char * const error_strings[LIZARDFS_ERROR_MAX + 1] = {
		"OK",
		"Operation not permitted",
		"Not a directory",
		"No such file or directory",
		"Permission denied",
		"File exists",
		"Invalid argument",
		"Directory not empty",
		"Chunk lost",
		"Out of memory",
		"Index too big",
		"Chunk locked",
		"No chunk servers",
		"No such chunk",
		"Chunk is busy",
		"Incorrect register BLOB",
		"Requested operation not completed",
		"Group info is not registered in master server",
		"Write not started",
		"Wrong chunk version",
		"Chunk already exists",
		"No space left",
		"IO error",
		"Incorrect block number",
		"Incorrect size",
		"Incorrect offset",
		"Can't connect",
		"Incorrect chunk id",
		"Disconnected",
		"CRC error",
		"Operation delayed",
		"Can't create path",
		"Data mismatch",
		"Read-only file system",
		"Quota exceeded",
		"Bad session id",
		"Password is needed",
		"Incorrect password",
		"Attribute not found",
		"Operation not supported",
		"Result too large",
		"Timeout",
		"Metadata checksum not matching",
		"Changelog inconsistent",
		"Parsing unsuccessful",
		"Metadata version mismatch",
		"No such lock",
		"Wrong lock id",
		"Operation not possible",
		"Operation temporarily not possible",
		"Waiting for operation completion",
		"Unknown LizardFS error",
		"Name too long",
		"File too large",
		"Bad file number",
		"No data available",
		"Argument list too long",
		"Unknown LizardFS error"
	};

	status = (status <= LIZARDFS_ERROR_MAX) ? status : (uint8_t)LIZARDFS_ERROR_MAX;
	return error_strings[status];
}
