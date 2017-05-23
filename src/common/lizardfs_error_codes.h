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

#ifndef __LIZARDFS_ERROR_CODES_H
#define __LIZARDFS_ERROR_CODES_H

enum lizardfs_error_code {
	LIZARDFS_STATUS_OK                     =  0,    // OK
	LIZARDFS_ERROR_EPERM                   =  1,    // Operation not permitted
	LIZARDFS_ERROR_ENOTDIR                 =  2,    // Not a directory
	LIZARDFS_ERROR_ENOENT                  =  3,    // No such file or directory
	LIZARDFS_ERROR_EACCES                  =  4,    // Permission denied
	LIZARDFS_ERROR_EEXIST                  =  5,    // File exists
	LIZARDFS_ERROR_EINVAL                  =  6,    // Invalid argument
	LIZARDFS_ERROR_ENOTEMPTY               =  7,    // Directory not empty
	LIZARDFS_ERROR_CHUNKLOST               =  8,    // Chunk lost
	LIZARDFS_ERROR_OUTOFMEMORY             =  9,    // Out of memory
	LIZARDFS_ERROR_INDEXTOOBIG             = 10,    // Index too big
	LIZARDFS_ERROR_LOCKED                  = 11,    // Chunk locked
	LIZARDFS_ERROR_NOCHUNKSERVERS          = 12,    // No chunk servers
	LIZARDFS_ERROR_NOCHUNK                 = 13,    // No such chunk
	LIZARDFS_ERROR_CHUNKBUSY               = 14,    // Chunk is busy
	LIZARDFS_ERROR_REGISTER                = 15,    // Incorrect register BLOB
	LIZARDFS_ERROR_NOTDONE                 = 16,    // Requested operation not completed
	LIZARDFS_ERROR_GROUPNOTREGISTERED      = 17,    // Group info is not registered in master server
	LIZARDFS_ERROR_NOTSTARTED              = 18,    // Write not started
	LIZARDFS_ERROR_WRONGVERSION            = 19,    // Wrong chunk version
	LIZARDFS_ERROR_CHUNKEXIST              = 20,    // Chunk already exists
	LIZARDFS_ERROR_NOSPACE                 = 21,    // No space left
	LIZARDFS_ERROR_IO                      = 22,    // IO error
	LIZARDFS_ERROR_BNUMTOOBIG              = 23,    // Incorrect block number
	LIZARDFS_ERROR_WRONGSIZE               = 24,    // Incorrect size
	LIZARDFS_ERROR_WRONGOFFSET             = 25,    // Incorrect offset
	LIZARDFS_ERROR_CANTCONNECT             = 26,    // Can't connect
	LIZARDFS_ERROR_WRONGCHUNKID            = 27,    // Incorrect chunk id
	LIZARDFS_ERROR_DISCONNECTED            = 28,    // Disconnected
	LIZARDFS_ERROR_CRC                     = 29,    // CRC error
	LIZARDFS_ERROR_DELAYED                 = 30,    // Operation delayed
	LIZARDFS_ERROR_CANTCREATEPATH          = 31,    // Can't create path
	LIZARDFS_ERROR_MISMATCH                = 32,    // Data mismatch
	LIZARDFS_ERROR_EROFS                   = 33,    // Read-only file system
	LIZARDFS_ERROR_QUOTA                   = 34,    // Quota exceeded
	LIZARDFS_ERROR_BADSESSIONID            = 35,    // Bad session id
	LIZARDFS_ERROR_NOPASSWORD              = 36,    // Password is needed
	LIZARDFS_ERROR_BADPASSWORD             = 37,    // Incorrect password
	LIZARDFS_ERROR_ENOATTR                 = 38,    // Attribute not found
	LIZARDFS_ERROR_ENOTSUP                 = 39,    // Operation not supported
	LIZARDFS_ERROR_ERANGE                  = 40,    // Result too large
	LIZARDFS_ERROR_TIMEOUT                 = 41,    // Timeout
	LIZARDFS_ERROR_BADMETADATACHECKSUM     = 42,    // Metadata checksum not matching
	LIZARDFS_ERROR_CHANGELOGINCONSISTENT   = 43,    // Changelog inconsistent
	LIZARDFS_ERROR_PARSE                   = 44,    // Parsing unsuccessful
	LIZARDFS_ERROR_METADATAVERSIONMISMATCH = 45,    // Metadata version mismatch
	LIZARDFS_ERROR_NOTLOCKED               = 46,    // No such lock
	LIZARDFS_ERROR_WRONGLOCKID             = 47,    // Wrong lock id
	LIZARDFS_ERROR_NOTPOSSIBLE             = 48,    // It's not possible to perform operation in this way
	LIZARDFS_ERROR_TEMP_NOTPOSSIBLE        = 49,    // Operation temporarily not possible
	LIZARDFS_ERROR_WAITING                 = 50,    // Waiting for operation completion
	LIZARDFS_ERROR_UNKNOWN                 = 51,    // Unknown error
	LIZARDFS_ERROR_ENAMETOOLONG            = 52,    // Name too long
	LIZARDFS_ERROR_EFBIG                   = 53,    // File too large
	LIZARDFS_ERROR_EBADF                   = 54,    // Bad file number
	LIZARDFS_ERROR_ENODATA                 = 55,    // No data available
	LIZARDFS_ERROR_MAX                     = 56
};

inline const char *lizardfs_error_string(uint8_t status) {
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
		"Unknown LizardFS error"
	};

	status = (status <= LIZARDFS_ERROR_MAX) ? status : (uint8_t)LIZARDFS_ERROR_MAX;
	return error_strings[status];
}

#endif
