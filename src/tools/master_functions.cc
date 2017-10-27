/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2017 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "common/datapack.h"
#include "common/mfserr.h"
#include "common/serialization.h"
#include "common/special_inode_defs.h"
#include "common/sockets.h"
#include "tools/tools_common_functions.h"

struct master_info_t {
	uint32_t ip;
	uint16_t port;
	uint32_t cuid;
	uint32_t version;
};

static thread_local int gCurrentMaster = -1;

static int master_register(int rfd, uint32_t cuid) {
	uint32_t i;
	const uint8_t *rptr;
	uint8_t *wptr, regbuff[8 + 73];

	wptr = regbuff;
	put32bit(&wptr, CLTOMA_FUSE_REGISTER);
	put32bit(&wptr, 73);
	memcpy(wptr, FUSE_REGISTER_BLOB_ACL, 64);
	wptr += 64;
	put8bit(&wptr, REGISTER_TOOLS);
	put32bit(&wptr, cuid);
	put16bit(&wptr, LIZARDFS_PACKAGE_VERSION_MAJOR);
	put8bit(&wptr, LIZARDFS_PACKAGE_VERSION_MINOR);
	put8bit(&wptr, LIZARDFS_PACKAGE_VERSION_MICRO);
	if (tcpwrite(rfd, regbuff, 8 + 73) != 8 + 73) {
		printf("register to master: send error\n");
		return -1;
	}
	if (tcpread(rfd, regbuff, 9) != 9) {
		printf("register to master: receive error\n");
		return -1;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i != MATOCL_FUSE_REGISTER) {
		printf("register to master: wrong answer (type)\n");
		return -1;
	}
	i = get32bit(&rptr);
	if (i != 1) {
		printf("register to master: wrong answer (length)\n");
		return -1;
	}
	if (*rptr) {
		printf("register to master: %s\n", lizardfs_error_string(*rptr));
		return -1;
	}
	return 0;
}

static int master_connect(const master_info_t *info) {
	for(int cnt = 0; cnt < 10; ++cnt) {
		int sd = tcpsocket();
		if (sd < 0) {
			return -1;
		}
		int timeout = (cnt % 2) ? (300 * (1 << (cnt >> 1))) : (200 * (1 << (cnt >> 1)));
		if (tcpnumtoconnect(sd, info->ip, info->port, timeout) >= 0) {
			return sd;
		}
		tcpclose(sd);
	}
	return -1;
}

static int read_master_info(const char *name, master_info_t *info) {
	static constexpr int kMasterInfoSize = 14;
	uint8_t buffer[kMasterInfoSize];
	struct stat stb;
	int sd;

	if (stat(name, &stb) < 0) {
		return -1;
	}

	if (stb.st_ino != SPECIAL_INODE_MASTERINFO || stb.st_nlink != 1 || stb.st_uid != 0 ||
	    stb.st_gid != 0 || stb.st_size != kMasterInfoSize) {
		return -1;
	}

	sd = open(name, O_RDONLY);
	if (sd < 0) {
		return -2;
	}

	if (read(sd, buffer, kMasterInfoSize) != kMasterInfoSize) {
		close(sd);
		return -2;
	}

	close(sd);

	deserialize(buffer, kMasterInfoSize, info->ip, info->port, info->cuid, info->version);

	return 0;
}

int open_master_conn(const char *name, uint32_t *inode, mode_t *mode, bool needrwfs) {
	char rpath[PATH_MAX + 1];
	struct stat stb;
	struct statvfs stvfsb;
	master_info_t master_info;

	rpath[0] = 0;
	if (realpath(name, rpath) == NULL) {
		printf("%s: realpath error on (%s): %s\n", name, rpath, strerr(errno));
		return -1;
	}
	if (needrwfs) {
		if (statvfs(rpath, &stvfsb) != 0) {
			printf("%s: (%s) statvfs error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
		if (stvfsb.f_flag & ST_RDONLY) {
			printf("%s: (%s) Read-only file system\n", name, rpath);
			return -1;
		}
	}
	if (stat(rpath, &stb) != 0) {
		printf("%s: (%s) stat error: %s\n", name, rpath, strerr(errno));
		return -1;
	}
	*inode = stb.st_ino;
	if (mode) {
		*mode = stb.st_mode;
	}
	if (gCurrentMaster >= 0) {
		close(gCurrentMaster);
		gCurrentMaster = -1;
	}

	for (;;) {
		uint32_t rpath_inode;

		if (stat(rpath, &stb) != 0) {
			printf("%s: (%s) stat error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
		rpath_inode = stb.st_ino;

		size_t rpath_len = strlen(rpath);
		if (rpath_len + sizeof("/" SPECIAL_FILE_NAME_MASTERINFO) > PATH_MAX) {
			printf("%s: path too long\n", name);
			return -1;
		}
		strcpy(rpath + rpath_len, "/" SPECIAL_FILE_NAME_MASTERINFO);

		int r = read_master_info(rpath, &master_info);
		if (r == -2) {
			printf("%s: can't read '" SPECIAL_FILE_NAME_MASTERINFO "'\n", name);
			return -1;
		}

		if (r == 0) {
			if (master_info.ip == 0 || master_info.port == 0 || master_info.cuid == 0) {
				printf("%s: incorrect '" SPECIAL_FILE_NAME_MASTERINFO "'\n", name);
				return -1;
			}

			if (rpath_inode == *inode) {
				*inode = SPECIAL_INODE_ROOT;
			}

			int sd = master_connect(&master_info);
			if (sd < 0) {
				printf("%s: can't connect to master (" SPECIAL_FILE_NAME_MASTERINFO "): %s\n", name,
				       strerr(errno));
				return -1;
			}

			if (master_register(sd, master_info.cuid) < 0) {
				printf("%s: can't register to master (" SPECIAL_FILE_NAME_MASTERINFO ")\n", name);
				tcpclose(sd);
				return -1;
			}

			gCurrentMaster = sd;
			return sd;
		}

		// remove .masterinfo from end of string
		rpath[rpath_len] = 0;

		if (rpath[0] != '/' || rpath[1] == '\0') {
			printf("%s: not LizardFS object\n", name);
			return -1;
		}
		dirname_inplace(rpath);
		if (stat(rpath, &stb) != 0) {
			printf("%s: (%s) stat error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
	}
	return -1;
}

void close_master_conn(int err) {
	if (gCurrentMaster < 0) {
		return;
	}
	if (err) {
		close(gCurrentMaster);
		gCurrentMaster = -1;
	}
}

void force_master_conn_close() {
	if (gCurrentMaster < 0) {
		return;
	}
	close(gCurrentMaster);
	gCurrentMaster = -1;
}
