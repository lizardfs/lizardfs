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
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "common/datapack.h"
#include "common/mfserr.h"
#include "common/special_inode_defs.h"
#include "tools/tools_common_functions.h"

int master_register_old(int rfd) {
	uint32_t i;
	const uint8_t *rptr;
	uint8_t *wptr, regbuff[8 + 72];

	wptr = regbuff;
	put32bit(&wptr, CLTOMA_FUSE_REGISTER);
	put32bit(&wptr, 68);
	memcpy(wptr, FUSE_REGISTER_BLOB_TOOLS_NOACL, 64);
	wptr += 64;
	put16bit(&wptr, LIZARDFS_PACKAGE_VERSION_MAJOR);
	put8bit(&wptr, LIZARDFS_PACKAGE_VERSION_MINOR);
	put8bit(&wptr, LIZARDFS_PACKAGE_VERSION_MICRO);
	if (tcpwrite(rfd, regbuff, 8 + 68) != 8 + 68) {
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

int master_register(int rfd, uint32_t cuid) {
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

#ifdef LIZARDFS_HAVE_THREAD_LOCAL
static thread_local dev_t current_device = 0;
static thread_local int current_master = -1;
static thread_local uint32_t masterversion = 0;
#else
static __thread dev_t current_device = 0;
static __thread int current_master = -1;
static __thread uint32_t masterversion = 0;
#endif

int open_master_conn(const char *name, uint32_t *inode, mode_t *mode, uint8_t needsamedev,
					 uint8_t needrwfs) {
	char rpath[PATH_MAX + 1];
	struct stat stb;
	struct statvfs stvfsb;
	int sd;
	uint8_t masterinfo[14];
	const uint8_t *miptr;
	uint8_t cnt;
	uint32_t masterip;
	uint16_t masterport;
	uint32_t mastercuid;

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
	if (lstat(rpath, &stb) != 0) {
		printf("%s: (%s) lstat error: %s\n", name, rpath, strerr(errno));
		return -1;
	}
	*inode = stb.st_ino;
	if (mode) {
		*mode = stb.st_mode;
	}
	if (current_master >= 0) {
		if (current_device == stb.st_dev) {
			return current_master;
		}
		if (needsamedev) {
			printf("%s: different device\n", name);
			return -1;
		}
	}
	if (current_master >= 0) {
		close(current_master);
		current_master = -1;
	}
	current_device = stb.st_dev;
	for (;;) {
		if (stb.st_ino == 1) {  // found fuse root
			// first try to locate ".masterinfo"
			if (strlen(rpath) + 12 < PATH_MAX) {
				strcat(rpath, "/" SPECIAL_FILE_NAME_MASTERINFO);
				if (lstat(rpath, &stb) == 0) {
					if (stb.st_ino == SPECIAL_INODE_MASTERINFO && stb.st_nlink == 1 &&
					    stb.st_uid == 0 && stb.st_gid == 0 &&
					    (stb.st_size == 10 || stb.st_size == 14)) {
						sd = open(rpath, O_RDONLY);
						if (stb.st_size == 10) {
							if (read(sd, masterinfo, 10) != 10) {
								printf("%s: can't read '" SPECIAL_FILE_NAME_MASTERINFO "'\n", name);
								close(sd);
								return -1;
							}
						} else if (stb.st_size == 14) {
							if (read(sd, masterinfo, 14) != 14) {
								printf("%s: can't read '" SPECIAL_FILE_NAME_MASTERINFO "'\n", name);
								close(sd);
								return -1;
							}
						}
						close(sd);
						miptr = masterinfo;
						masterip = get32bit(&miptr);
						masterport = get16bit(&miptr);
						mastercuid = get32bit(&miptr);
						if (stb.st_size == 14) {
							masterversion = get32bit(&miptr);
						} else {
							masterversion = 0;
						}
						if (masterip == 0 || masterport == 0 || mastercuid == 0) {
							printf("%s: incorrect '" SPECIAL_FILE_NAME_MASTERINFO "'\n", name);
							return -1;
						}
						cnt = 0;
						while (cnt < 10) {
							sd = tcpsocket();
							if (sd < 0) {
								printf("%s: can't create connection socket: %s\n", name,
								       strerr(errno));
								return -1;
							}
							if (tcpnumtoconnect(sd, masterip, masterport,
							                    (cnt % 2) ? (300 * (1 << (cnt >> 1)))
							                              : (200 * (1 << (cnt >> 1)))) < 0) {
								cnt++;
								if (cnt == 10) {
									printf(
									    "%s: can't connect to master (" SPECIAL_FILE_NAME_MASTERINFO
									    "): %s\n",
									    name, strerr(errno));
									return -1;
								}
								tcpclose(sd);
							} else {
								cnt = 10;
							}
						}
						if (master_register(sd, mastercuid) < 0) {
							printf("%s: can't register to master (" SPECIAL_FILE_NAME_MASTERINFO
							       ")\n",
							       name);
							return -1;
						}
						current_master = sd;
						return sd;
					}
				}
				rpath[strlen(rpath) - 4] = 0;  // cut '.masterinfo' to '.master' and try to fallback
				                               // to older communication method
				if (lstat(rpath, &stb) == 0) {
					if (stb.st_ino == SPECIAL_INODE_MASTERINFO && stb.st_nlink == 1 &&
					    stb.st_uid == 0 && stb.st_gid == 0) {
						fprintf(stderr,
						        "old version of mfsmount detected - using old and deprecated "
						        "version of protocol - "
						        "please upgrade your mfsmount\n");
						sd = open(rpath, O_RDWR);
						if (master_register_old(sd) < 0) {
							printf("%s: can't register to master (.master / old protocol)\n", name);
							return -1;
						}
						current_master = sd;
						return sd;
					}
				}
				printf("%s: not LizardFS object\n", name);
				return -1;
			} else {
				printf("%s: path too long\n", name);
				return -1;
			}
		}
		if (rpath[0] != '/' || rpath[1] == '\0') {
			printf("%s: not LizardFS object\n", name);
			return -1;
		}
		dirname_inplace(rpath);
		if (lstat(rpath, &stb) != 0) {
			printf("%s: (%s) lstat error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
	}
	return -1;
}

void close_master_conn(int err) {
	if (current_master < 0) {
		return;
	}
	if (err) {
		close(current_master);
		current_master = -1;
		current_device = 0;
	}
}

void force_master_conn_close() {
	if (current_master < 0) {
		return;
	}
	close(current_master);
	current_master = -1;
	current_device = 0;
}
