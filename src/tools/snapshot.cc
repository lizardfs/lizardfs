/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2016 Skytechnology sp. z o.o..

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
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "common/datapack.h"
#include "common/server_connection.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static void snapshot_usage() {
	fprintf(stderr,
	        "make snapshot (lazy copy)\n\nusage:\n lizardfs makesnapshot [-ofl] src [src ...] dst\n");
	fprintf(stderr, " -o,-f - allow to overwrite existing objects\n");
	fprintf(stderr, " -l - wait until snapshot will finish (otherwise there is 60s timeout)\n");
}

static int make_snapshot(const char *dstdir, const char *dstbase, const char *srcname,
						 uint32_t srcinode, uint8_t canoverwrite, int long_wait) {
	uint8_t reqbuff[8 + 22 + 255], *wptr, *buff;
	const uint8_t *rptr;
	uint32_t cmd, leng, dstinode, uid, gid;
	uint32_t nleng;
	int fd;
	nleng = strlen(dstbase);
	if (nleng > 255) {
		printf("%s: name too long\n", dstbase);
		return -1;
	}
	fd = open_master_conn(dstdir, &dstinode, NULL, 0, 1);
	if (fd < 0) {
		return -1;
	}
	uid = getuid();
	gid = getgid();
	wptr = reqbuff;
	put32bit(&wptr, CLTOMA_FUSE_SNAPSHOT);
	put32bit(&wptr, 22 + nleng);
	put32bit(&wptr, 0);
	put32bit(&wptr, srcinode);
	put32bit(&wptr, dstinode);
	put8bit(&wptr, nleng);
	memcpy(wptr, dstbase, nleng);
	wptr += nleng;
	put32bit(&wptr, uid);
	put32bit(&wptr, gid);
	put8bit(&wptr, canoverwrite);
	if (tcpwrite(fd, reqbuff, 30 + nleng) != (int32_t)(30 + nleng)) {
		printf("%s->%s/%s: master query: send error\n", srcname, dstdir, dstbase);
		close_master_conn(1);
		return -1;
	}
	if (tcptoread(fd, reqbuff, 8, long_wait ? -1 : 60000) != 8) {
		printf("%s->%s/%s: master query: receive error\n", srcname, dstdir, dstbase);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd != MATOCL_FUSE_SNAPSHOT) {
		printf("%s->%s/%s: master query: wrong answer (type)\n", srcname, dstdir, dstbase);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t *)malloc(leng);
	// snapshot can take long time to finish so we increase timeout
	if (tcptoread(fd, buff, leng, 60 * 1000) != (int32_t)leng) {
		printf("%s->%s/%s: master query: receive error\n", srcname, dstdir, dstbase);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	rptr = buff;
	cmd = get32bit(&rptr);  // queryid
	if (cmd != 0) {
		printf("%s->%s/%s: master query: wrong answer (queryid)\n", srcname, dstdir, dstbase);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	leng -= 4;
	if (leng != 1) {
		printf("%s->%s/%s: master query: wrong answer (leng)\n", srcname, dstdir, dstbase);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	if (*rptr != 0) {
		printf("%s->%s/%s: %s\n", srcname, dstdir, dstbase, mfsstrerr(*rptr));
		free(buff);
		return -1;
	}
	return 0;
}

static int snapshot(const char *dstname, char *const *srcnames, uint32_t srcelements,
					uint8_t canowerwrite, int long_wait) {
	char to[PATH_MAX + 1], base[PATH_MAX + 1], dir[PATH_MAX + 1];
	char src[PATH_MAX + 1];
	struct stat sst, dst;
	int status;
	uint32_t i, l;

	if (stat(dstname, &dst) < 0) {  // dst does not exist
		if (errno != ENOENT) {
			printf("%s: stat error: %s\n", dstname, strerr(errno));
			return -1;
		}
		if (srcelements > 1) {
			printf("can snapshot multiple elements only into existing directory\n");
			return -1;
		}
		if (lstat(srcnames[0], &sst) < 0) {
			printf("%s: lstat error: %s\n", srcnames[0], strerr(errno));
			return -1;
		}
		if (bsd_dirname(dstname, dir) < 0) {
			printf("%s: dirname error\n", dstname);
			return -1;
		}
		if (stat(dir, &dst) < 0) {
			printf("%s: stat error: %s\n", dir, strerr(errno));
			return -1;
		}
		if (sst.st_dev != dst.st_dev) {
			printf("(%s,%s): both elements must be on the same device\n", dstname, srcnames[0]);
			return -1;
		}
		if (realpath(dir, to) == NULL) {
			printf("%s: realpath error on %s: %s\n", dir, to, strerr(errno));
			return -1;
		}
		if (bsd_basename(dstname, base) < 0) {
			printf("%s: basename error\n", dstname);
			return -1;
		}
		if (strlen(dstname) > 0 && dstname[strlen(dstname) - 1] == '/' && !S_ISDIR(sst.st_mode)) {
			printf("directory %s does not exist\n", dstname);
			return -1;
		}
		return make_snapshot(to, base, srcnames[0], sst.st_ino, canowerwrite, long_wait);
	} else {  // dst exists
		if (realpath(dstname, to) == NULL) {
			printf("%s: realpath error on %s: %s\n", dstname, to, strerr(errno));
			return -1;
		}
		if (!S_ISDIR(dst.st_mode)) {  // dst id not a directory
			if (srcelements > 1) {
				printf("can snapshot multiple elements only into existing directory\n");
				return -1;
			}
			if (lstat(srcnames[0], &sst) < 0) {
				printf("%s: lstat error: %s\n", srcnames[0], strerr(errno));
				return -1;
			}
			if (sst.st_dev != dst.st_dev) {
				printf("(%s,%s): both elements must be on the same device\n", dstname, srcnames[0]);
				return -1;
			}
			memcpy(dir, to, PATH_MAX + 1);
			dirname_inplace(dir);
			if (bsd_basename(to, base) < 0) {
				printf("%s: basename error\n", to);
				return -1;
			}
			return make_snapshot(dir, base, srcnames[0], sst.st_ino, canowerwrite, long_wait);
		} else {  // dst is a directory
			status = 0;
			for (i = 0; i < srcelements; i++) {
				if (lstat(srcnames[i], &sst) < 0) {
					printf("%s: lstat error: %s\n", srcnames[i], strerr(errno));
					status = -1;
					continue;
				}
				if (sst.st_dev != dst.st_dev) {
					printf("(%s,%s): both elements must be on the same device\n", dstname,
					       srcnames[i]);
					status = -1;
					continue;
				}
				if (!S_ISDIR(sst.st_mode)) {      // src is not a directory
					if (!S_ISLNK(sst.st_mode)) {  // src is not a symbolic link
						if (realpath(srcnames[i], src) == NULL) {
							printf("%s: realpath error on %s: %s\n", srcnames[i], src,
							       strerr(errno));
							status = -1;
							continue;
						}
						if (bsd_basename(src, base) < 0) {
							printf("%s: basename error\n", src);
							status = -1;
							continue;
						}
					} else {  // src is a symbolic link
						if (bsd_basename(srcnames[i], base) < 0) {
							printf("%s: basename error\n", srcnames[i]);
							status = -1;
							continue;
						}
					}
					if (make_snapshot(to, base, srcnames[i], sst.st_ino, canowerwrite, long_wait) <
					    0) {
						status = -1;
					}
				} else {  // src is a directory
					l = strlen(srcnames[i]);
					if (l > 0 &&
					    srcnames[i][l - 1] !=
					        '/') {  // src is a directory and name has trailing slash
						if (realpath(srcnames[i], src) == NULL) {
							printf("%s: realpath error on %s: %s\n", srcnames[i], src,
							       strerr(errno));
							status = -1;
							continue;
						}
						if (bsd_basename(src, base) < 0) {
							printf("%s: basename error\n", src);
							status = -1;
							continue;
						}
						if (make_snapshot(to, base, srcnames[i], sst.st_ino, canowerwrite,
						                  long_wait) < 0) {
							status = -1;
						}
					} else {  // src is a directory and name has not trailing slash
						memcpy(dir, to, PATH_MAX + 1);
						dirname_inplace(dir);
						if (bsd_basename(to, base) < 0) {
							printf("%s: basename error\n", to);
							status = -1;
							continue;
						}
						if (make_snapshot(dir, base, srcnames[i], sst.st_ino, canowerwrite,
						                  long_wait) < 0) {
							status = -1;
						}
					}
				}
			}
			return status;
		}
	}
}

int snapshot_run(int argc, char **argv) {
	int ch;
	int oflag = 0;
	int lflag = 0;

	while ((ch = getopt(argc, argv, "fo")) != -1) {
		switch (ch) {
		case 'f':
		case 'o':
			oflag = 1;
			break;
		case 'l':
			lflag = 1;
			break;
		}
	}
	argc -= optind;
	argv += optind;
	if (argc < 2) {
		snapshot_usage();
		return 1;
	}
	return snapshot(argv[argc - 1], argv, argc - 1, oflag, lflag);
}
