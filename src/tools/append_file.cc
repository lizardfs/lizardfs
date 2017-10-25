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

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "common/datapack.h"
#include "common/mfserr.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static void append_file_usage() {
	fprintf(
	    stderr,
	    "append file chunks to another file. If destination file doesn't exist then it's created"
	    " as empty file and then chunks are appended\n\nusage:\n lizardfs appendchunks dstfile name [name "
	    "...]\n");
}

static int append_file(const char *fname, const char *afname) {
	uint8_t reqbuff[28], *wptr, *buff;
	const uint8_t *rptr;
	uint32_t cmd, leng, inode, ainode, uid, gid;
	mode_t dmode, smode;
	int fd;
	fd = open_master_conn(fname, &inode, &dmode, true);
	if (fd < 0) {
		return -1;
	}
	if (open_master_conn(afname, &ainode, &smode, true) < 0) {
		return -1;
	}

	if ((smode & S_IFMT) != S_IFREG) {
		printf("%s: not a file\n", afname);
		return -1;
	}
	if ((dmode & S_IFMT) != S_IFREG) {
		printf("%s: not a file\n", fname);
		return -1;
	}
	uid = getuid();
	gid = getgid();
	wptr = reqbuff;
	put32bit(&wptr, CLTOMA_FUSE_APPEND);
	put32bit(&wptr, 20);
	put32bit(&wptr, 0);
	put32bit(&wptr, inode);
	put32bit(&wptr, ainode);
	put32bit(&wptr, uid);
	put32bit(&wptr, gid);
	if (tcpwrite(fd, reqbuff, 28) != 28) {
		printf("%s: master query: send error\n", fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd, reqbuff, 8) != 8) {
		printf("%s: master query: receive error\n", fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd != MATOCL_FUSE_APPEND) {
		printf("%s: master query: wrong answer (type)\n", fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t *)malloc(leng);
	if (tcpread(fd, buff, leng) != (int32_t)leng) {
		printf("%s: master query: receive error\n", fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);  // queryid
	if (cmd != 0) {
		printf("%s: master query: wrong answer (queryid)\n", fname);
		free(buff);
		return -1;
	}
	leng -= 4;
	if (leng != 1) {
		printf("%s: master query: wrong answer (leng)\n", fname);
		free(buff);
		return -1;
	} else if (*rptr != LIZARDFS_STATUS_OK) {
		printf("%s: %s\n", fname, lizardfs_error_string(*rptr));
		free(buff);
		return -1;
	}
	free(buff);
	return 0;
}

int append_file_run(int argc, char **argv) {
	char *appendfname = nullptr;
	int i, status;

	while (getopt(argc, argv, "") != -1) {
	}
	argc -= optind;
	argv += optind;

	if (argc <= 1) {
		append_file_usage();
		return 1;
	}
	appendfname = argv[0];
	i = open(appendfname, O_RDWR | O_CREAT, 0666);
	if (i < 0) {
		fprintf(stderr, "can't create/open file: %s\n", appendfname);
		return 1;
	}
	close(i);
	argc--;
	argv++;

	if (argc < 1) {
		append_file_usage();
		return 1;
	}

	status = 0;
	while (argc > 0) {
		if (append_file(appendfname, *argv) < 0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}
