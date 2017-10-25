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

#include <stdio.h>
#include <stdlib.h>

#include "common/datapack.h"
#include "common/mfserr.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static void check_file_usage() {
	fprintf(stderr, "check files\n\nusage:\n lizardfs checkfile [-nhH] name [name ...]\n");
}

static int check_file(const char *fname) {
	uint8_t reqbuff[16], *wptr, *buff;
	const uint8_t *rptr;
	uint32_t cmd, leng, inode;
	uint8_t copies;
	uint32_t chunks;
	int fd;
	fd = open_master_conn(fname, &inode, nullptr, false);
	if (fd < 0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr, CLTOMA_FUSE_CHECK);
	put32bit(&wptr, 8);
	put32bit(&wptr, 0);
	put32bit(&wptr, inode);
	if (tcpwrite(fd, reqbuff, 16) != 16) {
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
	if (cmd != MATOCL_FUSE_CHECK) {
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
	if (leng == 1) {
		printf("%s: %s\n", fname, lizardfs_error_string(*rptr));
		free(buff);
		return -1;
	} else if (leng % 3 != 0 && leng != 44) {
		printf("%s: master query: wrong answer (leng)\n", fname);
		free(buff);
		return -1;
	}
	printf("%s:\n", fname);
	if (leng % 3 == 0) {
		for (cmd = 0; cmd < leng; cmd += 3) {
			copies = get8bit(&rptr);
			chunks = get16bit(&rptr);
			if (copies == 1) {
				printf("1 copy:");
			} else {
				printf("%" PRIu8 " copies:", copies);
			}
			print_number(" ", "\n", chunks, 1, 0, 1);
		}
	} else {
		for (cmd = 0; cmd < 11; cmd++) {
			chunks = get32bit(&rptr);
			if (chunks > 0) {
				if (cmd == 1) {
					printf(" chunks with 1 copy:    ");
				} else if (cmd >= 10) {
					printf(" chunks with 10+ copies:");
				} else {
					printf(" chunks with %u copies:  ", cmd);
				}
				print_number(" ", "\n", chunks, 1, 0, 1);
			}
		}
	}
	free(buff);
	return 0;
}

int check_file_run(int argc, char **argv) {
	int ch, status;

	while ((ch = getopt(argc, argv, "nhH")) != -1) {
		switch (ch) {
		case 'n':
			humode = 0;
			break;
		case 'h':
			humode = 1;
			break;
		case 'H':
			humode = 2;
			break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc < 1) {
		check_file_usage();
		return 1;
	}

	status = 0;
	while (argc > 0) {
		if (check_file(*argv) < 0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}
