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

static int kDefaultTimeout = 30 * 1000;
static int kInfiniteTimeout = -1;

static void set_trashtime_usage() {
	fprintf(stderr,
	        "set objects trashtime (how many seconds file should be left in trash)\n\nusage: "
	        "\n lizardfs settrashtime [-nhHrl] SECONDS[-|+] name [name ...]\n");
	print_numberformat_options();
	print_recursive_option();
	fprintf(stderr, " -l - wait until settrashtime will finish (otherwise there is 30s timeout)\n");
	fprintf(stderr, " SECONDS+ - if trashtime smaller then given value, increase trashtime to given value\n");
	fprintf(stderr, " SECONDS- - if trashtime bigger then given value, decrease trashtime to given value\n");
	fprintf(stderr, " SECONDS - just set trashtime to given value\n");
}

static int set_trashtime(const char *fname, uint32_t trashtime, uint8_t mode, int long_wait) {
	uint8_t reqbuff[25], *wptr, *buff;
	const uint8_t *rptr;
	uint32_t cmd, leng, inode, uid;
	uint32_t changed, notchanged, notpermitted;
	int fd;
	fd = open_master_conn(fname, &inode, nullptr, true);
	if (fd < 0) {
		return -1;
	}
	uid = getuid();
	wptr = reqbuff;
	put32bit(&wptr, CLTOMA_FUSE_SETTRASHTIME);
	put32bit(&wptr, 17);
	put32bit(&wptr, 0);
	put32bit(&wptr, inode);
	put32bit(&wptr, uid);
	put32bit(&wptr, trashtime);
	put8bit(&wptr, mode);
	if (tcpwrite(fd, reqbuff, 25) != 25) {
		printf("%s: master query: send error\n", fname);
		close_master_conn(1);
		return -1;
	}
	if (tcptoread(fd, reqbuff, 8, long_wait ? kInfiniteTimeout : kDefaultTimeout) != 8) {
		printf("%s: master query: receive error\n", fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd != MATOCL_FUSE_SETTRASHTIME) {
		printf("%s: master query: wrong answer (type)\n", fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t *)malloc(leng);
	if (tcptoread(fd, buff, leng, long_wait ? kInfiniteTimeout : kDefaultTimeout) != (int32_t)leng) {
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
	} else if (leng != 12) {
		printf("%s: master query: wrong answer (leng)\n", fname);
		free(buff);
		return -1;
	}
	changed = get32bit(&rptr);
	notchanged = get32bit(&rptr);
	notpermitted = get32bit(&rptr);
	if ((mode & SMODE_RMASK) == 0) {
		if (changed || mode == SMODE_SET) {
			printf("%s: %" PRIu32 "\n", fname, trashtime);
		} else {
			printf("%s: trashtime not changed\n", fname);
		}
	} else {
		printf("%s:\n", fname);
		print_number(" inodes with trashtime changed:     ", "\n", changed, 1, 0, 1);
		print_number(" inodes with trashtime not changed: ", "\n", notchanged, 1, 0, 1);
		print_number(" inodes with permission denied:     ", "\n", notpermitted, 1, 0, 1);
	}
	free(buff);
	return 0;
}

static int gene_set_trashtime_run(int argc, char **argv, int rflag) {
	int ch, status;
	uint32_t trashtime = 86400;
	uint8_t smode = SMODE_SET;
	int long_wait = 0;

	while ((ch = getopt(argc, argv, "rnhHl")) != -1) {
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
		case 'r':
			rflag = 1;
			break;
		case 'l':
			long_wait = 1;
			break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) {
		set_trashtime_usage();
		return 1;
	}

	char *p = argv[0];
	trashtime = 0;
	while (p[0] >= '0' && p[0] <= '9') {
		trashtime *= 10;
		trashtime += (p[0] - '0');
		p++;
	}
	if (p[0] == '\0' || ((p[0] == '-' || p[0] == '+') && p[1] == '\0')) {
		if (p[0] == '-') {
			smode = SMODE_DECREASE;
		} else if (p[0] == '+') {
			smode = SMODE_INCREASE;
		}
	} else {
		fprintf(
		    stderr,
		    "trashtime should be given as number of seconds optionally folowed by '-' or '+'\n");
		set_trashtime_usage();
		return 1;
	}
	argc--;
	argv++;

	if (argc < 1) {
		set_trashtime_usage();
		return 1;
	}
	status = 0;
	while (argc > 0) {
		if (set_trashtime(*argv, trashtime, (rflag) ? (smode | SMODE_RMASK) : smode, long_wait) < 0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}

int rset_trashtime_run(int argc, char **argv) {
	return gene_set_trashtime_run(argc, argv, 1);
}

int set_trashtime_run(int argc, char **argv) {
	return gene_set_trashtime_run(argc, argv, 0);
}
