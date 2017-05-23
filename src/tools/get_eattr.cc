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

static void get_eattr_usage() {
	fprintf(stderr, "get objects extra attributes\n\nusage:\n lizardfs geteattr [-nhHr] name [name ...]\n");
	print_numberformat_options();
	print_recursive_option();
}

static int get_eattr(const char *fname, uint8_t mode) {
	uint8_t reqbuff[17], *wptr, *buff;
	const uint8_t *rptr;
	uint32_t cmd, leng, inode;
	uint8_t fn, dn, i, j;
	uint32_t fcnt[EATTR_BITS];
	uint32_t dcnt[EATTR_BITS];
	uint8_t eattr;
	uint32_t cnt;
	int fd;
	fd = open_master_conn(fname, &inode, nullptr, 0, 0);
	if (fd < 0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr, CLTOMA_FUSE_GETEATTR);
	put32bit(&wptr, 9);
	put32bit(&wptr, 0);
	put32bit(&wptr, inode);
	put8bit(&wptr, mode);
	if (tcpwrite(fd, reqbuff, 17) != 17) {
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
	if (cmd != MATOCL_FUSE_GETEATTR) {
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
	} else if (leng % 5 != 2) {
		printf("%s: master query: wrong answer (leng)\n", fname);
		free(buff);
		return -1;
	} else if (mode == GMODE_NORMAL && leng != 7) {
		printf("%s: master query: wrong answer (leng)\n", fname);
		free(buff);
		return -1;
	}
	if (mode == GMODE_NORMAL) {
		fn = get8bit(&rptr);
		dn = get8bit(&rptr);
		eattr = get8bit(&rptr);
		cnt = get32bit(&rptr);
		if ((fn != 0 || dn != 1) && (fn != 1 || dn != 0)) {
			printf("%s: master query: wrong answer (fn,dn)\n", fname);
			free(buff);
			return -1;
		}
		if (cnt != 1) {
			printf("%s: master query: wrong answer (cnt)\n", fname);
			free(buff);
			return -1;
		}
		printf("%s: ", fname);
		if (eattr > 0) {
			cnt = 0;
			for (j = 0; j < EATTR_BITS; j++) {
				if (eattr & (1 << j)) {
					printf("%s%s", (cnt) ? "," : "", eattrtab[j]);
					cnt = 1;
				}
			}
			printf("\n");
		} else {
			printf("-\n");
		}
		//              printf("%s: %" PRIX8 "\n",fname,eattr);
	} else {
		for (j = 0; j < EATTR_BITS; j++) {
			fcnt[j] = 0;
			dcnt[j] = 0;
		}
		fn = get8bit(&rptr);
		dn = get8bit(&rptr);
		for (i = 0; i < fn; i++) {
			eattr = get8bit(&rptr);
			cnt = get32bit(&rptr);
			for (j = 0; j < EATTR_BITS; j++) {
				if (eattr & (1 << j)) {
					fcnt[j] += cnt;
				}
			}
		}
		for (i = 0; i < dn; i++) {
			eattr = get8bit(&rptr);
			cnt = get32bit(&rptr);
			for (j = 0; j < EATTR_BITS; j++) {
				if (eattr & (1 << j)) {
					dcnt[j] += cnt;
				}
			}
		}
		printf("%s:\n", fname);
		for (j = 0; j < EATTR_BITS; j++) {
			if (eattrtab[j][0]) {
				printf(" not directory nodes with attribute %16s :", eattrtab[j]);
				print_number(" ", "\n", fcnt[j], 1, 0, 1);
				printf(" directories with attribute         %16s :", eattrtab[j]);
				print_number(" ", "\n", dcnt[j], 1, 0, 1);
			} else {
				if (fcnt[j] > 0) {
					printf(" not directory nodes with attribute      'unknown-%u' :", j);
					print_number(" ", "\n", fcnt[j], 1, 0, 1);
				}
				if (dcnt[j] > 0) {
					printf(" directories with attribute              'unknown-%u' :", j);
					print_number(" ", "\n", dcnt[j], 1, 0, 1);
				}
			}
		}
	}
	free(buff);
	return 0;
}

int get_eattr_run(int argc, char **argv) {
	int rflag = 0;
	int ch, status;

	while ((ch = getopt(argc, argv, "rnhH")) != -1) {
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
		}
	}
	argc -= optind;
	argv += optind;

	if (argc < 1) {
		get_eattr_usage();
		return 1;
	}

	status = 0;
	while (argc > 0) {
		if (get_eattr(*argv, (rflag) ? GMODE_RECURSIVE : GMODE_NORMAL) < 0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}
