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

#include "tools/tools_commands.h"

int file_repair(const char *fname) {
	uint8_t reqbuff[24], *wptr, *buff;
	const uint8_t *rptr;
	uint32_t cmd, leng, inode;
	uint32_t notchanged, erased, repaired;
	int fd;
	fd = open_master_conn(fname, &inode, nullptr, 0, 1);
	if (fd < 0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr, CLTOMA_FUSE_REPAIR);
	put32bit(&wptr, 16);
	put32bit(&wptr, 0);
	put32bit(&wptr, inode);
	put32bit(&wptr, getuid());
	put32bit(&wptr, getgid());
	if (tcpwrite(fd, reqbuff, 24) != 24) {
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
	if (cmd != MATOCL_FUSE_REPAIR) {
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
	rptr = buff;
	cmd = get32bit(&rptr);  // queryid
	if (cmd != 0) {
		printf("%s: master query: wrong answer (queryid)\n", fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	leng -= 4;
	if (leng == 1) {
		printf("%s: %s\n", fname, mfsstrerr(*rptr));
		free(buff);
		close_master_conn(1);
		return -1;
	} else if (leng != 12) {
		printf("%s: master query: wrong answer (leng)\n", fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	notchanged = get32bit(&rptr);
	erased = get32bit(&rptr);
	repaired = get32bit(&rptr);
	free(buff);
	printf("%s:\n", fname);
	print_number(" chunks not changed: ", "\n", notchanged, 1, 0, 1);
	print_number(" chunks erased:      ", "\n", erased, 1, 0, 1);
	print_number(" chunks repaired:    ", "\n", repaired, 1, 0, 1);
	return 0;
}
