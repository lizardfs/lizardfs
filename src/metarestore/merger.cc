/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

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
#include "metarestore/merger.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>

#include "protocol/MFSCommunication.h"
#include "common/lizardfs_error_codes.h"
#include "common/slogger.h"
#include "master/restore.h"

#define BSIZE 200000

typedef struct _hentry {
	FILE *fd;
	char *filename;
	char *buff;
	char *ptr;
	uint64_t nextid;
} hentry;

static hentry *heap;
static uint32_t heapsize;
static uint64_t maxidhole;

#define PARENT(x) (((x)-1)/2)
#define CHILD(x) (((x)*2)+1)

void merger_heap_sort_down(void) {
	uint32_t l,r,m;
	uint32_t pos=0;
	hentry x;
	while (pos<heapsize) {
		l = CHILD(pos);
		r = l+1;
		if (l>=heapsize) {
			return;
		}
		m = l;
		if (r<heapsize && heap[r].nextid < heap[l].nextid) {
			m = r;
		}
		if (heap[pos].nextid <= heap[m].nextid) {
			return;
		}
		x = heap[pos];
		heap[pos] = heap[m];
		heap[m] = x;
		pos = m;
	}
}

void merger_heap_sort_up(void) {
	uint32_t pos=heapsize-1;
	uint32_t p;
	hentry x;
	while (pos>0) {
		p = PARENT(pos);
		if (heap[pos].nextid >= heap[p].nextid) {
			return;
		}
		x = heap[pos];
		heap[pos] = heap[p];
		heap[p] = x;
		pos = p;
	}
}


void merger_nextentry(uint32_t pos) {
	if (fgets(heap[pos].buff,BSIZE,heap[pos].fd)) {
		uint64_t nextid = strtoull(heap[pos].buff,&(heap[pos].ptr),10);
		if (heap[pos].nextid==0 || (nextid>heap[pos].nextid && nextid<heap[pos].nextid+maxidhole)) {
			heap[pos].nextid = nextid;
		} else {
			lzfs_pretty_syslog(LOG_ERR, "found garbage at the end of file: %s (last correct id: %" PRIu64 ")",
					heap[pos].filename, heap[pos].nextid);
			heap[pos].nextid = 0;
		}
	} else {
		heap[pos].nextid = 0;
	}
}

void merger_delete_entry(void) {
	if (heap[heapsize].fd) {
		fclose(heap[heapsize].fd);
	}
	if (heap[heapsize].filename) {
		free(heap[heapsize].filename);
	}
	if (heap[heapsize].buff) {
		free(heap[heapsize].buff);
	}
}

void merger_new_entry(const char *filename) {
	// printf("add file: %s\n",filename);
	if ((heap[heapsize].fd = fopen(filename,"r"))!=NULL) {
		heap[heapsize].filename = strdup(filename);
		heap[heapsize].buff = (char*) malloc(BSIZE);
		heap[heapsize].ptr = NULL;
		heap[heapsize].nextid = 0;
		merger_nextentry(heapsize);
	} else {
		lzfs_pretty_syslog(LOG_ERR, "can't open changelog file: %s", filename);
		heap[heapsize].filename = NULL;
		heap[heapsize].buff = NULL;
		heap[heapsize].ptr = NULL;
		heap[heapsize].nextid = 0;
	}
}

int merger_start(const std::vector<std::string>& filenames, uint64_t maxhole) {
	heapsize = 0;
	heap = (hentry*)malloc(sizeof(hentry)*filenames.size());
	if (heap==NULL) {
		return -1;
	}
	for (const auto& filename : filenames) {
		merger_new_entry(filename.c_str());
		if (heap[heapsize].nextid==0) {
			merger_delete_entry();
		} else {
			heapsize++;
			merger_heap_sort_up();
		}
	}
	maxidhole = maxhole;
	return 0;
}

uint8_t merger_loop(void) {
	uint8_t status;
	hentry h;

	while (heapsize) {
//              lzfs_pretty_syslog(LOG_DEBUG, "current id: %" PRIu64 " / %s",heap[0].nextid,heap[0].ptr);
		if ((status=restore(heap[0].filename, heap[0].nextid, heap[0].ptr,
				RestoreRigor::kIgnoreParseErrors)) != LIZARDFS_STATUS_OK) {
			while (heapsize) {
				heapsize--;
				merger_delete_entry();
			}
			return status;
		}
		merger_nextentry(0);
		if (heap[0].nextid==0) {
			heapsize--;
			h = heap[0];
			heap[0] = heap[heapsize];
			heap[heapsize] = h;
			merger_delete_entry();
		}
		merger_heap_sort_down();
	}
	return 0;
}
