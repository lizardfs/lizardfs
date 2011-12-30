#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#include "restore.h"

#define BSIZE 10000

typedef struct _hentry {
	FILE *fd;
	const char *filename;
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
			printf("found garbage at the end of file: %s (last correct id: %"PRIu64")\n",heap[pos].filename,heap[pos].nextid);
			heap[pos].nextid = 0;
		}
	} else {
		heap[pos].nextid = 0;
	}
}

void merger_delete_entry(void) {
	fclose(heap[heapsize].fd);
	free(heap[heapsize].buff);
}

void merger_new_entry(const char *filename) {
	if ((heap[heapsize].fd = fopen(filename,"r"))!=NULL) {
		heap[heapsize].filename = filename;
		heap[heapsize].buff = malloc(BSIZE);
		heap[heapsize].ptr = NULL;
		heap[heapsize].nextid = 0;
		merger_nextentry(heapsize);
	} else {
		printf("can't open changelog file: %s\n",filename);
		heap[heapsize].filename = NULL;
		heap[heapsize].buff = NULL;
		heap[heapsize].ptr = NULL;
		heap[heapsize].nextid = 0;
	}
}

int merger_start(uint32_t files,char **filenames,uint64_t maxhole) {
	uint32_t i;
	heapsize = 0;
	heap = (hentry*)malloc(sizeof(hentry)*files);
	if (heap==NULL) {
		return -1;
	}
	for (i=0 ; i<files ; i++) {
		merger_new_entry(filenames[i]);
//		printf("file: %s / firstid: %"PRIu64"\n",filenames[i],heap[heapsize].nextid);
		if (heap[heapsize].nextid==0) {
			merger_delete_entry();
		} else {
			heapsize++;
			merger_heap_sort_up();
		}
	}
	maxidhole = maxhole;
//	for (i=0 ; i<heapsize ; i++) {
//		printf("heap: %u / firstid: %"PRIu64"\n",i,heap[i].nextid);
//	}
	return 0;
}

int merger_loop(void) {
	int status;
	hentry h;
	while (heapsize) {
//		printf("current id: %"PRIu64" / %s\n",heap[0].nextid,heap[0].ptr);
		if ((status=restore(heap[0].filename,heap[0].nextid,heap[0].ptr))<0) {
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
