/*
   Copyright 2008 Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"

// #include <execinfo.h> // for backtrace - debugs only
#include <inttypes.h>
#include <syslog.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <dirent.h>
#include <errno.h>
#ifdef _THREAD_SAFE
#include <pthread.h>
#endif

#include "MFSCommunication.h"
#include "cfg.h"
#include "datapack.h"
#include "crc.h"
#ifndef _THREAD_SAFE
#include "main.h"
#endif


#define PRESERVE_BLOCK 1

#if defined(HAVE_PREAD) && defined(HAVE_PWRITE)
#define USE_PIO 1
#endif

/* system every DELAYEDSTEP seconds searches opened/crc_loaded chunk list for chunks to be closed/free crc */
#define DELAYEDSTEP 2

#define OPENDELAY 5
#define CRCDELAY 100
#define OPENSTEPS (OPENDELAY/DELAYEDSTEP)+1
#define CRCSTEPS (CRCDELAY/DELAYEDSTEP)+1

#ifdef PRESERVE_BLOCK
#define PRESERVEDELAY 10
#define PRESERVESTEPS (PRESERVEDELAY/DELAYEDSTEP)+1
#endif

#define LOSTCHUNKSBLOCKSIZE 1024

#define CHUNKHDRSIZE (1024+4*1024)
#define CHUNKHDRCRC 1024

#define LASTERRSIZE 3
#define LASTERRTIME 60

#define HASHSIZE 32768
#define HASHPOS(chunkid) ((chunkid)&0x7FFF)

#define DHASHSIZE 64
#define DHASHPOS(chunkid) ((chunkid)&0x3F)

#define CH_NEW_NONE 0
#define CH_NEW_AUTO 1
#define CH_NEW_EXCLUSIVE 2

#define CHUNKLOCKED ((void*)1)

typedef struct damagedchunk {
	uint64_t chunkid;
	struct damagedchunk *next;
} damagedchunk;

typedef struct lostchunk {
	uint64_t chunkidblock[LOSTCHUNKSBLOCKSIZE];
	uint32_t chunksinblock;
	struct lostchunk *next;
} lostchunk;

typedef struct dopchunk {
	uint64_t chunkid;
	struct dopchunk *next;
} dopchunk;

struct folder;

typedef struct ioerror {
	uint64_t chunkid;
	uint32_t timestamp;
} ioerror;

#ifdef _THREAD_SAFE
typedef struct _cntcond {
	pthread_cond_t cond;
	uint32_t wcnt;
	struct _cntcond *next;
} cntcond;

#endif

typedef struct chunk {
	char *filename;
	uint64_t chunkid;
	struct folder *owner;
	uint32_t version;
	uint16_t blocks;
	uint16_t crcrefcount;
	uint8_t opensteps;
	uint8_t crcsteps;
	uint8_t crcchanged;
#ifdef _THREAD_SAFE
#define CH_AVAIL 0
#define CH_LOCKED 1
#define CH_DELETED 2
#define CH_TOBEDELETED 3
	uint8_t state;	// CH_AVAIL,CH_LOCKED,CH_DELETED
	cntcond *ccond;
#endif
	uint8_t *crc;
	int fd;

#ifdef PRESERVE_BLOCK
	uint8_t *block;
	uint16_t blockno;	// 0xFFFF == invalid
	uint8_t blocksteps;
#endif
#ifdef _THREAD_SAFE
	uint32_t testtime;	// at start use max(atime,mtime) then every operation set it to current time
	struct chunk *testnext,**testprev;
#endif
	struct chunk *next;
} chunk;

typedef struct folder {
	char *path;
	int needrefresh;
	int todel;
	uint64_t leavefree;
	uint64_t avail;
	uint64_t total;
	ioerror lasterrtab[LASTERRSIZE];
	uint32_t chunkcount;
	uint32_t lasterrindx;
	uint32_t lastrefresh;
	dev_t devid;
	ino_t lockinode;
#ifdef _THREAD_SAFE
	pthread_t scanthread;
	uint8_t testid;
	struct chunk *testhead,**testtail;
#endif
	struct folder *next;
} folder;

/*
typedef struct damaged {
	char *path;
	uint64_t avail;
	uint64_t total;
	ioerror lasterror;
	uint32_t chunkcount;
	struct damaged_disk *next;
} damaged;
*/

static uint32_t HDDTestFreq=10;

/* folders data */
static folder *damagedhead=NULL;
static folder *folderhead=NULL;
static folder *bestfolder=NULL;

/* chunk hash */
static chunk* hashtab[HASHSIZE];

/* extra chunk info */
static dopchunk *dophashtab[DHASHSIZE];
//static dopchunk *dopchunks=NULL;
static dopchunk *newdopchunks=NULL;

// master reports
static damagedchunk *damagedchunks=NULL;
static lostchunk *lostchunks=NULL;
static uint32_t errorcounter=0;
static int hddspacechanged=0;

#ifdef _THREAD_SAFE

static pthread_t foldersthread,delayedthread,testerthread;

// stats_X
static pthread_mutex_t statslock = PTHREAD_MUTEX_INITIALIZER;

// newdopchunks + dophashtab
static pthread_mutex_t doplock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t ndoplock = PTHREAD_MUTEX_INITIALIZER;

// master reports = damaged chunks, lost chunks, errorcounter, hddspacechanged
static pthread_mutex_t dclock = PTHREAD_MUTEX_INITIALIZER;

// hashtab - only hash tab, chunks have their own separate locks
static pthread_mutex_t hashlock = PTHREAD_MUTEX_INITIALIZER;
static cntcond *cclist = NULL;

// damagedhead + folderhead + all data in structures
static pthread_mutex_t folderlock = PTHREAD_MUTEX_INITIALIZER;

// chunk tester
static pthread_mutex_t testlock = PTHREAD_MUTEX_INITIALIZER;
#endif

#ifndef PRESERVE_BLOCK
#ifdef _THREAD_SAFE
static pthread_key_t hdrbufferkey;
static pthread_key_t blockbufferkey;
#else
static uint8_t hdrbuffer[CHUNKHDRSIZE];
static uint8_t blockbuffer[0x10000];
#endif
#endif

static uint32_t emptyblockcrc;

static uint32_t stats_bytesr=0;
static uint32_t stats_bytesw=0;
static uint32_t stats_opr=0;
static uint32_t stats_opw=0;
static uint32_t stats_databytesr=0;
static uint32_t stats_databytesw=0;
static uint32_t stats_dataopr=0;
static uint32_t stats_dataopw=0;
static uint64_t stats_rtime=0;
static uint64_t stats_wtime=0;

static uint32_t stats_create=0;
static uint32_t stats_delete=0;
static uint32_t stats_test=0;
static uint32_t stats_version=0;
static uint32_t stats_duplicate=0;
static uint32_t stats_truncate=0;
static uint32_t stats_duptrunc=0;

/*
void printbacktrace(void) {
	void* callstack[128];
	int i, frames = backtrace(callstack, 128);
	char** strs = backtrace_symbols(callstack, frames);
	for (i = 0; i < frames; ++i) {
		printf("%s\n", strs[i]);
	}
	free(strs);
}
*/
void hdd_report_damaged_chunk(uint64_t chunkid) {
	damagedchunk *dc;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&dclock);
#endif
	dc = malloc(sizeof(damagedchunk));
	dc->chunkid = chunkid;
	dc->next = damagedchunks;
	damagedchunks = dc;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&dclock);
#endif
}

uint32_t hdd_get_damaged_chunk_count(void) {
	damagedchunk *dc;
	uint32_t result;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&dclock);
#endif
	result=0;
	for (dc = damagedchunks ; dc ; dc=dc->next) {
		result++;
	}
	return result;
}

void hdd_get_damaged_chunk_data(uint8_t *buff) {
	damagedchunk *dc,*ndc;
	uint64_t chunkid;
	if (buff) {
		dc=damagedchunks;
		while (dc) {
			ndc = dc;
			dc = dc->next;
			chunkid = ndc->chunkid;
			put64bit(&buff,chunkid);
			free(ndc);
		}
		damagedchunks=NULL;
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&dclock);
#endif
}

void hdd_report_lost_chunk(uint64_t chunkid) {
	lostchunk *lc;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&dclock);
#endif
	if (lostchunks && lostchunks->chunksinblock<LOSTCHUNKSBLOCKSIZE) {
		lostchunks->chunkidblock[lostchunks->chunksinblock++]=chunkid;
	} else {
		lc = malloc(sizeof(lostchunk));
		lc->chunkidblock[0]=chunkid;
		lc->chunksinblock=1;
		lc->next = lostchunks;
		lostchunks = lc;
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&dclock);
#endif
}

uint32_t hdd_get_lost_chunk_count(void) {
	lostchunk *lc;
	uint32_t result;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&dclock);
#endif
	result=0;
	for (lc=lostchunks ; lc ; lc=lc->next) {
		result+=lc->chunksinblock;
	}
	return result;
}

void hdd_get_lost_chunk_data(uint8_t *buff) {
	lostchunk *lc,*nlc;
	uint64_t chunkid;
	uint32_t i;
	if (buff) {
		lc=lostchunks;
		while (lc) {
			nlc = lc;
			for (i=0 ; i<lc->chunksinblock ; i++) {
				chunkid = lc->chunkidblock[i];
				put64bit(&buff,chunkid);
			}
			lc = lc->next;
			free(nlc);
		}
		lostchunks=NULL;
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&dclock);
#endif
}

uint32_t hdd_errorcounter(void) {
	uint32_t result;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&dclock);
#endif
	result = errorcounter;
	errorcounter = 0;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&dclock);
#endif
	return result;
}

int hdd_spacechanged(void) {
	uint32_t result;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&dclock);
#endif
	result = hddspacechanged;
	hddspacechanged = 0;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&dclock);
#endif
	return result;
}

void hdd_stats(uint32_t *br,uint32_t *bw,uint32_t *opr,uint32_t *opw,uint32_t *dbr,uint32_t *dbw,uint32_t *dopr,uint32_t *dopw,uint64_t *rtime,uint64_t *wtime) {
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&statslock);
#endif
	*br = stats_bytesr;
	*bw = stats_bytesw;
	*opr = stats_opr;
	*opw = stats_opw;
	*dbr = stats_databytesr;
	*dbw = stats_databytesw;
	*dopr = stats_dataopr;
	*dopw = stats_dataopw;
	*rtime = stats_rtime;
	*wtime = stats_wtime;
	stats_bytesr=0;
	stats_bytesw=0;
	stats_opr=0;
	stats_opw=0;
	stats_databytesr=0;
	stats_databytesw=0;
	stats_dataopr=0;
	stats_dataopw=0;
	stats_rtime=0;
	stats_wtime=0;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&statslock);
#endif
}

void hdd_op_stats(uint32_t *op_create,uint32_t *op_delete,uint32_t *op_version,uint32_t *op_duplicate,uint32_t *op_truncate,uint32_t *op_duptrunc,uint32_t *op_test) {
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&statslock);
#endif
	*op_create = stats_create;
	*op_delete = stats_delete;
	*op_version = stats_version;
	*op_duplicate = stats_duplicate;
	*op_truncate = stats_truncate;
	*op_duptrunc = stats_duptrunc;
	*op_test = stats_test;
	stats_create=0;
	stats_delete=0;
	stats_version=0;
	stats_duplicate=0;
	stats_truncate=0;
	stats_duptrunc=0;
	stats_test=0;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&statslock);
#endif
}

static inline void hdd_stats_read(uint32_t size) {
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&statslock);
#endif
	stats_opr++;
	stats_bytesr += size;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&statslock);
#endif
}

static inline void hdd_stats_write(uint32_t size) {
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&statslock);
#endif
	stats_opw++;
	stats_bytesw += size;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&statslock);
#endif
}

static inline void hdd_stats_dataread(uint32_t size,uint64_t rtime) {
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&statslock);
#endif
	stats_dataopr++;
	stats_databytesr += size;
	stats_rtime += rtime;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&statslock);
#endif
}

static inline void hdd_stats_datawrite(uint32_t size,uint64_t wtime) {
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&statslock);
#endif
	stats_dataopw++;
	stats_databytesw += size;
	stats_wtime += wtime;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&statslock);
#endif
}

uint32_t hdd_diskinfo_size() {
	folder *f;
	uint32_t s=0,sl;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	for (f=folderhead ; f ; f=f->next ) {
		sl = strlen(f->path);
		if (sl>255) {
			sl=255;
		}
		s+=34+sl;
	}
	for (f=damagedhead ; f ; f=f->next ) {
		sl = strlen(f->path);
		if (sl>255) {
			sl=255;
		}
		s+=34+sl;
	}
	return s;
}

void hdd_diskinfo_data(uint8_t *buff) {
	folder *f;
	uint32_t sl;
	uint32_t ei;
	if (buff) {
		for (f=folderhead ; f ; f=f->next ) {
			sl = strlen(f->path);
			if (sl>255) {
				put8bit(&buff,255);
				memcpy(buff,"(...)",5);
				memcpy(buff+5,f->path+(sl-250),250);
				buff+=255;
			} else {
				put8bit(&buff,sl);
				if (sl>0) {
					memcpy(buff,f->path,sl);
					buff+=sl;
				}
			}
			if (f->todel) {
				put8bit(&buff,1);
			} else {
				put8bit(&buff,0);
			}
			ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
			put64bit(&buff,f->lasterrtab[ei].chunkid);
			put32bit(&buff,f->lasterrtab[ei].timestamp);
			put64bit(&buff,f->total-f->avail);
			put64bit(&buff,f->total);
			put32bit(&buff,f->chunkcount);
		}
		for (f=damagedhead ; f ; f=f->next ) {
			sl = strlen(f->path);
			if (sl>255) {
				put8bit(&buff,255);
				memcpy(buff,"(...)",5);
				memcpy(buff+5,f->path+(sl-250),250);
				buff+=255;
			} else {
				put8bit(&buff,sl);
				if (sl>0) {
					memcpy(buff,f->path,sl);
					buff+=sl;
				}
			}
			if (f->todel) {
				put8bit(&buff,3);
			} else {
				put8bit(&buff,2);
			}
			ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
			put64bit(&buff,f->lasterrtab[ei].chunkid);
			put32bit(&buff,f->lasterrtab[ei].timestamp);
			put64bit(&buff,f->total-f->avail);
			put64bit(&buff,f->total);
			put32bit(&buff,f->chunkcount);
		}
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
}

static inline void hdd_chunk_remove(chunk *c) {
	chunk **cptr,*cp;
	uint32_t hashpos = HASHPOS(c->chunkid);
	cptr = &(hashtab[hashpos]);
	while ((cp=*cptr)) {
		if (c==cp) {
			*cptr = cp->next;
			if (cp->fd>=0) {
				close(cp->fd);
			}
			if (cp->crc!=NULL) {
				free(cp->crc);
			}
#ifdef PRESERVE_BLOCK
			if (cp->block!=NULL) {
				free(cp->block);
			}
#endif /* PRESERVE_BLOCK */
			if (cp->filename!=NULL) {
				free(cp->filename);
			}
#ifdef _THREAD_SAFE
			if (cp->owner) {
				pthread_mutex_lock(&testlock);
				if (cp->testnext) {
					cp->testnext->testprev = cp->testprev;
				} else {
					cp->owner->testtail = cp->testprev;
				}
				*(cp->testprev) = cp->testnext;
				pthread_mutex_unlock(&testlock);
			}
#endif
			free(cp);
			return;
		}
		cptr = &(cp->next);
	}
}

#ifdef _THREAD_SAFE
static void hdd_chunk_release(chunk *c) {
	pthread_mutex_lock(&hashlock);
	if (c->state==CH_LOCKED) {
		c->state = CH_AVAIL;
		if (c->ccond) {
//			printf("wake up one thread waiting for AVAIL chunk: %"PRIu64" on ccond:%p\n",c->chunkid,c->ccond);
//			printbacktrace();
			pthread_cond_signal(&(c->ccond->cond));
		}
	} else if (c->state==CH_TOBEDELETED) {
		if (c->ccond) {
			c->state = CH_DELETED;
//			printf("wake up one thread waiting for DELETED chunk: %"PRIu64" on ccond:%p\n",c->chunkid,c->ccond);
//			printbacktrace();
			pthread_cond_signal(&(c->ccond->cond));
		} else {
			hdd_chunk_remove(c);
		}
	}
	pthread_mutex_unlock(&hashlock);
}
#else
#define hdd_chunk_release(c)
#endif

static chunk* hdd_chunk_tryfind(uint64_t chunkid) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&hashlock);
#endif
	for (c=hashtab[hashpos] ; c && c->chunkid!=chunkid ; c=c->next) {}
#ifdef _THREAD_SAFE
	if (c!=NULL) {
		if (c->state==CH_LOCKED) {
			c = CHUNKLOCKED;
		} else if (c->state!=CH_AVAIL) {
			c = NULL;
		} else {
			c->state=CH_LOCKED;
		}
	}
	pthread_mutex_unlock(&hashlock);
#endif
	return c;
}

static chunk* hdd_chunk_get(uint64_t chunkid,uint8_t cflag) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
#ifdef _THREAD_SAFE
	cntcond *cc;
	pthread_mutex_lock(&hashlock);
#endif
	for (c=hashtab[hashpos] ; c && c->chunkid!=chunkid ; c=c->next) {}
	if (c==NULL) {
		if (cflag!=CH_NEW_NONE) {
			c = malloc(sizeof(chunk));
			c->chunkid = chunkid;
			c->version = 0;
			c->owner = NULL;
			c->filename = NULL;
			c->blocks = 0;
			c->crcrefcount = 0;
			c->opensteps = 0;
			c->crcsteps = 0;
			c->crcchanged = 0;
			c->fd = -1;
			c->crc = NULL;
#ifdef _THREAD_SAFE
			c->state = CH_LOCKED;
			c->ccond = NULL;
#endif
#ifdef PRESERVE_BLOCK
			c->block = NULL;
			c->blockno = 0xFFFF;
			c->blocksteps = 0;
#endif
			c->testnext = NULL;
			c->testprev = NULL;
			c->next = hashtab[hashpos];
			hashtab[hashpos]=c;
		}
#ifdef _THREAD_SAFE
		pthread_mutex_unlock(&hashlock);
#endif
		return c;
	}
	if (cflag==CH_NEW_EXCLUSIVE) {
#ifdef _THREAD_SAFE
		if (c->state==CH_AVAIL || c->state==CH_LOCKED) {
			pthread_mutex_unlock(&hashlock);
			return NULL;
		}
#else
		return NULL;
#endif
	}
#ifdef _THREAD_SAFE
	for (;;) {
		switch (c->state) {
		case CH_AVAIL:
			c->state=CH_LOCKED;
			pthread_mutex_unlock(&hashlock);
			return c;
		case CH_DELETED:
			if (cflag!=CH_NEW_NONE) {
				if (c->fd>=0) {
					close(c->fd);
				}
				if (c->crc!=NULL) {
					free(c->crc);
				}
#ifdef PRESERVE_BLOCK
				if (c->block!=NULL) {
					free(c->block);
				}
#endif /* PRESERVE_BLOCK */
				if (c->filename!=NULL) {
					free(c->filename);
				}
#ifdef _THREAD_SAFE
				pthread_mutex_lock(&testlock);
				if (c->testnext) {
					c->testnext->testprev = c->testprev;
				} else {
					c->owner->testtail = c->testprev;
				}
				*(c->testprev) = c->testnext;
				c->testnext = NULL;
				c->testprev = NULL;
				pthread_mutex_unlock(&testlock);
#endif
				c->version = 0;
				c->owner = NULL;
				c->filename = NULL;
				c->blocks = 0;
				c->crcrefcount = 0;
				c->opensteps = 0;
				c->crcsteps = 0;
				c->crcchanged = 0;
				c->fd = -1;
				c->crc = NULL;
#ifdef PRESERVE_BLOCK
				c->block = NULL;
				c->blockno = 0xFFFF;
				c->blocksteps = 0;
#endif /* PRESERVE_BLOCK */
				c->state = CH_LOCKED;
				pthread_mutex_unlock(&hashlock);
				return c;
			}
			if (c->ccond==NULL) {	// no more waiting threads - remove
				hdd_chunk_remove(c);
			} else {	// there are waiting threads - wake them up
//				printf("wake up one thread waiting for DELETED chunk: %"PRIu64" on ccond:%p\n",c->chunkid,c->ccond);
//				printbacktrace();
				pthread_cond_signal(&(c->ccond->cond));
			}
			pthread_mutex_unlock(&hashlock);
			return NULL;
		case CH_TOBEDELETED:
		case CH_LOCKED:
			if (c->ccond==NULL) {
				for (cc=cclist ; cc && cc->wcnt ; cc=cc->next) {}
				if (cc==NULL) {
					cc = malloc(sizeof(cntcond));
					pthread_cond_init(&(cc->cond),NULL);
					cc->wcnt = 0;
					cc->next = cclist;
					cclist = cc;
				}
				c->ccond = cc;
			}
			c->ccond->wcnt++;
//			printf("wait for %s chunk: %"PRIu64" on ccond:%p\n",(c->state==CH_LOCKED)?"LOCKED":"TOBEDELETED",c->chunkid,c->ccond);
//			printbacktrace();
			pthread_cond_wait(&(c->ccond->cond),&hashlock);
//			printf("%s chunk: %"PRIu64" woke up on ccond:%p\n",(c->state==CH_LOCKED)?"LOCKED":(c->state==CH_DELETED)?"DELETED":(c->state==CH_AVAIL)?"AVAIL":"TOBEDELETED",c->chunkid,c->ccond);
			c->ccond->wcnt--;
			if (c->ccond->wcnt==0) {
				c->ccond=NULL;
			}
		}
	}
#else
	return c;
#endif
}

#ifdef _THREAD_SAFE
static void hdd_chunk_delete(chunk *c) {
	folder *f;
	pthread_mutex_lock(&hashlock);
	f = c->owner;
	if (c->ccond) {
		c->state = CH_DELETED;
//		printf("wake up one thread waiting for DELETED chunk: %"PRIu64" ccond:%p\n",c->chunkid,c->ccond);
//		printbacktrace();
		pthread_cond_signal(&(c->ccond->cond));
	} else {
		hdd_chunk_remove(c);
	}
	pthread_mutex_unlock(&hashlock);
	pthread_mutex_lock(&folderlock);
	f->chunkcount--;
	f->needrefresh = 1;
	pthread_mutex_unlock(&folderlock);
}
#else
static void hdd_chunk_delete(chunk *c) {
	c->owner->chunkcount--;
	c->owner->needrefresh = 1;
	hdd_chunk_remove(c);
}
#endif

static chunk* hdd_chunk_create(folder *f,uint64_t chunkid,uint32_t version) {
	uint32_t leng;
	chunk *c;

	c = hdd_chunk_get(chunkid,CH_NEW_EXCLUSIVE);
	if (c==NULL) {
		return NULL;
	}
	c->version = version;
	leng = strlen(f->path);
	c->filename = malloc(leng+38);
	memcpy(c->filename,f->path,leng);
	sprintf(c->filename+leng,"%1X/chunk_%016"PRIX64"_%08"PRIX32".mfs",(unsigned int)(chunkid&0xF),chunkid,version);
	f->needrefresh = 1;
	f->chunkcount++;
	c->owner = f;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&testlock);
	c->testnext = NULL;
	c->testprev = f->testtail;
	(*c->testprev) = c;
	f->testtail = &(c->testnext);
	pthread_mutex_unlock(&testlock);
#endif
	return c;
}

#define hdd_chunk_find(chunkid) hdd_chunk_get(chunkid,CH_NEW_NONE)

#ifdef _THREAD_SAFE
static void hdd_chunk_testmove(chunk *c) {
	pthread_mutex_lock(&testlock);
	if (c->testnext) {
		*(c->testprev) = c->testnext;
		c->testnext->testprev = c->testprev;
		c->testnext = NULL;
		c->testprev = c->owner->testtail;
		*(c->testprev) = c;
		c->owner->testtail = &(c->testnext);
	}
	c->testtime = time(NULL);
	pthread_mutex_unlock(&testlock);
}
#endif

// no locks - locked by caller
static inline void hdd_refresh_usage(folder *f) {
	struct statvfs fsinfo;

	if (statvfs(f->path,&fsinfo)<0) {
		f->avail = 0ULL;
		f->total = 0ULL;
	}
	f->avail = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_bavail);
	f->total = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks);
	if (f->avail < f->leavefree) {
		f->avail = 0ULL;
	} else {
		f->avail -= f->leavefree;
	}
}

void hdd_check_folders() {
	folder **fptr,*f;
	chunk **cptr,*c;
	uint32_t i;
	uint32_t now;
	int changed=0,err;
	double avail,bestavail=0.0;
	struct timeval tv;
	gettimeofday(&tv,NULL);
	now = tv.tv_sec;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	bestfolder=NULL;
	fptr = &(folderhead);
	while ((f=*fptr)) {
		err=1;
		for (i=0 ; err && i<LASTERRSIZE ; i++) {
			if (f->lasterrtab[i].timestamp+LASTERRTIME<now) {
				err=0;
			}
		}
		if (err) {
			syslog(LOG_WARNING,"%u errors occurred in %u seconds on folder: %s",LASTERRSIZE,LASTERRTIME,f->path);
#ifdef _THREAD_SAFE
			pthread_mutex_lock(&testlock);
			f->testhead = NULL;
			f->testtail = NULL;
			pthread_mutex_unlock(&testlock);
			pthread_mutex_lock(&hashlock);
#endif
			for (i=0 ; i<HASHSIZE ; i++) {
				cptr = &(hashtab[i]);
				while ((c=*cptr)) {
					if (c->owner==f) {
						hdd_report_lost_chunk(c->chunkid);
#ifdef _THREAD_SAFE
						if (c->state==CH_AVAIL) {
#endif
							*cptr=c->next;
							if (c->fd>=0) {
								close(c->fd);
							}
							if (c->crc!=NULL) {
								free(c->crc);
							}
#ifdef PRESERVE_BLOCK
							if (c->block!=NULL) {
								free(c->block);
							}
#endif /* PRESERVE_BLOCK */
							if (c->filename) {
								free(c->filename);
							}
							free(c);
#ifdef _THREAD_SAFE
						} else if (c->state==CH_LOCKED) {
							cptr = &(c->next);
							c->testnext = NULL;
							c->testprev = NULL;
							c->owner = NULL;
							c->state=CH_TOBEDELETED;
						}
#endif
					} else {
						cptr = &(c->next);
					}
				}
			}
#ifdef _THREAD_SAFE
			pthread_mutex_unlock(&hashlock);
#endif
			(*fptr)=f->next;
//			free(f->path);
//			free(f);
			f->next = damagedhead;
			damagedhead = f;
			changed=1;
		} else {
			if (f->needrefresh || f->lastrefresh+60<now) {
				hdd_refresh_usage(f);
				f->needrefresh = 0;
				f->lastrefresh = now;
				changed=1;
			}
			if (f->total>0 && f->avail>0 && f->todel==0) {
				avail = (double)(f->avail)/(double)(f->total);
				if (avail>bestavail) {
					bestavail = avail;
					bestfolder = f;
				}
			}
			fptr = &(f->next);
		}
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
	if (changed) {
#ifdef _THREAD_SAFE
		pthread_mutex_lock(&dclock);
#endif
		hddspacechanged=1;
#ifdef _THREAD_SAFE
		pthread_mutex_unlock(&dclock);
#endif
	}
}

void hdd_error_occured(chunk *c) {
	uint32_t i;
	folder *f;
	struct timeval tv;

#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	gettimeofday(&tv,NULL);
	f = c->owner;
	i = f->lasterrindx;
	f->lasterrtab[i].chunkid = c->chunkid;
	f->lasterrtab[i].timestamp = tv.tv_sec;
	i = (i+1)%LASTERRSIZE;
	f->lasterrindx = i;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif

#ifdef _THREAD_SAFE
	pthread_mutex_lock(&dclock);
#endif
	errorcounter++;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&dclock);
#endif
}


/* interface */

uint32_t hdd_get_chunks_count() {
	uint32_t res=0;
	uint32_t i;
	chunk *c;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&hashlock);
#endif
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c = hashtab[i] ; c ; c=c->next) {
			res++;
		}
	}
	return res;
}

void hdd_get_chunks_data(uint8_t *buff) {
	uint32_t i,v;
	chunk *c;
	if (buff) {
		for (i=0 ; i<HASHSIZE ; i++) {
			for (c = hashtab[i] ; c ; c=c->next) {
				put64bit(&buff,c->chunkid);
				v = c->version;
				if (c->owner->todel) {
					v|=0x80000000;
				}
				put32bit(&buff,v);
			}
		}
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&hashlock);
#endif
}

/*
uint32_t get_changedchunkscount() {
	uint32_t res=0;
	folder *f;
	chunk *c;
	if (somethingchanged==0) {
		return 0;
	}
	for (f = folderhead ; f ; f=f->next) {
		for (c = f->chunkhead ; c ; c=c->next) {
			if (c->lengthchanged) {
				res++;
			}
		}
	}
	return res;
}

void fill_changedchunksinfo(uint8_t *buff) {
	folder *f;
	chunk *c;
	for (f = folderhead ; f ; f=f->next) {
		for (c = f->chunkhead ; c ; c=c->next) {
			if (c->lengthchanged) {
				put64bit(&buff,c->chunkid);
				put32bit(&buff,c->version);
				c->lengthchanged = 0;
			}
		}
	}
	somethingchanged = 0;
}
*/

void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount) {
	folder *f;
	uint64_t avail,total;
	uint64_t tdavail,tdtotal;
	uint32_t chunks,tdchunks;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	avail = total = tdavail = tdtotal = 0ULL;
	chunks = tdchunks = 0;
	for (f = folderhead ; f ; f=f->next) {
		if (f->todel==0) {
			avail+=f->avail;
			total+=f->total;
			chunks+=f->chunkcount;
		} else {
			tdavail+=f->avail;
			tdtotal+=f->total;
			tdchunks+=f->chunkcount;
		}
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
	*usedspace = total - avail;
	*totalspace = total;
	*chunkcount = chunks;
	*tdusedspace = tdtotal - tdavail;
	*tdtotalspace = tdtotal;
	*tdchunkcount = tdchunks;
}

void chunk_emptycrc(chunk *c) {
	c->crc = (uint8_t*)malloc(4096);
}

int chunk_readcrc(chunk *c) {
	int ret;
	uint8_t hdr[20];
	const uint8_t *ptr;
	uint64_t chunkid;
	uint32_t version;
#ifdef USE_PIO
	if (pread(c->fd,hdr,20,0)!=20) {
		syslog(LOG_WARNING,"chunk_readcrc: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
		return ERROR_IO;
	}
#else /* USE_PIO */
	lseek(c->fd,0,SEEK_SET);
	if (read(c->fd,hdr,20)!=20) {
		syslog(LOG_WARNING,"chunk_readcrc: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
		return ERROR_IO;
	}
#endif /* USE_PIO */
	if (memcmp(hdr,"MFSC 1.0",8)!=0) {
		syslog(LOG_WARNING,"chunk_readcrc: file:%s - wrong header",c->filename);
		return ERROR_IO;
	}
	ptr = hdr+8;
	chunkid = get64bit(&ptr);
	version = get32bit(&ptr);
	if (c->chunkid!=chunkid || c->version!=version) {
		syslog(LOG_WARNING,"chunk_readcrc: file:%s - wrong id/version in header (%016"PRIX64"_%08"PRIX32")",c->filename,chunkid,version);
		return ERROR_IO;
	}
	c->crc = (uint8_t*)malloc(4096);
#ifdef USE_PIO
	ret = pread(c->fd,c->crc,4096,CHUNKHDRCRC);
#else /* USE_PIO */
	lseek(c->fd,CHUNKHDRCRC,SEEK_SET);
	ret = read(c->fd,c->crc,4096);
#endif /* USE_PIO */
	hdd_stats_read(4096);
	if (ret!=4096) {
		syslog(LOG_WARNING,"chunk_readcrc: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
		free(c->crc);
		c->crc = NULL;
		return ERROR_IO;
	}
	return STATUS_OK;
}

void chunk_freecrc(chunk *c) {
	free(c->crc);
	c->crc = NULL;
}

int chunk_writecrc(chunk *c) {
	int ret;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	c->owner->needrefresh = 1;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
#ifdef USE_PIO
	ret = pwrite(c->fd,c->crc,4096,CHUNKHDRCRC);
#else /* USE_PIO */
	lseek(c->fd,CHUNKHDRCRC,SEEK_SET);
	ret = write(c->fd,c->crc,4096);
#endif /* USE_PIO */
	hdd_stats_write(4096);
	if (ret!=4096) {
		syslog(LOG_WARNING,"chunk_writecrc: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
		return ERROR_IO;
	}
	return STATUS_OK;
}

void hdd_test_show_chunks(void) {
	uint32_t hashpos;
	chunk *c;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&hashlock);
#endif
	for (hashpos=0 ; hashpos<HASHSIZE ; hashpos++) {
		for (c=hashtab[hashpos] ; c ; c=c->next) {
			printf("chunk id:%"PRIu64" version:%"PRIu32" state:%"PRIu8"\n",c->chunkid,c->version,c->state);
		}
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&hashlock);
#endif
}

void hdd_test_show_openedchunks(void) {
	dopchunk *cc,*tcc;
	uint32_t dhashpos;
	chunk *c;

#ifdef _THREAD_SAFE
	printf("lock doplock\n");
	if (pthread_mutex_lock(&doplock)<0) {
		printf("lock error: %u\n",errno);
	}
	printf("lock ndoplock\n");
	if (pthread_mutex_lock(&ndoplock)<0) {
		printf("lock error: %u\n",errno);
	}
#endif
/* append new chunks */
	cc = newdopchunks;
	while (cc) {
		dhashpos = DHASHPOS(cc->chunkid);
		for (tcc=dophashtab[dhashpos] ; tcc && tcc->chunkid!=cc->chunkid ; tcc=tcc->next) {}
		if (tcc) {	// found - ignore
			tcc = cc;
			cc = cc->next;
			free(tcc);
		} else {	// not found - add
			tcc = cc;
			cc = cc->next;
			tcc->next = dophashtab[dhashpos];
			dophashtab[dhashpos] = tcc;
		}
	}
	newdopchunks=NULL;
#ifdef _THREAD_SAFE
	printf("unlock ndoplock\n");
	if (pthread_mutex_unlock(&ndoplock)<0) {
		printf("unlock error: %u\n",errno);
	}
#endif
/* show all */
	for (dhashpos=0 ; dhashpos<DHASHSIZE ; dhashpos++) {
		for (cc = dophashtab[dhashpos]; cc ; cc=cc->next) {
			c = hdd_chunk_find(cc->chunkid);
			if (c==NULL) {	// no chunk - delete entry
				printf("id: %"PRIu64" - chunk doesn't exist\n",cc->chunkid);
			} else if (c->crcrefcount>0) {	// io in progress - skip entry
				printf("id: %"PRIu64" - chunk in use (refcount:%u)\n",cc->chunkid,c->crcrefcount);
				hdd_chunk_release(c);
			} else {
#ifdef PRESERVE_BLOCK
				printf("id: %"PRIu64" - fd:%d (steps:%u) crc:%p (steps:%u) block:%p,blockno:%u (steps:%u)\n",cc->chunkid,c->fd,c->opensteps,c->crc,c->crcsteps,c->block,c->blockno,c->blocksteps);
#else /* PRESERVE_BLOCK */
				printf("id: %"PRIu64" - fd:%d (steps:%u) crc:%p (steps:%u)\n",cc->chunkid,c->fd,c->opensteps,c->crc,c->crcsteps);
#endif /* PRESERVE_BLOCK */
				hdd_chunk_release(c);
			}
		}
	}
#ifdef _THREAD_SAFE
	printf("unlock doplock\n");
	if (pthread_mutex_unlock(&doplock)<0) {
		printf("unlock error: %u\n",errno);
	}
#endif
}

void hdd_delayed_ops() {
	dopchunk **ccp,*cc,*tcc;
	uint32_t dhashpos;
	chunk *c;
//	int status;
//	printf("delayed ops: before lock\n");
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&doplock);
	pthread_mutex_lock(&ndoplock);
#endif
//	printf("delayed ops: after lock\n");
/* append new chunks */
	cc = newdopchunks;
	while (cc) {
		dhashpos = DHASHPOS(cc->chunkid);
		for (tcc=dophashtab[dhashpos] ; tcc && tcc->chunkid!=cc->chunkid ; tcc=tcc->next) {}
		if (tcc) {	// found - ignore
			tcc = cc;
			cc = cc->next;
			free(tcc);
		} else {	// not found - add
			tcc = cc;
			cc = cc->next;
			tcc->next = dophashtab[dhashpos];
			dophashtab[dhashpos] = tcc;
		}
	}
	newdopchunks=NULL;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&ndoplock);
#endif
/* check all */
//	printf("delayed ops: before loop\n");
	for (dhashpos=0 ; dhashpos<DHASHSIZE ; dhashpos++) {
		ccp = dophashtab+dhashpos;
		while ((cc=*ccp)) {
//			printf("find chunk: %llu\n",cc->chunkid);
			c = hdd_chunk_tryfind(cc->chunkid);
//			if (c!=NULL && c!=CHUNKLOCKED) {
//				printf("found chunk: %llu (c->state:%u c->crcrefcount:%u)\n",cc->chunkid,c->state,c->crcrefcount);
//			}
//			c = hdd_chunk_find(cc->chunkid);
			if (c==NULL) {	// no chunk - delete entry
				*ccp = cc->next;
				free(cc);
			} else if (c==CHUNKLOCKED) {	// locked chunk - just ignore
				ccp = &(cc->next);
			} else if (c->crcrefcount>0) {	// io in progress - skip entry
				hdd_chunk_release(c);
				ccp = &(cc->next);
			} else {
#ifdef PRESERVE_BLOCK
//				printf("block\n");
				if (c->blocksteps>0) {
					c->blocksteps--;
				} else if (c->block!=NULL) {
					free(c->block);
					c->block = NULL;
					c->blockno = 0xFFFF;
				}
#endif /* PRESERVE_BLOCK */
//				printf("descriptor\n");
				if (c->opensteps>0) {	// decrease counter
					c->opensteps--;
				} else if (c->fd>=0) {	// close descriptor
					if (close(c->fd)<0) {
						syslog(LOG_WARNING,"hdd_delayed_ops: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
						hdd_error_occured(c);
						hdd_report_damaged_chunk(c->chunkid);
					}
					c->fd = -1;
				}
//				printf("crc\n");
				if (c->crcsteps>0) {	// decrease counter
					c->crcsteps--;
				} else if (c->crc!=NULL) {	// free crc block
					if (c->crcchanged) {
						syslog(LOG_ERR,"serious error: crc changes lost (chunk:%016"PRIX64"_%08"PRIX32")",c->chunkid,c->version);
					}
//					printf("chunk %llu - free crc record\n",c->chunkid);
					chunk_freecrc(c);
				}
#ifdef PRESERVE_BLOCK
				if (c->fd<0 && c->crc==NULL && c->block==NULL) {
#else /* PRESERVE_BLOCK */
				if (c->fd<0 && c->crc==NULL) {
#endif /* PRESERVE_BLOCK */
					*ccp = cc->next;
					free(cc);
				} else {
					ccp = &(cc->next);
				}
				hdd_chunk_release(c);
			}
		}
	}
//	printf("delayed ops: after loop , before unlock\n");
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&doplock);
#endif
//	printf("delayed ops: after unlock\n");
}

static int hdd_io_begin(chunk *c,int newflag) {
	dopchunk *cc;
	int status;
	int add;
//	syslog(LOG_NOTICE,"chunk: %"PRIu64" - before io",c->chunkid);
	if (c->crcrefcount==0) {
#ifdef PRESERVE_BLOCK
		add = (c->fd<0 && c->crc==NULL && c->block==NULL);
#else /* PRESERVE_BLOCK */
		add = (c->fd<0 && c->crc==NULL);
#endif /* PRESERVE_BLOCK */
		if (c->fd<0) {
			if (newflag) {
				c->fd = open(c->filename,O_RDWR | O_TRUNC | O_CREAT,0666);
			} else {
				c->fd = open(c->filename,O_RDWR);
			}
			if (c->fd<0) {
				syslog(LOG_WARNING,"hdd_io_begin: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
				return ERROR_IO;
			}
		}
		if (c->crc==NULL) {
			if (newflag) {
				chunk_emptycrc(c);
			} else {
				status = chunk_readcrc(c);
				if (status!=STATUS_OK) {
					if (add) {
						close(c->fd);
						c->fd=-1;
					}
					syslog(LOG_WARNING,"hdd_io_begin: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
					return status;
				}
			}
			c->crcchanged=0;
		}
#ifdef PRESERVE_BLOCK
		if (c->block==NULL) {
			c->block = malloc(0x10000);
			c->blockno = 0xFFFF;
		}
#endif /* PRESERVE_BLOCK */
		if (add) {
			cc = malloc(sizeof(dopchunk));
			cc->chunkid = c->chunkid;
#ifdef _THREAD_SAFE
			pthread_mutex_lock(&ndoplock);
#endif
			cc->next = newdopchunks;
			newdopchunks = cc;
#ifdef _THREAD_SAFE
			pthread_mutex_unlock(&ndoplock);
#endif
		}
	}
	c->crcrefcount++;
#ifdef _THREAD_SAFE
	hdd_chunk_testmove(c);
#endif
	return STATUS_OK;
}

static int hdd_io_end(chunk *c) {
	int status;
//	syslog(LOG_NOTICE,"chunk: %"PRIu64" - after io",c->chunkid);
	if (c->crcchanged) {
		status = chunk_writecrc(c);
		c->crcchanged=0;
		if (status!=STATUS_OK) {
			syslog(LOG_WARNING,"hdd_io_end: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
			return status;
		}
	}
	c->crcrefcount--;
	if (c->crcrefcount==0) {
		if (OPENSTEPS==0) {
			if (close(c->fd)<0) {
				c->fd = -1;
				syslog(LOG_WARNING,"hdd_io_end: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
				return ERROR_IO;
			}
			c->fd = -1;
		} else {
#ifdef F_FULLFSYNC
//			printf("afterio - FULLFSYNC (desc: %d)\n",c->fd);
			if (fcntl(c->fd,F_FULLFSYNC)<0) {
				syslog(LOG_WARNING,"hdd_io_end: file:%s - fsync (via fcntl) error (%d:%s)",c->filename,errno,strerror(errno));
				return ERROR_IO;
			}
//			printf("afterio - synced (desc: %d)\n",c->fd);
#else
			if (fsync(c->fd)<0) {
				syslog(LOG_WARNING,"hdd_io_end: file:%s - fsync (direct call) error (%d:%s)",c->filename,errno,strerror(errno));
				return ERROR_IO;
			}
#endif
			c->opensteps = OPENSTEPS;
		}
		c->crcsteps = CRCSTEPS;
#ifdef PRESERVE_BLOCK
		c->blocksteps = PRESERVESTEPS;
#endif
	}
	return STATUS_OK;
}

static inline uint64_t get_msectime() {
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return ((uint64_t)(tv.tv_sec))*1000000+tv.tv_usec;
}



/* I/O operations */

int hdd_open(uint64_t chunkid) {
	int status;
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_report_damaged_chunk(chunkid);
	}
	hdd_chunk_release(c);
//	if (status==STATUS_OK) {
//		syslog(LOG_NOTICE,"chunk %08"PRIX64" opened",chunkid);
//	}
	return status;
}

int hdd_close(uint64_t chunkid) {
	int status;
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_report_damaged_chunk(chunkid);
	}
	hdd_chunk_release(c);
//	if (status==STATUS_OK) {
//		syslog(LOG_NOTICE,"chunk %08"PRIX64" closed",chunkid);
//	}
	return status;
}

int hdd_read(uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff) {
	chunk *c;
	int ret;
	const uint8_t *rcrcptr;
	uint32_t crc,bcrc,precrc,postcrc,combinedcrc;
	uint64_t ts,te;
#ifdef _THREAD_SAFE
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer=malloc(0x10000);
		pthread_setspecific(blockbufferkey,blockbuffer);
	}
#endif /* PRESERVE_BLOCK */
#endif /* _THREAD_SAFE */
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=0x400) {
		hdd_chunk_release(c);
		return ERROR_BNUMTOOBIG;
	}
	if (size>0x10000) {
		hdd_chunk_release(c);
		return ERROR_WRONGSIZE;
	}
	if ((offset>=0x10000) || (offset+size>0x10000)) {
		hdd_chunk_release(c);
		return ERROR_WRONGOFFSET;
	}
	if (blocknum>=c->blocks) {
		memset(buffer,0,size);
		if (size==0x10000) {
			crc = emptyblockcrc;
		} else {
			crc = mycrc32_zeroblock(0,size);
		}
		put32bit(&crcbuff,crc);
		hdd_chunk_release(c);
		return STATUS_OK;
	}
	if (offset==0 && size==0x10000) {
#ifdef PRESERVE_BLOCK
		if (c->blockno==blocknum) {
			memcpy(buffer,c->block,0x10000);
			ret = 0x10000;
		} else {
#endif /* PRESERVE_BLOCK */
		ts = get_msectime();
#ifdef USE_PIO
		ret = pread(c->fd,buffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
		ret = read(c->fd,buffer,0x10000);
#endif /* USE_PIO */
		te = get_msectime();
		hdd_stats_dataread(0x10000,te-ts);
#ifdef PRESERVE_BLOCK
			c->blockno = blocknum;
			memcpy(c->block,buffer,0x10000);
		}
#endif /* PRESERVE_BLOCK */
		crc = mycrc32(0,buffer,0x10000);
		rcrcptr = (c->crc)+(4*blocknum);
		bcrc = get32bit(&rcrcptr);
		if (bcrc!=crc) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		if (ret!=0x10000) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
	} else {
#ifdef PRESERVE_BLOCK
		if (c->blockno != blocknum) {
			ts = get_msectime();
#ifdef USE_PIO
			ret = pread(c->fd,c->block,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else /* USE_PIO */
			lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
			ret = read(c->fd,c->block,0x10000);
#endif /* USE_PIO */
			te = get_msectime();
			hdd_stats_dataread(0x10000,te-ts);
			c->blockno = blocknum;
		} else {
			ret = 0x10000;
		}
		precrc = mycrc32(0,c->block,offset);
		crc = mycrc32(0,c->block+offset,size);
		postcrc = mycrc32(0,c->block+offset+size,0x10000-(offset+size));
#else /* PRESERVE_BLOCK */
		ts = get_msectime();
#ifdef USE_PIO
		ret = pread(c->fd,blockbuffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
		ret = read(c->fd,blockbuffer,0x10000);
#endif /* USE_PIO */
		te = get_msectime();
		hdd_stats_dataread(0x10000,te-ts);
//		crc = mycrc32(0,blockbuffer+offset,size);	// first calc crc for piece
		precrc = mycrc32(0,blockbuffer,offset);
		crc = mycrc32(0,blockbuffer+offset,size);
		postcrc = mycrc32(0,blockbuffer+offset+size,0x10000-(offset+size));
#endif /* PRESERVE_BLOCK */
		if (offset==0) {
			combinedcrc = mycrc32_combine(crc,postcrc,0x10000-(offset+size));
		} else {
			combinedcrc = mycrc32_combine(precrc,crc,size);
			if ((offset+size)<0x10000) {
				combinedcrc = mycrc32_combine(combinedcrc,postcrc,0x10000-(offset+size));
			}
		}
		rcrcptr = (c->crc)+(4*blocknum);
		bcrc = get32bit(&rcrcptr);
//		if (bcrc!=mycrc32(0,blockbuffer,0x10000)) {
		if (bcrc!=combinedcrc) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		if (ret!=0x10000) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		memcpy(buffer,c->block+offset,size);
#else /* PRESERVE_BLOCK */
		memcpy(buffer,blockbuffer+offset,size);
#endif /* PRESERVE_BLOCK */
	}
	put32bit(&crcbuff,crc);
	hdd_chunk_release(c);
	return STATUS_OK;
}

int hdd_write(uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff) {
	chunk *c;
	int ret;
	uint8_t *wcrcptr;
	const uint8_t *rcrcptr;
	uint32_t crc,bcrc,precrc,postcrc,combinedcrc,chcrc;
	uint32_t i;
	uint64_t ts,te;
#ifndef PRESERVE_BLOCK
#ifdef _THREAD_SAFE
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer=malloc(0x10000);
		pthread_setspecific(blockbufferkey,blockbuffer);
	}
#endif /* _THREAD_SAFE */
#endif /* PRESERVE_BLOCK */
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=0x400) {
		hdd_chunk_release(c);
		return ERROR_BNUMTOOBIG;
	}
	if (size>0x10000) {
		hdd_chunk_release(c);
		return ERROR_WRONGSIZE;
	}
	if ((offset>=0x10000) || (offset+size>0x10000)) {
		hdd_chunk_release(c);
		return ERROR_WRONGOFFSET;
	}
	crc = get32bit(&crcbuff);
	if (crc!=mycrc32(0,buffer,size)) {
		hdd_chunk_release(c);
		return ERROR_CRC;
	}
	if (offset==0 && size==0x10000) {
		if (blocknum>=c->blocks) {
			wcrcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				put32bit(&wcrcptr,emptyblockcrc);
			}
			c->blocks=blocknum+1;
		}
		ts = get_msectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,buffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
		ret = write(c->fd,buffer,0x10000);
#endif /* USE_PIO */
		te = get_msectime();
		hdd_stats_datawrite(0x10000,te-ts);
		if (crc!=mycrc32(0,buffer,0x10000)) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		wcrcptr = (c->crc)+(4*blocknum);
		put32bit(&wcrcptr,crc);
		c->crcchanged=1;
		if (ret!=0x10000) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		memcpy(c->block,buffer,0x10000);
		c->blockno = blocknum;
#endif /* PRESERVE_BLOCK */
	} else {
		if (blocknum<c->blocks) {
#ifdef PRESERVE_BLOCK
			if (c->blockno != blocknum) {
				ts = get_msectime();
#ifdef USE_PIO
				ret = pread(c->fd,c->block,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else /* USE_PIO */
				lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
				ret = read(c->fd,c->block,0x10000);
#endif /* USE_PIO */
				te = get_msectime();
				hdd_stats_dataread(0x10000,te-ts);
				c->blockno = blocknum;
			} else {
				ret = 0x10000;
			}
#else /* PRESERVE_BLOCK */
			ts = get_msectime();
#ifdef USE_PIO
			ret = pread(c->fd,blockbuffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else /* USE_PIO */
			lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
			ret = read(c->fd,blockbuffer,0x10000);
#endif /* USE_PIO */
			te = get_msectime();
			hdd_stats_dataread(0x10000,te-ts);
#endif /* PRESERVE_BLOCK */
			if (ret!=0x10000) {
				syslog(LOG_WARNING,"write_block_to_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_error_occured(c);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			precrc = mycrc32(0,c->block,offset);
			chcrc = mycrc32(0,c->block+offset,size);
			postcrc = mycrc32(0,c->block+offset+size,0x10000-(offset+size));
#else /* PRESERVE_BLOCK */
			precrc = mycrc32(0,blockbuffer,offset);
			chcrc = mycrc32(0,blockbuffer+offset,size);
			postcrc = mycrc32(0,blockbuffer+offset+size,0x10000-(offset+size));
#endif /* PRESERVE_BLOCK */
			if (offset==0) {
				combinedcrc = mycrc32_combine(chcrc,postcrc,0x10000-(offset+size));
			} else {
				combinedcrc = mycrc32_combine(precrc,chcrc,size);
				if ((offset+size)<0x10000) {
					combinedcrc = mycrc32_combine(combinedcrc,postcrc,0x10000-(offset+size));
				}
			}
			rcrcptr = (c->crc)+(4*blocknum);
			bcrc = get32bit(&rcrcptr);
//			if (bcrc!=mycrc32(0,blockbuffer,0x10000)) {
			if (bcrc!=combinedcrc) {
				syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
				hdd_error_occured(c);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_CRC;
			}
		} else {
			if (ftruncate(c->fd,CHUNKHDRSIZE+(((uint32_t)(blocknum+1))<<16))<0) {
				syslog(LOG_WARNING,"write_block_to_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_error_occured(c);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
			wcrcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				put32bit(&wcrcptr,emptyblockcrc);
			}
			c->blocks=blocknum+1;
#ifdef PRESERVE_BLOCK
			memset(c->block,0,0x10000);
			c->blockno = blocknum;
#else /* PRESERVE_BLOCK */
			memset(blockbuffer,0,0x10000);
#endif /* PRESERVE_BLOCK */
			precrc = mycrc32_zeroblock(0,offset);
			postcrc = mycrc32_zeroblock(0,0x10000-(offset+size));
		}
#ifdef PRESERVE_BLOCK
		memcpy(c->block+offset,buffer,size);
		ts = get_msectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,c->block+offset,size,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset);
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset,SEEK_SET);
		ret = write(c->fd,c->block+offset,size);
#endif /* USE_PIO */
		te = get_msectime();
		hdd_stats_datawrite(size,te-ts);
		chcrc = mycrc32(0,c->block+offset,size);
#else /* PRESERVE_BLOCK */
		memcpy(blockbuffer+offset,buffer,size);
		ts = get_msectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,blockbuffer+offset,size,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset);
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset,SEEK_SET);
		ret = write(c->fd,blockbuffer+offset,size);
#endif /* USE_PIO */
		te = get_msectime();
		hdd_stats_datawrite(size,te-ts);
		chcrc = mycrc32(0,blockbuffer+offset,size);
#endif /* PRESERVE_BLOCK */
		if (offset==0) {
			combinedcrc = mycrc32_combine(chcrc,postcrc,0x10000-(offset+size));
		} else {
			combinedcrc = mycrc32_combine(precrc,chcrc,size);
			if ((offset+size)<0x10000) {
				combinedcrc = mycrc32_combine(combinedcrc,postcrc,0x10000-(offset+size));
			}
		}
		wcrcptr = (c->crc)+(4*blocknum);
//		bcrc = mycrc32(0,blockbuffer,0x10000);
//		put32bit(&wcrcptr,bcrc);
		put32bit(&wcrcptr,combinedcrc);
		c->crcchanged=1;
//		if (crc!=mycrc32(0,blockbuffer+offset,size)) {
		if (crc!=chcrc) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		if (ret!=(int)size) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}



/* chunk info */

int hdd_check_version(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}

int hdd_get_blocks(uint64_t chunkid,uint32_t version,uint16_t *blocks) {
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	*blocks = c->blocks;
	hdd_chunk_release(c);
	return STATUS_OK;
}

int hdd_get_checksum(uint64_t chunkid,uint32_t version,uint32_t *checksum) {
	int status;
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(c);
		return status;
	}
	*checksum = mycrc32(0,c->crc,4096);
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}

int hdd_get_checksum_tab(uint64_t chunkid,uint32_t version,uint8_t *checksum_tab) {
	int status;
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(c);
		return status;
	}
	memcpy(checksum_tab,c->crc,4096);
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}





/* chunk operations */

static int hdd_int_create(uint64_t chunkid,uint32_t version) {
	chunk *c;
	int status;
	uint8_t *ptr;
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKHDRSIZE];
#else /* PRESERVE_BLOCK */
#ifdef _THREAD_SAFE
	uint8_t *hdrbuffer;
#endif
#endif /* PRESERVE_BLOCK */

#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	if (bestfolder==NULL) {
#ifdef _THREAD_SAFE
		pthread_mutex_unlock(&folderlock);
		return ERROR_NOSPACE;
#endif
	}
	c = hdd_chunk_create(bestfolder,chunkid,version);
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
	if (c==NULL) {
		return ERROR_CHUNKEXIST;
	}

#ifndef PRESERVE_BLOCK
#ifdef _THREAD_SAFE
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer=malloc(CHUNKHDRSIZE);
		pthread_setspecific(hdrbufferkey,hdrbuffer);
	}
#endif
#endif /* PRESERVE_BLOCK */

	status = hdd_io_begin(c,1);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		return ERROR_IO;
	}
	memset(hdrbuffer,0,CHUNKHDRSIZE);
	memcpy(hdrbuffer,"MFSC 1.0",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (write(c->fd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"create_newchunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_io_end(c);
		unlink(c->filename);
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		return ERROR_IO;
	}
	hdd_stats_write(CHUNKHDRSIZE);
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		unlink(c->filename);
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		return status;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}

static int hdd_int_test(uint64_t chunkid,uint32_t version) {
	const uint8_t *ptr;
	uint16_t block;
	uint32_t bcrc;
	int32_t retsize;
	int status;
	chunk *c;
#ifndef PRESERVE_BLOCK
#ifdef _THREAD_SAFE
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer=malloc(0x10000);
		pthread_setspecific(blockbufferkey,blockbuffer);
	}
#endif /* _THREAD_SAFE */
#endif /* PRESERVE_BLOCK */
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_chunk_release(c);
		return status;
	}
	lseek(c->fd,CHUNKHDRSIZE,SEEK_SET);
	ptr = c->crc;
	for (block=0 ; block<c->blocks ; block++) {
#ifdef PRESERVE_BLOCK
		retsize = read(c->fd,c->block,0x10000);
#else /* PRESERVE_BLOCK */
		retsize = read(c->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
		if (retsize!=0x10000) {
			syslog(LOG_WARNING,"test_chunk: file:%s - data read error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_io_end(c);
			hdd_error_occured(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		hdd_stats_read(0x10000);
#ifdef PRESERVE_BLOCK
		c->blockno = block;
#endif
		bcrc = get32bit(&ptr);
#ifdef PRESERVE_BLOCK
		if (bcrc!=mycrc32(0,c->block,0x10000)) {
#else /* PRESERVE_BLOCK */
		if (bcrc!=mycrc32(0,blockbuffer,0x10000)) {
#endif /* PRESERVE_BLOCK */
			syslog(LOG_WARNING,"test_chunk: file:%s - crc error",c->filename);
			hdd_io_end(c);
			hdd_error_occured(c);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}

static int hdd_int_duplicate(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion) {
	uint32_t filenameleng;
	char *newfilename;
	uint8_t *ptr,vbuff[4];
	uint16_t block;
	int32_t retsize;
	int status;
	chunk *c,*oc;
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKHDRSIZE];
#else /* PRESERVE_BLOCK */
#ifdef _THREAD_SAFE
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer=malloc(0x10000);
		pthread_setspecific(blockbufferkey,blockbuffer);
	}
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer=malloc(CHUNKHDRSIZE);
		pthread_setspecific(hdrbufferkey,hdrbuffer);
	}
#endif
#endif /* PRESERVE_BLOCK */

	oc = hdd_chunk_find(chunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->version!=version && version>0) {
		hdd_chunk_release(oc);
		return ERROR_WRONGVERSION;
	}
	if (copyversion==0) {
		copyversion=newversion;
	}
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	if (bestfolder==NULL) {
#ifdef _THREAD_SAFE
		pthread_mutex_unlock(&folderlock);
#endif
		hdd_chunk_release(oc);
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(bestfolder,copychunkid,copyversion);
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
	if (c==NULL) {
		hdd_chunk_release(oc);
		return ERROR_CHUNKEXIST;
	}

	if (newversion!=version) {
		filenameleng = strlen(oc->filename);
		if (oc->filename[filenameleng-13]=='_') {	// new file name format
			newfilename = malloc(filenameleng+1);
			if (newfilename==NULL) {
				hdd_chunk_delete(c);
				hdd_chunk_release(oc);
				return ERROR_OUTOFMEMORY;
			}
			memcpy(newfilename,c->filename,filenameleng+1);
			sprintf(newfilename+filenameleng-12,"%08"PRIX32".mfs",newversion);
			if (rename(oc->filename,newfilename)<0) {
				syslog(LOG_WARNING,"duplicate_chunk: file:%s - rename error (%d:%s)",oc->filename,errno,strerror(errno));
				free(newfilename);
				hdd_chunk_delete(c);
				hdd_error_occured(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			free(oc->filename);
			oc->filename = newfilename;
		}
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_chunk_delete(c);
			hdd_error_occured(oc);
			hdd_chunk_release(oc);
			return status;	//can't change file version
		}
		ptr = vbuff;
		put32bit(&ptr,newversion);
#ifdef USE_PIO
		if (pwrite(oc->fd,vbuff,4,16)!=4) {
#else /* USE_PIO */
		lseek(oc->fd,16,SEEK_SET);
		if (write(oc->fd,vbuff,4)!=4) {
#endif /* USE_PIO */
			syslog(LOG_WARNING,"duplicate_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_error_occured(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		hdd_stats_write(4);
		oc->version = newversion;
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_chunk_delete(c);
			hdd_error_occured(oc);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(oc);
			return status;
		}
	}
	status = hdd_io_begin(c,1);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return status;
	}
	memset(hdrbuffer,0,CHUNKHDRSIZE);
	memcpy(hdrbuffer,"MFSC 1.0",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	memcpy(c->crc,oc->crc,4096);
	memcpy(hdrbuffer+1024,oc->crc,4096);
	if (write(c->fd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"duplicate_chunk: file:%s - hdr write error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_io_end(c);
		unlink(c->filename);
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return ERROR_IO;
	}
	hdd_stats_write(CHUNKHDRSIZE);
#ifndef PRESERVE_BLOCK
	lseek(oc->fd,CHUNKHDRSIZE,SEEK_SET);
#endif /* PRESERVE_BLOCK */
	for (block=0 ; block<oc->blocks ; block++) {
#ifdef PRESERVE_BLOCK
		if (oc->blockno==block) {
			memcpy(c->block,oc->block,0x10000);
			retsize = 0x10000;
		} else {
#ifdef USE_PIO
			retsize = pread(oc->fd,c->block,0x10000,CHUNKHDRSIZE+(((uint32_t)block)<<16));
#else /* USE_PIO */
			lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<16),SEEK_SET);
			retsize = read(oc->fd,c->block,0x10000);
#endif /* USE_PIO */
		}
#else /* PRESERVE_BLOCK */
		retsize = read(oc->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
		if (retsize!=0x10000) {
			syslog(LOG_WARNING,"duplicate_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
			hdd_io_end(c);
			unlink(c->filename);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_error_occured(oc);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		if (oc->blockno!=block) {
			hdd_stats_read(0x10000);
		}
		retsize = write(c->fd,c->block,0x10000);
#else /* PRESERVE_BLOCK */
		hdd_stats_read(0x10000);
		retsize = write(c->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
		if (retsize!=0x10000) {
			syslog(LOG_WARNING,"duplicate_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_io_end(c);
			unlink(c->filename);
			hdd_error_occured(c);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;	//write error
		}
		hdd_stats_write(0x10000);
#ifdef PRESERVE_BLOCK
		c->blockno = block;
#endif /* PRESERVE_BLOCK */
	}
	status = hdd_io_end(oc);
	if (status!=STATUS_OK) {
		hdd_io_end(c);
		unlink(c->filename);
		hdd_error_occured(oc);
		hdd_chunk_delete(c);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		unlink(c->filename);
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = oc->blocks;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	c->owner->needrefresh = 1;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
	hdd_chunk_release(c);
	hdd_chunk_release(oc);
	return STATUS_OK;
}

static int hdd_int_version(uint64_t chunkid,uint32_t version,uint32_t newversion) {
	int status;
	uint32_t filenameleng;
	char *newfilename;
	uint8_t *ptr,vbuff[4];
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	filenameleng = strlen(c->filename);
	if (c->filename[filenameleng-13]=='_') {	// new file name format
		newfilename = malloc(filenameleng+1);
		if (newfilename==NULL) {
			hdd_chunk_release(c);
			return ERROR_OUTOFMEMORY;
		}
		memcpy(newfilename,c->filename,filenameleng+1);
		sprintf(newfilename+filenameleng-12,"%08"PRIX32".mfs",newversion);
		if (rename(c->filename,newfilename)<0) {
			syslog(LOG_WARNING,"set_chunk_version: file:%s - rename error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			free(newfilename);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		free(c->filename);
		c->filename = newfilename;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		syslog(LOG_WARNING,"set_chunk_version: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		hdd_chunk_release(c);
		return status;
	}
	ptr = vbuff;
	put32bit(&ptr,newversion);
#ifdef USE_PIO
	if (pwrite(c->fd,vbuff,4,16)!=4) {
#else /* USE_PIO */
	lseek(c->fd,16,SEEK_SET);
	if (write(c->fd,vbuff,4)!=4) {
#endif /* USE_PIO */
		syslog(LOG_WARNING,"set_chunk_version: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_io_end(c);
		hdd_error_occured(c);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	hdd_stats_write(4);
	c->version = newversion;
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
	}
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_truncate(uint64_t chunkid,uint32_t version,uint32_t newversion,uint32_t length) {
	int status;
	uint32_t filenameleng;
	char *newfilename;
	uint8_t *ptr,vbuff[4];
	chunk *c;
	uint32_t blocks;
	uint32_t i;
#ifdef _THREAD_SAFE
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer=malloc(0x10000);
		pthread_setspecific(blockbufferkey,blockbuffer);
	}
#endif
#endif
	if (length>0x4000000) {
		return ERROR_WRONGSIZE;
	}
	c = hdd_chunk_find(chunkid);
	// step 1 - change version
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	filenameleng = strlen(c->filename);
	if (c->filename[filenameleng-13]=='_') {	// new file name format
		newfilename = malloc(filenameleng+1);
		if (newfilename==NULL) {
			hdd_chunk_release(c);
			return ERROR_OUTOFMEMORY;
		}
		memcpy(newfilename,c->filename,filenameleng+1);
		sprintf(newfilename+filenameleng-12,"%08"PRIX32".mfs",newversion);
		if (rename(c->filename,newfilename)<0) {
			syslog(LOG_WARNING,"truncate_chunk: file:%s - rename error (%d:%s)",c->filename,errno,strerror(errno));
			free(newfilename);
			hdd_error_occured(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		free(c->filename);
		c->filename = newfilename;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_chunk_release(c);
		return status;	//can't change file version
	}
	ptr = vbuff;
	put32bit(&ptr,newversion);
#ifdef USE_PIO
	if (pwrite(c->fd,vbuff,4,16)!=4) {
#else /* USE_PIO */
	lseek(c->fd,16,SEEK_SET);
	if (write(c->fd,vbuff,4)!=4) {
#endif /* USE_PIO */
		syslog(LOG_WARNING,"truncate_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_io_end(c);
		hdd_error_occured(c);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	hdd_stats_write(4);
	c->version = newversion;
	// step 2. truncate
	blocks = ((length+0xFFFF)>>16);
	if (blocks>c->blocks) {
		if (ftruncate(c->fd,CHUNKHDRSIZE+(blocks<<16))<0) {
			syslog(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_io_end(c);
			hdd_error_occured(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		ptr = (c->crc)+(4*(c->blocks));
		for (i=c->blocks ; i<blocks ; i++) {
			put32bit(&ptr,emptyblockcrc);
		}
		c->crcchanged=1;
	} else {
		uint32_t blocknum = length>>16;
		uint32_t blockpos = length&0x3FF0000;
		uint32_t blocksize = length&0xFFFF;
		if (ftruncate(c->fd,CHUNKHDRSIZE+length)<0) {
			syslog(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_io_end(c);
			hdd_error_occured(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		if (blocksize>0) {
			if (ftruncate(c->fd,CHUNKHDRSIZE+(blocks<<16))<0) {
				syslog(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_io_end(c);
				hdd_error_occured(c);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (c->blockno>=blocks) {
				c->blockno=0xFFFF;	// invalidate truncated block
			}
			if (c->blockno!=(blockpos>>16)) {

#ifdef USE_PIO
				if (pread(c->fd,c->block,blocksize,CHUNKHDRSIZE+blockpos)!=(signed)blocksize) {
#else /* USE_PIO */
				lseek(c->fd,CHUNKHDRSIZE+blockpos,SEEK_SET);
				if (read(c->fd,c->block,blocksize)!=(signed)blocksize) {
#endif /* USE_PIO */
#else /* PRESERVE_BLOCK */
#ifdef USE_PIO
			if (pread(c->fd,blockbuffer,blocksize,CHUNKHDRSIZE+blockpos)!=(signed)blocksize) {
#else /* USE_PIO */
			lseek(c->fd,CHUNKHDRSIZE+blockpos,SEEK_SET);
			if (read(c->fd,blockbuffer,blocksize)!=(signed)blocksize) {
#endif /* USE_PIO */
#endif /* PRESERVE_BLOCK */
				syslog(LOG_WARNING,"truncate_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_io_end(c);
				hdd_error_occured(c);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
			hdd_stats_read(blocksize);
#ifdef PRESERVE_BLOCK
			}
			memset(c->block+blocksize,0,0x10000-blocksize);
			c->blockno = blockpos>>16;
			i = mycrc32_zeroexpanded(0,c->block,blocksize,0x10000-blocksize);
#else /* PRESERVE_BLOCK */
			i = mycrc32_zeroexpanded(0,blockbuffer,blocksize,0x10000-blocksize);
#endif /* PRESERVE_BLOCK */
			ptr = (c->crc)+(4*blocknum);
			put32bit(&ptr,i);
			c->crcchanged=1;
		}
	}
	if (c->blocks != blocks) {
#ifdef _THREAD_SAFE
		pthread_mutex_lock(&folderlock);
#endif
		c->owner->needrefresh = 1;
#ifdef _THREAD_SAFE
		pthread_mutex_unlock(&folderlock);
#endif
	}
	c->blocks=blocks;
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
	}
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_duptrunc(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
	uint32_t filenameleng;
	char *newfilename;
	uint8_t *ptr,vbuff[4];
	uint16_t block;
	uint16_t blocks; 
	int32_t retsize;
	uint32_t crc;
	int status;
	chunk *c,*oc;
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKHDRSIZE];
#else /* PRESERVE_BLOCK */
#ifdef _THREAD_SAFE
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer=malloc(0x10000);
		pthread_setspecific(blockbufferkey,blockbuffer);
	}
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer=malloc(CHUNKHDRSIZE);
		pthread_setspecific(hdrbufferkey,hdrbuffer);
	}
#endif
#endif /* PRESERVE_BLOCK */

	if (length>0x4000000) {
		return ERROR_WRONGSIZE;
	}
	oc = hdd_chunk_find(chunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->version!=version && version>0) {
		hdd_chunk_release(oc);
		return ERROR_WRONGVERSION;
	}
	if (copyversion==0) {
		copyversion=newversion;
	}
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	if (bestfolder==NULL) {
#ifdef _THREAD_SAFE
		pthread_mutex_unlock(&folderlock);
#endif
		hdd_chunk_release(oc);
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(bestfolder,copychunkid,copyversion);
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
	if (c==NULL) {
		hdd_chunk_release(oc);
		return ERROR_CHUNKEXIST;
	}

	if (newversion!=version) {
		filenameleng = strlen(oc->filename);
		if (oc->filename[filenameleng-13]=='_') {	// new file name format
			newfilename = malloc(filenameleng+1);
			if (newfilename==NULL) {
				hdd_chunk_delete(c);
				hdd_chunk_release(oc);
				return ERROR_OUTOFMEMORY;
			}
			memcpy(newfilename,c->filename,filenameleng+1);
			sprintf(newfilename+filenameleng-12,"%08"PRIX32".mfs",newversion);
			if (rename(oc->filename,newfilename)<0) {
				syslog(LOG_WARNING,"duplicate_chunk: file:%s - rename error (%d:%s)",oc->filename,errno,strerror(errno));
				free(newfilename);
				hdd_chunk_delete(c);
				hdd_error_occured(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			free(oc->filename);
			oc->filename = newfilename;
		}
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_chunk_delete(c);
			hdd_error_occured(oc);
			hdd_chunk_release(oc);
			return status;	//can't change file version
		}
		ptr = vbuff;
		put32bit(&ptr,newversion);
#ifdef USE_PIO
		if (pwrite(oc->fd,vbuff,4,16)!=4) {
#else /* USE_PIO */
		lseek(oc->fd,16,SEEK_SET);
		if (write(oc->fd,vbuff,4)!=4) {
#endif /* USE_PIO */
			syslog(LOG_WARNING,"duptrunc_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_error_occured(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		hdd_stats_write(4);
		oc->version = newversion;
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_chunk_delete(c);
			hdd_error_occured(oc);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(oc);
			return status;
		}
	}
	status = hdd_io_begin(c,1);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return status;
	}
	blocks = ((length+0xFFFF)>>16);
	memset(hdrbuffer,0,CHUNKHDRSIZE);
	memcpy(hdrbuffer,"MFSC 1.0",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	memcpy(hdrbuffer+1024,oc->crc,4096);
// do not write header yet - only seek to apriopriate position
	lseek(c->fd,CHUNKHDRSIZE,SEEK_SET);
#ifndef PRESERVE_BLOCK
	lseek(oc->fd,CHUNKHDRSIZE,SEEK_SET);
#endif /* PRESERVE_BLOCK */
	if (blocks>oc->blocks) { // expanding
		for (block=0 ; block<oc->blocks ; block++) {
#ifdef PRESERVE_BLOCK
			if (oc->blockno==block) {
				memcpy(c->block,oc->block,0x10000);
				retsize = 0x10000;
			} else {
#ifdef USE_PIO
				retsize = pread(oc->fd,c->block,0x10000,CHUNKHDRSIZE+(((uint32_t)block)<<16));
#else /* USE_PIO */
				lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<16),SEEK_SET);
				retsize = read(oc->fd,c->block,0x10000);
#endif /* USE_PIO */
			}
#else /* PRESERVE_BLOCK */
			retsize = read(oc->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
			if (retsize!=0x10000) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
				hdd_io_end(c);
				unlink(c->filename);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_error_occured(oc);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (oc->blockno!=block) {
				hdd_stats_read(0x10000);
			}
			retsize = write(c->fd,c->block,0x10000);
#else /* PRESERVE_BLOCK */
			hdd_stats_read(0x10000);
			retsize = write(c->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
			if (retsize!=0x10000) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_io_end(c);
				unlink(c->filename);
				hdd_error_occured(c);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_write(0x10000);
#ifdef PRESERVE_BLOCK
			c->blockno = block;
#endif /* PRESERVE_BLOCK */
		}
		if (ftruncate(c->fd,CHUNKHDRSIZE+(((uint32_t)blocks)<<16))<0) {
			syslog(LOG_WARNING,"duptrunc_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_io_end(c);
			unlink(c->filename);
			hdd_error_occured(c);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;	//write error
		}
		ptr = hdrbuffer+CHUNKHDRCRC+4*(oc->blocks);
		for (block=oc->blocks ; block<blocks ; block++) {
			put32bit(&ptr,emptyblockcrc);
		}
	} else { // shrinking
		uint32_t blocksize = (length&0xFFFF);
		if (blocksize==0) { // aligned shring
			for (block=0 ; block<blocks ; block++) {
#ifdef PRESERVE_BLOCK
				if (oc->blockno==block) {
					memcpy(c->block,oc->block,0x10000);
					retsize = 0x10000;
				} else {
#ifdef USE_PIO
					retsize = pread(oc->fd,c->block,0x10000,CHUNKHDRSIZE+(((uint32_t)block)<<16));
#else /* USE_PIO */
					lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<16),SEEK_SET);
					retsize = read(oc->fd,c->block,0x10000);
#endif /* USE_PIO */
				}
#else /* PRESERVE_BLOCK */
				retsize = read(oc->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
					hdd_io_end(c);
					unlink(c->filename);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_error_occured(oc);
					hdd_report_damaged_chunk(chunkid);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
#ifdef PRESERVE_BLOCK
				if (oc->blockno!=block) {
					hdd_stats_read(0x10000);
				}
				retsize = write(c->fd,c->block,0x10000);
#else /* PRESERVE_BLOCK */
				hdd_stats_read(0x10000);
				retsize = write(c->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
					hdd_io_end(c);
					unlink(c->filename);
					hdd_error_occured(c);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
				hdd_stats_write(0x10000);
#ifdef PRESERVE_BLOCK
				c->blockno = block;
#endif /* PRESERVE_BLOCK */
			}
		} else { // misaligned shrink
			for (block=0 ; block<blocks-1 ; block++) {
#ifdef PRESERVE_BLOCK
				if (oc->blockno==block) {
					memcpy(c->block,oc->block,0x10000);
					retsize = 0x10000;
				} else {
#ifdef USE_PIO
					retsize = pread(oc->fd,c->block,0x10000,CHUNKHDRSIZE+(((uint32_t)block)<<16));
#else /* USE_PIO */
					lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<16),SEEK_SET);
					retsize = read(oc->fd,c->block,0x10000);
#endif /* USE_PIO */
				}
#else /* PRESERVE_BLOCK */
				retsize = read(oc->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
					hdd_io_end(c);
					unlink(c->filename);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_error_occured(oc);
					hdd_report_damaged_chunk(chunkid);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
#ifdef PRESERVE_BLOCK
				if (oc->blockno!=block) {
					hdd_stats_read(0x10000);
				}
				retsize = write(c->fd,c->block,0x10000);
#else /* PRESERVE_BLOCK */
				hdd_stats_read(0x10000);
				retsize = write(c->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
					hdd_io_end(c);
					unlink(c->filename);
					hdd_error_occured(c);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return ERROR_IO;	//write error
				}
				hdd_stats_write(0x10000);
			}
			block = blocks-1;
#ifdef PRESERVE_BLOCK
			if (oc->blockno==block) {
				memcpy(c->block,oc->block,blocksize);
				retsize = blocksize;
			} else {
#ifdef USE_PIO
				retsize = pread(oc->fd,c->block,blocksize,CHUNKHDRSIZE+(((uint32_t)block)<<16));
#else /* USE_PIO */
				lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<16),SEEK_SET);
				retsize = read(oc->fd,c->block,blocksize);
#endif /* USE_PIO */
			}
#else /* PRESERVE_BLOCK */
			retsize = read(oc->fd,blockbuffer,blocksize);
#endif /* PRESERVE_BLOCK */
			if (retsize!=(signed)blocksize) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
				hdd_io_end(c);
				unlink(c->filename);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_error_occured(oc);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (oc->blockno!=block) {
				hdd_stats_read(blocksize);
			}
			memset(c->block+blocksize,0,0x10000-blocksize);
			retsize = write(c->fd,c->block,0x10000);
#else /* PRESERVE_BLOCK */
			hdd_stats_read(blocksize);
			memset(blockbuffer+blocksize,0,0x10000-blocksize);
			retsize = write(c->fd,blockbuffer,0x10000);
#endif /* PRESERVE_BLOCK */
			if (retsize!=0x10000) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_io_end(c);
				unlink(c->filename);
				hdd_error_occured(c);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_write(0x10000);
			ptr = hdrbuffer+CHUNKHDRCRC+4*(blocks-1);
#ifdef PRESERVE_BLOCK
			crc = mycrc32_zeroexpanded(0,c->block,blocksize,0x10000-blocksize);
#else /* PRESERVE_BLOCK */
			crc = mycrc32_zeroexpanded(0,blockbuffer,blocksize,0x10000-blocksize);
#endif /* PRESERVE_BLOCK */
			put32bit(&ptr,crc);
#ifdef PRESERVE_BLOCK
			c->blockno = block;
#endif /* PRESERVE_BLOCK */
		}
	}
// and now write header
	memcpy(c->crc,hdrbuffer+1024,4096);
	lseek(c->fd,0,SEEK_SET);
	if (write(c->fd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - hdr write error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_io_end(c);
		unlink(c->filename);
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return ERROR_IO;
	}
	hdd_stats_write(CHUNKHDRSIZE);
	status = hdd_io_end(oc);
	if (status!=STATUS_OK) {
		hdd_io_end(c);
		unlink(c->filename);
		hdd_chunk_delete(c);
		hdd_error_occured(oc);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		unlink(c->filename);
		hdd_error_occured(c);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = blocks;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&folderlock);
#endif
	c->owner->needrefresh = 1;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&folderlock);
#endif
	hdd_chunk_release(c);
	hdd_chunk_release(oc);
	return STATUS_OK;
}

static int hdd_int_delete(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (unlink(c->filename)<0) {
		syslog(LOG_WARNING,"delete_chunk: file:%s - unlink error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	hdd_chunk_delete(c);
	return STATUS_OK;
}

/* all chunk operations in one call */
// newversion>0 && length==0xFFFFFFFF && copychunkid==0   -> change version
// newversion>0 && length==0xFFFFFFFF && copycnunkid>0    -> duplicate
// newversion>0 && length<=0x4000000 && copychunkid==0    -> truncate
// newversion>0 && length<=0x4000000 && copychunkid>0     -> duplicate and truncate
// newversion==0 && length==0                             -> delete
// newversion==0 && length==1                             -> create
// newversion==0 && length==2                             -> check chunk contents
int hdd_chunkop(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&statslock);
#endif
	if (newversion>0) {
		if (length==0xFFFFFFFF) {
			if (copychunkid==0) {
				stats_version++;
			} else {
				stats_duplicate++;
			}
		} else if (length<=0x4000000) {
			if (copychunkid==0) {
				stats_truncate++;
			} else {
				stats_duptrunc++;
			}
		}
	} else {
		if (length==0) {
			stats_delete++;
		} else if (length==1) {
			stats_create++;
		} else if (length==2) {
			stats_test++;
		}
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&statslock);
#endif
	if (newversion>0) {
		if (length==0xFFFFFFFF) {
			if (copychunkid==0) {
				return hdd_int_version(chunkid,version,newversion);
			} else {
				return hdd_int_duplicate(chunkid,version,newversion,copychunkid,copyversion);
			}
		} else if (length<=0x4000000) {
			if (copychunkid==0) {
				return hdd_int_truncate(chunkid,version,newversion,length);
			} else {
				return hdd_int_duptrunc(chunkid,version,newversion,copychunkid,copyversion,length);
			}
		} else {
			return ERROR_EINVAL;
		}
	} else {
		if (length==0) {
			return hdd_int_delete(chunkid,version);
		} else if (length==1) {
			return hdd_int_create(chunkid,version);
		} else if (length==2) {
			return hdd_int_test(chunkid,version);
		} else {
			return ERROR_EINVAL;
		}
	}
}

#ifdef _THREAD_SAFE
void* hdd_tester_thread(void* arg) {
	uint8_t foldersno = (unsigned long)arg;
	uint8_t nexttestid = 0;
	folder *f;
	chunk *c;
	uint64_t chunkid;
	uint32_t version;
	char *path;

	sleep(5);
	for (;;) {
		path=NULL;
		chunkid=0;
		version=0;
		pthread_mutex_lock(&folderlock);
		pthread_mutex_lock(&hashlock);
		pthread_mutex_lock(&testlock);
		for (f=folderhead ; f ; f=f->next) {
			if (f->testid==nexttestid) {
				c = f->testhead;
				if (c && c->state==CH_AVAIL) {
					chunkid = c->chunkid;
					version = c->version;
					path = strdup(c->filename);
				}
			}
		}
		pthread_mutex_unlock(&testlock);
		pthread_mutex_unlock(&hashlock);
		pthread_mutex_unlock(&folderlock);
		nexttestid = (nexttestid+1)%foldersno;
		if (path) {
			syslog(LOG_NOTICE,"testing chunk: %s",path);
			if (hdd_int_test(chunkid,version)!=STATUS_OK) {
				hdd_report_damaged_chunk(chunkid);
			}
			free(path);
			sleep(HDDTestFreq);
		} else {
			sleep(1);
		}
	}
	return NULL;
}

int hdd_testcompare(const void *a,const void *b) {
	chunk const* *aa = (chunk const* *)a;
	chunk const* *bb = (chunk const* *)b;
	return (**aa).testtime - (**bb).testtime;
}

void hdd_testsort(folder *f) {
	uint32_t i,chunksno;
	chunk **csorttab,*c;
	pthread_mutex_lock(&testlock);
	chunksno=0;
	for (c=f->testhead ; c ; c=c->testnext) {
		chunksno++;
	}
	csorttab = malloc(sizeof(chunk*)*chunksno);
	chunksno=0;
	for (c=f->testhead ; c ; c=c->testnext) {
		csorttab[chunksno++]=c;
	}
	qsort(csorttab,chunksno,sizeof(chunk*),hdd_testcompare);
	f->testhead = NULL;
	f->testtail = &(f->testhead);
	for (i=0 ; i<chunksno ; i++) {
		c=csorttab[i];
		c->testnext = NULL;
		c->testprev = f->testtail;
		*(c->testprev) = c;
		f->testtail = &(c->testnext);
	}
	free(csorttab);
	pthread_mutex_unlock(&testlock);
}
#endif

/* initialization */

static void* hdd_folder_scan(void *arg) {
	folder *f = (folder*)arg;
	DIR *dd;
	struct dirent *de;
	struct stat sb;
	int fd;
	uint8_t subf;
	chunk *c;
	char *fullname,*oldfullname;
	int nameok;
	char ch;
	uint32_t i;
	uint8_t plen;
//	uint8_t *ptr,buff[1024];
	uint64_t namechunkid;
	uint32_t nameversion;
	folder *prevf;

	plen = strlen(f->path);
	fullname = malloc(plen+38);
	oldfullname = malloc(plen+29);

	memcpy(fullname,f->path,plen);
	fullname[plen]='\0';
	mkdir(fullname,0755);

	fullname[plen++]='_';
	fullname[plen++]='/';
	fullname[plen]='\0';
	for (subf=0 ; subf<16 ; subf++) {
		if (subf<10) {
			fullname[plen-2]='0'+subf;
		} else {
			fullname[plen-2]='A'+(subf-10);
		}
		fullname[plen]='\0';
		mkdir(fullname,0755);
		dd = opendir(fullname);
		if (dd==NULL) {
			continue;
		}
		while ((de = readdir(dd)) != NULL) {
#ifdef HAVE_STRUCT_DIRENT_D_TYPE
			if (de->d_type != DT_REG) {
				continue;
			}
#endif
			// old name format: "chunk_XXXXXXXXXXXXXXXX.mfs"
			// new name format: "chunk_XXXXXXXXXXXXXXXX_YYYYYYYY.mfs" (X - chunkid ; Y - version)
			if (strncmp(de->d_name,"chunk_",6)!=0) {
				continue;
			}
			namechunkid = 0;
			nameversion = 0;
			nameok=1;
			for (i=6 ; i<22 && nameok==1 ; i++) {
				ch = de->d_name[i];
				if (ch>='0' && ch<='9') {
					ch-='0';
				} else if (ch>='A' && ch<='F') {
					ch-='A'-10;
				} else {
					ch=0;
					nameok=0;
				}
				namechunkid*=16;
				namechunkid+=ch;
			}
			if (nameok==0) {
				continue;
			}
			if (de->d_name[22]=='_') {	// new name
				for (i=23 ; i<31 && nameok==1 ; i++) {
					ch = de->d_name[i];
					if (ch>='0' && ch<='9') {
						ch-='0';
					} else if (ch>='A' && ch<='F') {
						ch-='A'-10;
					} else {
						ch=0;
						nameok=0;
					}
					nameversion*=16;
					nameversion+=ch;
				}
				if (nameok==0) {
					continue;
				}
				if (strcmp(de->d_name+31,".mfs")!=0) {
					continue;
				}
				nameok = 2;
			} else if (de->d_name[22]=='.') {	// old name
				if (strcmp(de->d_name+23,"mfs")!=0) {
					continue;
				}
			} else {
				continue;
			}
			if (nameok==2) {
				memcpy(fullname+plen,de->d_name,36);
			} else {
				const uint8_t *ptr;
				uint8_t hdr[20];
				uint64_t hdrchunkid;
				memcpy(oldfullname,fullname,plen);
				memcpy(oldfullname+plen,de->d_name,27);
				fd = open(oldfullname,O_RDONLY);
				if (fd<0) {
					continue;
				}
				if (read(fd,hdr,20)!=20) {
					close(fd);
					unlink(oldfullname);
					continue;
				}
				close(fd);
				if (memcmp(hdr,"MFSC 1.0",8)!=0) {
					unlink(oldfullname);
					continue;
				}
				ptr = hdr+8;
				hdrchunkid = get64bit(&ptr);
				nameversion = get32bit(&ptr);
				if (hdrchunkid!=namechunkid) {
					unlink(fullname);
					continue;
				}
				sprintf(fullname+plen,"chunk_%016"PRIX64"_%08"PRIX32".mfs",namechunkid,nameversion);
				if (rename(oldfullname,fullname)<0) {
					syslog(LOG_WARNING,"can't rename %s to %s: %m",oldfullname,fullname);
					memcpy(fullname+plen,oldfullname+plen,27);
				}
			}
			if (stat(fullname,&sb)<0) {
				unlink(fullname);
				continue;
			}
			if (access(fullname,R_OK | W_OK)<0) {
				syslog(LOG_WARNING,"access to file: %s: %m",fullname);
				continue;
			}
			if (sb.st_size<CHUNKHDRSIZE || sb.st_size>(CHUNKHDRSIZE+0x4000000) || ((sb.st_size-CHUNKHDRSIZE)&0xFFFF)!=0) {
				unlink(fullname);	// remove wrong chunk
				continue;
			}
			prevf = NULL;
			c = hdd_chunk_get(namechunkid,CH_NEW_AUTO);
			if (c->filename!=NULL) {	// already have this chunk
				if (nameversion <= c->version) {	// current chunk is older
					unlink(fullname);
				} else {
					unlink(c->filename);
					free(c->filename);
					prevf = c->owner;
					c->filename = strdup(fullname);
					c->version = nameversion;
					c->blocks = (sb.st_size - CHUNKHDRSIZE) / 0x10000;
					c->owner = f;
					c->testtime = (sb.st_atime>sb.st_mtime)?sb.st_atime:sb.st_mtime;
#ifdef _THREAD_SAFE
					pthread_mutex_lock(&testlock);
					// remove from previous chain
					*(c->testprev) = c->testnext;
					if (c->testnext) {
						c->testnext->testprev = c->testprev;
					} else {
						prevf->testtail = c->testprev;
					}
					// add to new one
					c->testprev = f->testtail;
					*(c->testprev) = c;
					f->testtail = &(c->testnext);
					pthread_mutex_unlock(&testlock);
#endif
				}
			} else {
				c->filename = strdup(fullname);
				c->version = nameversion;
				c->blocks = (sb.st_size - CHUNKHDRSIZE) / 0x10000;
				c->owner = f;
				c->testtime = (sb.st_atime>sb.st_mtime)?sb.st_atime:sb.st_mtime;
#ifdef _THREAD_SAFE
				pthread_mutex_lock(&testlock);
				c->testprev = f->testtail;
				*(c->testprev) = c;
				f->testtail = &(c->testnext);
				pthread_mutex_unlock(&testlock);
#endif
			}
			hdd_chunk_release(c);
#ifdef _THREAD_SAFE
			pthread_mutex_lock(&folderlock);
#endif
			if (prevf) {
				prevf->chunkcount--;
			}
			f->chunkcount++;
#ifdef _THREAD_SAFE
			pthread_mutex_unlock(&folderlock);
#endif
		}
		closedir(dd);
	}
	free(fullname);
	free(oldfullname);
	return NULL;
}

#ifdef _THREAD_SAFE
void* hdd_folders_thread(void *arg) {
	for (;;) {
		sleep((unsigned long)arg);
		hdd_check_folders();
	}
}

void* hdd_delayed_thread(void *arg) {
	for (;;) {
		sleep((unsigned long)arg);
		hdd_delayed_ops();
	}
}
#endif

int hdd_init(void) {
	uint32_t l;
	int lfp,td;
	uint32_t hp;
	FILE *fd;
	char buff[1000];
	char *pptr;
	char *lockfname;
	char *hddfname;
	struct stat sb;
	folder *f,*sf;
#ifdef _THREAD_SAFE
	uint8_t testid;
#endif

	// this routine is called at the beginning from the main thread so no locks are necessary here
	for (hp=0 ; hp<HASHSIZE ; hp++) {
		hashtab[hp]=NULL;
	}
	for (hp=0 ; hp<DHASHSIZE ; hp++) {
		dophashtab[hp]=NULL;
	}

#ifndef PRESERVE_BLOCK
#ifdef _THREAD_SAFE
	pthread_key_create(&hdrbufferkey,free);
	pthread_key_create(&blockbufferkey,free);
#endif
#endif /* PRESERVE_BLOCK */

//	memset(blockbuffer,0,0x10000);
//	emptyblockcrc = mycrc32(0,blockbuffer,0x10000);
	emptyblockcrc = mycrc32_zeroblock(0,0x10000);

	config_getnewstr("HDD_CONF_FILENAME",ETC_PATH "/mfshdd.cfg",&hddfname);

	fd = fopen(hddfname,"r");
	free(hddfname);
	if (!fd) {
		return -1;
	}
	while (fgets(buff,999,fd)) {
		buff[999]=0;
		if (buff[0]=='#') {
			continue;
		}
		l = strlen(buff);
		while (l>0 && (buff[l-1]=='\r' || buff[l-1]=='\n' || buff[l-1]==' ' || buff[l-1]=='\t')) {
			l--;
		}
		if (l>0) {
			if (buff[l-1]!='/') {
				buff[l]='/';
				buff[l+1]='\0';
				l++;
			} else {
				buff[l]='\0';
			}
			if (buff[0]=='*') {
				td = 1;
				pptr = buff+1;
				l--;
			} else {
				td = 0;
				pptr = buff;
			}
			lockfname = (char*)malloc(l+6);
			memcpy(lockfname,pptr,l);
			memcpy(lockfname+l,".lock",6);
			lfp=open(lockfname,O_RDWR|O_CREAT|O_TRUNC,0640);
			if (lfp<0) {
				syslog(LOG_ERR,"can't create lock file '%s': %m",lockfname);
				free(lockfname);
				return -1;
			}
			if (lockf(lfp,F_TLOCK,0)<0) {
				if (errno==EAGAIN) {
					syslog(LOG_ERR,"data folder '%s' already locked (used by another process)",pptr);
				} else {
					syslog(LOG_NOTICE,"lockf '%s' error: %m",lockfname);
				}
				free(lockfname);
				return -1;
			}
			if (fstat(lfp,&sb)<0) {
				syslog(LOG_NOTICE,"fstat '%s' error: %m",lockfname);
				free(lockfname);
				return -1;
			}
			free(lockfname);
			for (sf=folderhead ; sf ; sf=sf->next) {
				if (sf->devid==sb.st_dev) {
					if (sf->lockinode==sb.st_ino) {
						syslog(LOG_ERR,"data folder '%s' already locked (used by this process)",pptr);
						return -1;
					} else {
						syslog(LOG_WARNING,"data folders '%s' and '%s' are on the same physical device (could lead to unexpected behaviours)",pptr,sf->path);
					}
				}
			}
			f = (folder*)malloc(sizeof(folder));
			f->todel = td;
			f->path = strdup(pptr);
			f->leavefree = 0x10000000; // about 256MB  -  future: (uint64_t)as*0x40000000;
			f->avail = 0ULL;
			f->total = 0ULL;
			f->chunkcount = 0;
			for (l=0 ; l<LASTERRSIZE ; l++) {
				f->lasterrtab[l].chunkid = 0ULL;
				f->lasterrtab[l].timestamp = 0;
			}
			f->lasterrindx = 0;
			f->lastrefresh = 0;
			f->needrefresh = 1;
			f->devid = sb.st_dev;
			f->lockinode = sb.st_ino;
#ifdef _THREAD_SAFE
			f->testhead = NULL;
			f->testtail = &(f->testhead);
#endif
			f->next = folderhead;
			folderhead = f;
		}
	}

	if (folderhead==NULL) {
		syslog(LOG_ERR,"no hdd space !!!");
		return -1;
	}

#ifdef _THREAD_SAFE
	/* make advantage from thread safety and scan folders in separate threads */
	testid=0;
	for (f=folderhead ; f ; f=f->next) {
		f->testid = testid++;
		pthread_create(&(f->scanthread),NULL,hdd_folder_scan,f);
	}
	for (f=folderhead ; f ; f=f->next) {
		pthread_join(f->scanthread,NULL);
		hdd_refresh_usage(f);
		f->needrefresh = 0;
	}
	config_getuint32("HDD_TEST_FREQ",10,&HDDTestFreq);
	if (HDDTestFreq>0) {
		for (f=folderhead ; f ; f=f->next) {
			hdd_testsort(f);
		}
		pthread_create(&testerthread,NULL,hdd_tester_thread,(void*)(unsigned long)(testid));
	}
#else
	for (f=folderhead ; f ; f=f->next) {
		hdd_folder_scan(f);
		hdd_refresh_usage(f);
		f->needrefresh = 0;
	}
#endif
	hdd_check_folders();

#ifdef _THREAD_SAFE
	pthread_create(&foldersthread,NULL,hdd_folders_thread,(void*)1);
	pthread_create(&delayedthread,NULL,hdd_delayed_thread,(void*)DELAYEDSTEP);
//	atexit(hdd_flush_crc);
#else
	main_timeregister(TIMEMODE_RUNONCE,1,0,hdd_check_folders);
	main_timeregister(TIMEMODE_RUNONCE,DELAYEDSTEP,0,hdd_delayed_ops);
//	main_destructregister(hdd_flush_crc);
#endif
	return 0;
}
