/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "chunkserver/hddspacemgr.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "common/cfg.h"
#include "common/crc.h"
#include "common/datapack.h"
#include "common/disk_info.h"
#include "common/list.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/MFSCommunication.h"
#include "common/moosefs_vector.h"
#include "common/random.h"
#include "common/slogger.h"

#define PRESERVE_BLOCK 1

#if defined(LIZARDFS_HAVE_PREAD) && defined(LIZARDFS_HAVE_PWRITE)
#  define USE_PIO 1
#endif

/* system every DELAYEDSTEP seconds searches opened/crc_loaded chunk list for chunks to be closed/free crc */
#define DELAYEDSTEP 2

#define OPENDELAY 5
#define CRCDELAY 100
#define OPENSTEPS (OPENDELAY/DELAYEDSTEP)+1
#define CRCSTEPS (CRCDELAY/DELAYEDSTEP)+1

#ifdef PRESERVE_BLOCK
#  define PRESERVEDELAY 10
#  define PRESERVESTEPS (PRESERVEDELAY/DELAYEDSTEP)+1
#endif

#define LOSTCHUNKSBLOCKSIZE 1024
#define NEWCHUNKSBLOCKSIZE 4096

#define CHUNKHDRSIZE (1024+4*1024)
#define CHUNKHDRCRC 1024

#define STATSHISTORY (24*60)

#define ERRORLIMIT 2
#define LASTERRSIZE 30
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

typedef struct newchunk {
	uint64_t chunkidblock[NEWCHUNKSBLOCKSIZE];
	uint32_t versionblock[NEWCHUNKSBLOCKSIZE];
	uint32_t chunksinblock;
	struct newchunk *next;
} newchunk;

typedef struct dopchunk {
	uint64_t chunkid;
	struct dopchunk *next;
} dopchunk;

struct folder;

typedef struct ioerror {
	uint64_t chunkid;
	uint32_t timestamp;
	int errornumber;
} ioerror;

typedef struct _cntcond {
	pthread_cond_t cond;
	uint32_t wcnt;
	struct _cntcond *next;
} cntcond;

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
#define CH_AVAIL 0
#define CH_LOCKED 1
#define CH_DELETED 2
#define CH_TOBEDELETED 3
	uint8_t state;  // CH_AVAIL,CH_LOCKED,CH_DELETED
	cntcond *ccond;
	uint8_t *crc;
	int fd;

#ifdef PRESERVE_BLOCK
	uint8_t *block;
	uint16_t blockno;       // 0xFFFF == invalid
	uint8_t blocksteps;
#endif
	uint8_t validattr;
	uint8_t todel;
	struct chunk *testnext,**testprev;
	struct chunk *next;
} chunk;

typedef struct folder {
	char *path;
#define SCST_SCANNEEDED 0
#define SCST_SCANINPROGRESS 1
#define SCST_SCANTERMINATE 2
#define SCST_SCANFINISHED 3
#define SCST_SENDNEEDED 4
#define SCST_WORKING 5
	unsigned int scanstate:3;
	unsigned int needrefresh:1;
	unsigned int todel:2;
	unsigned int damaged:1;
	unsigned int toremove:2;
	uint8_t scanprogress;
	uint64_t sizelimit;
	uint64_t leavefree;
	uint64_t avail;
	uint64_t total;
	HddStatistics cstat;
	HddStatistics stats[STATSHISTORY];
	uint32_t statspos;
	ioerror lasterrtab[LASTERRSIZE];
	uint32_t chunkcount;
	uint32_t lasterrindx;
	uint32_t lastrefresh;
	dev_t devid;
	ino_t lockinode;
	int lfd;
	double carry;
	pthread_t scanthread;
	struct chunk *testhead,**testtail;
	struct folder *next;
} folder;

static uint32_t HDDTestFreq = 10;
static uint64_t LeaveFree;

/* folders data */
static folder *folderhead = NULL;

/* chunk hash */
static chunk* hashtab[HASHSIZE];

/* extra chunk info */
static dopchunk *dophashtab[DHASHSIZE];
//static dopchunk *dopchunks = NULL;
static dopchunk *newdopchunks = NULL;

// master reports
static damagedchunk *damagedchunks = NULL;
static lostchunk *lostchunks = NULL;
static newchunk *newchunks = NULL;
static uint32_t errorcounter = 0;
static int hddspacechanged = 0;

static pthread_attr_t thattr;

static pthread_t foldersthread,delayedthread,testerthread;
static uint8_t term = 0;
static uint8_t folderactions = 0;
static uint8_t testerreset = 0;
static pthread_mutex_t termlock = PTHREAD_MUTEX_INITIALIZER;

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

// folderhead + all data in structures
static pthread_mutex_t folderlock = PTHREAD_MUTEX_INITIALIZER;

// chunk tester
static pthread_mutex_t testlock = PTHREAD_MUTEX_INITIALIZER;

#ifndef PRESERVE_BLOCK
static pthread_key_t hdrbufferkey;
static pthread_key_t blockbufferkey;
#endif

static uint32_t emptyblockcrc;

static uint64_t stats_bytesr = 0;
static uint64_t stats_bytesw = 0;
static uint32_t stats_opr = 0;
static uint32_t stats_opw = 0;
static uint64_t stats_databytesr = 0;
static uint64_t stats_databytesw = 0;
static uint32_t stats_dataopr = 0;
static uint32_t stats_dataopw = 0;
static uint64_t stats_rtime = 0;
static uint64_t stats_wtime = 0;

static uint32_t stats_create = 0;
static uint32_t stats_delete = 0;
static uint32_t stats_test = 0;
static uint32_t stats_version = 0;
static uint32_t stats_duplicate = 0;
static uint32_t stats_truncate = 0;
static uint32_t stats_duptrunc = 0;

void hdd_report_damaged_chunk(uint64_t chunkid) {
	damagedchunk *dc;
	zassert(pthread_mutex_lock(&dclock));
	dc = (damagedchunk*) malloc(sizeof(damagedchunk));
	passert(dc);
	dc->chunkid = chunkid;
	dc->next = damagedchunks;
	damagedchunks = dc;
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_get_damaged_chunks(std::vector<uint64_t>& buffer) {
	damagedchunk *dc,*ndc;
	uint64_t chunkid;
	sassert(buffer.empty());
	zassert(pthread_mutex_lock(&dclock));
	buffer.reserve(list_length(damagedchunks));
	dc = damagedchunks;
	while (dc) {
		ndc = dc;
		dc = dc->next;
		chunkid = ndc->chunkid;
		buffer.push_back(chunkid);
		free(ndc);
	}
	damagedchunks = NULL;
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_report_lost_chunk(uint64_t chunkid) {
	lostchunk *lc;
	zassert(pthread_mutex_lock(&dclock));
	if (lostchunks && lostchunks->chunksinblock<LOSTCHUNKSBLOCKSIZE) {
		lostchunks->chunkidblock[lostchunks->chunksinblock++] = chunkid;
	} else {
		lc = (lostchunk*) malloc(sizeof(lostchunk));
		passert(lc);
		lc->chunkidblock[0] = chunkid;
		lc->chunksinblock = 1;
		lc->next = lostchunks;
		lostchunks = lc;
	}
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_get_lost_chunks(std::vector<uint64_t>& chunks, uint32_t limit) {
	lostchunk *lc,**lcptr;
	uint64_t chunkid;
	uint32_t i;
	sassert(chunks.empty());
	chunks.reserve(limit);
	zassert(pthread_mutex_lock(&dclock));
	lcptr = &lostchunks;
	while ((lc=*lcptr)) {
		if (limit>lc->chunksinblock) {
			for (i=0 ; i<lc->chunksinblock ; i++) {
				chunkid = lc->chunkidblock[i];
				chunks.push_back(chunkid);
			}
			limit -= lc->chunksinblock;
			*lcptr = lc->next;
			free(lc);
		} else {
			lcptr = &(lc->next);
		}
	}
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_report_new_chunk(uint64_t chunkid, uint32_t version) {
	newchunk *nc;
	zassert(pthread_mutex_lock(&dclock));
	if (newchunks && newchunks->chunksinblock<NEWCHUNKSBLOCKSIZE) {
		newchunks->chunkidblock[newchunks->chunksinblock] = chunkid;
		newchunks->versionblock[newchunks->chunksinblock] = version;
		newchunks->chunksinblock++;
	} else {
		nc = (newchunk*) malloc(sizeof(newchunk));
		passert(nc);
		nc->chunkidblock[0] = chunkid;
		nc->versionblock[0] = version;
		nc->chunksinblock = 1;
		nc->next = newchunks;
		newchunks = nc;
	}
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_get_new_chunks(std::vector<ChunkWithVersion>& chunks, uint32_t limit) {
	newchunk *nc,**ncptr;
	uint64_t chunkid;
	uint32_t version;
	uint32_t i;
	sassert(chunks.empty());
	chunks.reserve(limit);
	zassert(pthread_mutex_lock(&dclock));
	ncptr = &newchunks;
	while ((nc=*ncptr)) {
		if (limit>nc->chunksinblock) {
			for (i=0 ; i<nc->chunksinblock ; i++) {
				chunkid = nc->chunkidblock[i];
				version = nc->versionblock[i];
				chunks.push_back(ChunkWithVersion{chunkid, version});
			}
			limit -= nc->chunksinblock;
			*ncptr = nc->next;
			free(nc);
		} else {
			ncptr = &(nc->next);
		}
	}
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_errorcounter(void) {
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = errorcounter;
	errorcounter = 0;
	zassert(pthread_mutex_unlock(&dclock));
	return result;
}

int hdd_spacechanged(void) {
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = hddspacechanged;
	hddspacechanged = 0;
	zassert(pthread_mutex_unlock(&dclock));
	return result;
}

void hdd_stats(uint64_t *br,uint64_t *bw,uint32_t *opr,uint32_t *opw,uint64_t *dbr,uint64_t *dbw,uint32_t *dopr,uint32_t *dopw,uint64_t *rtime,uint64_t *wtime) {
	zassert(pthread_mutex_lock(&statslock));
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
	stats_bytesr = 0;
	stats_bytesw = 0;
	stats_opr = 0;
	stats_opw = 0;
	stats_databytesr = 0;
	stats_databytesw = 0;
	stats_dataopr = 0;
	stats_dataopw = 0;
	stats_rtime = 0;
	stats_wtime = 0;
	zassert(pthread_mutex_unlock(&statslock));
}

void hdd_op_stats(uint32_t *op_create,uint32_t *op_delete,uint32_t *op_version,uint32_t *op_duplicate,uint32_t *op_truncate,uint32_t *op_duptrunc,uint32_t *op_test) {
	zassert(pthread_mutex_lock(&statslock));
	*op_create = stats_create;
	*op_delete = stats_delete;
	*op_version = stats_version;
	*op_duplicate = stats_duplicate;
	*op_truncate = stats_truncate;
	*op_duptrunc = stats_duptrunc;
	*op_test = stats_test;
	stats_create = 0;
	stats_delete = 0;
	stats_version = 0;
	stats_duplicate = 0;
	stats_truncate = 0;
	stats_duptrunc = 0;
	stats_test = 0;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_read(uint32_t size) {
	zassert(pthread_mutex_lock(&statslock));
	stats_opr++;
	stats_bytesr += size;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_write(uint32_t size) {
	zassert(pthread_mutex_lock(&statslock));
	stats_opw++;
	stats_bytesw += size;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_dataread(folder *f,uint32_t size,int64_t rtime) {
	if (rtime<=0) {
		return;
	}
	zassert(pthread_mutex_lock(&statslock));
	stats_dataopr++;
	stats_databytesr += size;
	stats_rtime += rtime;
	f->cstat.rops++;
	f->cstat.rbytes += size;
	f->cstat.usecreadsum += rtime;
	if (rtime>f->cstat.usecreadmax) {
		f->cstat.usecreadmax = rtime;
	}
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_datawrite(folder *f,uint32_t size,int64_t wtime) {
	if (wtime<=0) {
		return;
	}
	zassert(pthread_mutex_lock(&statslock));
	stats_dataopw++;
	stats_databytesw += size;
	stats_wtime += wtime;
	f->cstat.wops++;
	f->cstat.wbytes += size;
	f->cstat.usecwritesum += wtime;
	if (wtime>f->cstat.usecwritemax) {
		f->cstat.usecwritemax = wtime;
	}
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_datafsync(folder *f,int64_t fsynctime) {
	if (fsynctime<=0) {
		return;
	}
	zassert(pthread_mutex_lock(&statslock));
	stats_wtime += fsynctime;
	f->cstat.fsyncops++;
	f->cstat.usecfsyncsum += fsynctime;
	if (fsynctime>f->cstat.usecfsyncmax) {
		f->cstat.usecfsyncmax = fsynctime;
	}
	zassert(pthread_mutex_unlock(&statslock));
}

uint32_t hdd_diskinfo_v1_size() {
	folder *f;
	uint32_t s,sl;

	s = 0;
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		sl = strlen(f->path);
		if (sl>255) {
			sl = 255;
		}
		s += 34+sl;
	}
	return s;
}

void hdd_diskinfo_v1_data(uint8_t *buff) {
	folder *f;
	uint32_t sl;
	uint32_t ei;
	if (buff) {
		for (f=folderhead ; f ; f=f->next) {
			sl = strlen(f->path);
			if (sl>255) {
				put8bit(&buff,255);
				memcpy(buff,"(...)",5);
				memcpy(buff+5,f->path+(sl-250),250);
				buff += 255;
			} else {
				put8bit(&buff,sl);
				if (sl>0) {
					memcpy(buff,f->path,sl);
					buff += sl;
				}
			}
			put8bit(&buff,((f->todel)?1:0)+((f->damaged)?2:0)+((f->scanstate==SCST_SCANINPROGRESS)?4:0));
			ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
			put64bit(&buff,f->lasterrtab[ei].chunkid);
			put32bit(&buff,f->lasterrtab[ei].timestamp);
			put64bit(&buff,f->total-f->avail);
			put64bit(&buff,f->total);
			put32bit(&buff,f->chunkcount);
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
}

uint32_t hdd_diskinfo_v2_size() {
	folder *f;
	uint32_t s,sl;

	s = 0;
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		sl = strlen(f->path);
		if (sl>255) {
			sl = 255;
		}
		s += 2+226+sl;
	}
	return s;
}

void hdd_diskinfo_v2_data(uint8_t *buff) {
	folder *f;
	HddStatistics s;
	uint32_t ei;
	uint32_t pos;
	if (buff) {
		MooseFSVector<DiskInfo> diskInfoVector;
		zassert(pthread_mutex_lock(&statslock));
		for (f = folderhead; f; f = f->next) {
			diskInfoVector.emplace_back();
			DiskInfo& diskInfo = diskInfoVector.back();
			diskInfo.path = f->path;
			if (diskInfo.path.length() > MooseFsString<uint8_t>::maxLength()) {
				std::string dots("(...)");
				uint32_t substrSize = MooseFsString<uint8_t>::maxLength() - dots.length();
				diskInfo.path = dots + diskInfo.path.substr(diskInfo.path.length()
						- substrSize, substrSize);
			}
			diskInfo.entrySize = serializedSize(diskInfo) - serializedSize(diskInfo.entrySize);
			diskInfo.flags = (f->todel ? DiskInfo::kToDeleteFlagMask : 0)
					+ (f->damaged ? DiskInfo::kDamagedFlagMask : 0)
					+ (f->scanstate == SCST_SCANINPROGRESS ? DiskInfo::kScanInProgressFlagMask : 0);
			ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
			diskInfo.errorChunkId = f->lasterrtab[ei].chunkid;
			diskInfo.errorTimeStamp = f->lasterrtab[ei].timestamp;
			if (f->scanstate==SCST_SCANINPROGRESS) {
				diskInfo.used = f->scanprogress;
				diskInfo.total = 0;
			} else {
				diskInfo.used = f->total-f->avail;
				diskInfo.total = f->total;
			}
			diskInfo.chunksCount = f->chunkcount;
			s = f->stats[f->statspos];
			diskInfo.lastMinuteStats = s;
			for (pos=1 ; pos<60 ; pos++) {
				s.add(f->stats[(f->statspos+pos)%STATSHISTORY]);
			}
			diskInfo.lastHourStats = s;
			for (pos=60 ; pos<24*60 ; pos++) {
				s.add(f->stats[(f->statspos+pos)%STATSHISTORY]);
			}
			diskInfo.lastDayStats = s;
		}
		zassert(pthread_mutex_unlock(&statslock));
		serialize(&buff, diskInfoVector);
	}
	zassert(pthread_mutex_unlock(&folderlock));
}

void hdd_diskinfo_movestats(void) {
	folder *f;
	zassert(pthread_mutex_lock(&folderlock));
	zassert(pthread_mutex_lock(&statslock));
	for (f=folderhead ; f ; f=f->next) {
		if (f->statspos==0) {
			f->statspos = STATSHISTORY-1;
		} else {
			f->statspos--;
		}
		f->stats[f->statspos] = f->cstat;
		f->cstat.clear();
	}
	zassert(pthread_mutex_unlock(&statslock));
	zassert(pthread_mutex_unlock(&folderlock));
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
			if (cp->owner) {
				zassert(pthread_mutex_lock(&testlock));
				if (cp->testnext) {
					cp->testnext->testprev = cp->testprev;
				} else {
					cp->owner->testtail = cp->testprev;
				}
				*(cp->testprev) = cp->testnext;
				zassert(pthread_mutex_unlock(&testlock));
			}
			free(cp);
			return;
		}
		cptr = &(cp->next);
	}
}

static void hdd_chunk_release(chunk *c) {
	zassert(pthread_mutex_lock(&hashlock));
//      syslog(LOG_WARNING,"hdd_chunk_release got chunk: %016" PRIX64 " (c->state:%u)",c->chunkid,c->state);
	if (c->state==CH_LOCKED) {
		c->state = CH_AVAIL;
		if (c->ccond) {
//                      printf("wake up one thread waiting for AVAIL chunk: %" PRIu64 " on ccond:%p\n",c->chunkid,c->ccond);
			zassert(pthread_cond_signal(&(c->ccond->cond)));
		}
	} else if (c->state==CH_TOBEDELETED) {
		if (c->ccond) {
			c->state = CH_DELETED;
//                      printf("wake up one thread waiting for DELETED chunk: %" PRIu64 " on ccond:%p\n",c->chunkid,c->ccond);
			zassert(pthread_cond_signal(&(c->ccond->cond)));
		} else {
			hdd_chunk_remove(c);
		}
	}
	zassert(pthread_mutex_unlock(&hashlock));
}

static int hdd_chunk_getattr(chunk *c) {
	struct stat sb;
	if (stat(c->filename,&sb)<0) {
		return -1;
	}
	if ((sb.st_mode & S_IFMT) != S_IFREG) {
		return -1;
	}
	if (sb.st_size<CHUNKHDRSIZE || sb.st_size>(CHUNKHDRSIZE+MFSCHUNKSIZE) || ((sb.st_size-CHUNKHDRSIZE)&MFSBLOCKMASK)!=0) {
		return -1;
	}
	c->blocks = (sb.st_size - CHUNKHDRSIZE) / MFSBLOCKSIZE;
	c->validattr = 1;
	return 0;
}

static chunk* hdd_chunk_tryfind(uint64_t chunkid) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
	zassert(pthread_mutex_lock(&hashlock));
	for (c=hashtab[hashpos] ; c && c->chunkid!=chunkid ; c=c->next) {}
	if (c!=NULL) {
		if (c->state==CH_LOCKED) {
			c = (chunk*) CHUNKLOCKED;
		} else if (c->state!=CH_AVAIL) {
			c = NULL;
		} else {
			c->state = CH_LOCKED;
		}
	}
//      if (c!=NULL && c!=CHUNKLOCKED) {
//              syslog(LOG_WARNING,"hdd_chunk_tryfind returns chunk: %016" PRIX64 " (c->state:%u)",c->chunkid,c->state);
//      }
	zassert(pthread_mutex_unlock(&hashlock));
	return c;
}

static void hdd_chunk_delete(chunk *c);

static chunk* hdd_chunk_get(uint64_t chunkid,uint8_t cflag) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
	cntcond *cc;
	zassert(pthread_mutex_lock(&hashlock));
	for (c=hashtab[hashpos] ; c && c->chunkid!=chunkid ; c=c->next) {}
	if (c==NULL) {
		if (cflag!=CH_NEW_NONE) {
			c = (chunk*) malloc(sizeof(chunk));
			passert(c);
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
			c->state = CH_LOCKED;
			c->ccond = NULL;
#ifdef PRESERVE_BLOCK
			c->block = NULL;
			c->blockno = 0xFFFF;
			c->blocksteps = 0;
#endif
			c->validattr = 0;
			c->todel = 0;
			c->testnext = NULL;
			c->testprev = NULL;
			c->next = hashtab[hashpos];
			hashtab[hashpos] = c;
		}
//              syslog(LOG_WARNING,"hdd_chunk_get returns chunk: %016" PRIX64 " (c->state:%u)",c->chunkid,c->state);
		zassert(pthread_mutex_unlock(&hashlock));
		return c;
	}
	if (cflag==CH_NEW_EXCLUSIVE) {
		if (c->state==CH_AVAIL || c->state==CH_LOCKED) {
			zassert(pthread_mutex_unlock(&hashlock));
			return NULL;
		}
	}
	for (;;) {
		switch (c->state) {
		case CH_AVAIL:
			c->state = CH_LOCKED;
//                      syslog(LOG_WARNING,"hdd_chunk_get returns chunk: %016" PRIX64 " (c->state:%u)",c->chunkid,c->state);
			zassert(pthread_mutex_unlock(&hashlock));
			if (c->validattr==0) {
				if (hdd_chunk_getattr(c)) {
					hdd_report_damaged_chunk(c->chunkid);
					unlink(c->filename);
					hdd_chunk_delete(c);
					return NULL;
				}
			}
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
				zassert(pthread_mutex_lock(&testlock));
				if (c->testnext) {
					c->testnext->testprev = c->testprev;
				} else {
					c->owner->testtail = c->testprev;
				}
				*(c->testprev) = c->testnext;
				c->testnext = NULL;
				c->testprev = NULL;
				zassert(pthread_mutex_unlock(&testlock));
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
				c->validattr = 0;
				c->todel = 0;
				c->state = CH_LOCKED;
//                              syslog(LOG_WARNING,"hdd_chunk_get returns chunk: %016" PRIX64 " (c->state:%u)",c->chunkid,c->state);
				zassert(pthread_mutex_unlock(&hashlock));
				return c;
			}
			if (c->ccond==NULL) {   // no more waiting threads - remove
				hdd_chunk_remove(c);
			} else {        // there are waiting threads - wake them up
//                              printf("wake up one thread waiting for DELETED chunk: %" PRIu64 " on ccond:%p\n",c->chunkid,c->ccond);
				zassert(pthread_cond_signal(&(c->ccond->cond)));
			}
			zassert(pthread_mutex_unlock(&hashlock));
			return NULL;
		case CH_TOBEDELETED:
		case CH_LOCKED:
			if (c->ccond==NULL) {
				for (cc=cclist ; cc && cc->wcnt ; cc=cc->next) {}
				if (cc==NULL) {
					cc = (cntcond*) malloc(sizeof(cntcond));
					passert(cc);
					zassert(pthread_cond_init(&(cc->cond),NULL));
					cc->wcnt = 0;
					cc->next = cclist;
					cclist = cc;
				}
				c->ccond = cc;
			}
			c->ccond->wcnt++;
//                      printf("wait for %s chunk: %" PRIu64 " on ccond:%p\n",(c->state==CH_LOCKED)?"LOCKED":"TOBEDELETED",c->chunkid,c->ccond);
			zassert(pthread_cond_wait(&(c->ccond->cond),&hashlock));
//                      printf("%s chunk: %" PRIu64 " woke up on ccond:%p\n",(c->state==CH_LOCKED)?"LOCKED":(c->state==CH_DELETED)?"DELETED":(c->state==CH_AVAIL)?"AVAIL":"TOBEDELETED",c->chunkid,c->ccond);
			c->ccond->wcnt--;
			if (c->ccond->wcnt==0) {
				c->ccond = NULL;
			}
		}
	}
}

static void hdd_chunk_delete(chunk *c) {
	folder *f;
	zassert(pthread_mutex_lock(&hashlock));
	f = c->owner;
	if (c->ccond) {
		c->state = CH_DELETED;
//              printf("wake up one thread waiting for DELETED chunk: %" PRIu64 " ccond:%p\n",c->chunkid,c->ccond);
		zassert(pthread_cond_signal(&(c->ccond->cond)));
	} else {
		hdd_chunk_remove(c);
	}
	zassert(pthread_mutex_unlock(&hashlock));
	zassert(pthread_mutex_lock(&folderlock));
	f->chunkcount--;
	f->needrefresh = 1;
	zassert(pthread_mutex_unlock(&folderlock));
}

static chunk* hdd_chunk_create(folder *f,uint64_t chunkid,uint32_t version) {
	uint32_t leng;
	chunk *c;

	c = hdd_chunk_get(chunkid,CH_NEW_EXCLUSIVE);
	if (c==NULL) {
		return NULL;
	}
	c->version = version;
	leng = strlen(f->path);
	c->filename = (char*) malloc(leng+39);
	passert(c->filename);
	memcpy(c->filename,f->path,leng);
	sprintf(c->filename+leng,"%02X/chunk_%016" PRIX64 "_%08" PRIX32 ".mfs",(unsigned int)(chunkid&255),chunkid,version);
	f->needrefresh = 1;
	f->chunkcount++;
	c->owner = f;
	zassert(pthread_mutex_lock(&testlock));
	c->testnext = NULL;
	c->testprev = f->testtail;
	(*c->testprev) = c;
	f->testtail = &(c->testnext);
	zassert(pthread_mutex_unlock(&testlock));
	return c;
}

#define hdd_chunk_find(chunkid) hdd_chunk_get(chunkid,CH_NEW_NONE)

static void hdd_chunk_testmove(chunk *c) {
	zassert(pthread_mutex_lock(&testlock));
	if (c->testnext) {
		*(c->testprev) = c->testnext;
		c->testnext->testprev = c->testprev;
		c->testnext = NULL;
		c->testprev = c->owner->testtail;
		*(c->testprev) = c;
		c->owner->testtail = &(c->testnext);
	}
	zassert(pthread_mutex_unlock(&testlock));
}

// no locks - locked by caller
static inline void hdd_refresh_usage(folder *f) {
	if (f->sizelimit) {
		uint32_t knownblocks;
		uint32_t knowncount;
		uint64_t calcsize;
		chunk *c;
		knownblocks = 0;
		knowncount = 0;
		zassert(pthread_mutex_lock(&hashlock));
		zassert(pthread_mutex_lock(&testlock));
		for (c=f->testhead ; c ; c=c->testnext) {
			if (c->state==CH_AVAIL && c->validattr==1) {
				knowncount++;
				knownblocks+=c->blocks;
			}
		}
		zassert(pthread_mutex_unlock(&testlock));
		zassert(pthread_mutex_unlock(&hashlock));
		if (knowncount>0) {
			calcsize = knownblocks;
			calcsize *= f->chunkcount;
			calcsize /= knowncount;
			calcsize *= 64;
			calcsize += f->chunkcount*5;
			calcsize *= 1024;
		} else { // unknown result;
			calcsize = 0;
		}
		f->total = f->sizelimit;
		f->avail = (calcsize>f->sizelimit)?0:f->sizelimit-calcsize;
	} else {
		struct statvfs fsinfo;

		if (statvfs(f->path,&fsinfo)<0) {
			f->avail = 0ULL;
			f->total = 0ULL;
			return;
		}
		f->avail = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_bavail);
		f->total = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));
		if (f->avail < f->leavefree) {
			f->avail = 0ULL;
		} else {
			f->avail -= f->leavefree;
		}
	}
}

static inline folder* hdd_getfolder() {
	folder *f,*bf;
	double maxcarry;
	double minavail,maxavail;
	double s,d;
	double pavail;
	int ok;

	minavail = 0.0;
	maxavail = 0.0;
	maxcarry = 1.0;
	bf = NULL;
	ok = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged || f->todel || f->total==0 || f->avail==0 || f->scanstate!=SCST_WORKING) {
			continue;
		}
		if (f->carry >= maxcarry) {
			maxcarry = f->carry;
			bf = f;
		}
		pavail = (double)(f->avail)/(double)(f->total);
		if (ok==0 || minavail>pavail) {
			minavail = pavail;
			ok = 1;
		}
		if (pavail>maxavail) {
			maxavail = pavail;
		}
	}
	if (bf) {
		bf->carry -= 1.0;
		return bf;
	}
	if (maxavail==0.0) {    // no space
		return NULL;
	}
	if (maxavail<0.01) {
		s = 0.0;
	} else {
		s = minavail*0.8;
		if (s<0.01) {
			s = 0.01;
		}
	}
	d = maxavail-s;
	maxcarry = 1.0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged || f->todel || f->total==0 || f->avail==0 || f->scanstate!=SCST_WORKING) {
			continue;
		}
		pavail = (double)(f->avail)/(double)(f->total);
		if (pavail>s) {
			f->carry += ((pavail-s)/d);
		}
		if (f->carry >= maxcarry) {
			maxcarry = f->carry;
			bf = f;
		}
	}
	if (bf) {       // should be always true
		bf->carry -= 1.0;
	}
	return bf;
}

void hdd_senddata(folder *f,int rmflag) {
	uint32_t i;
	uint8_t todel;
	chunk **cptr,*c;

	todel = f->todel;
	zassert(pthread_mutex_lock(&hashlock));
	zassert(pthread_mutex_lock(&testlock));
	for (i=0 ; i<HASHSIZE ; i++) {
		cptr = &(hashtab[i]);
		while ((c=*cptr)) {
			if (c->owner==f) {
				c->todel = todel;
				if (rmflag) {
					hdd_report_lost_chunk(c->chunkid);
					if (c->state==CH_AVAIL) {
						*cptr = c->next;
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
						if (c->testnext) {
							c->testnext->testprev = c->testprev;
						} else {
							c->owner->testtail = c->testprev;
						}
						*(c->testprev) = c->testnext;
						free(c);
					} else if (c->state==CH_LOCKED) {
						cptr = &(c->next);
						c->state = CH_TOBEDELETED;
					}
				} else {
					hdd_report_new_chunk(c->chunkid,c->version|((c->todel)?0x80000000:0));
					cptr = &(c->next);
				}
			} else {
				cptr = &(c->next);
			}
		}
	}
	zassert(pthread_mutex_unlock(&testlock));
	zassert(pthread_mutex_unlock(&hashlock));
}

void* hdd_folder_scan(void *arg);

void hdd_check_folders() {
	folder *f,**fptr;
	uint32_t i;
	uint32_t now;
	int changed,err;
	struct timeval tv;

	gettimeofday(&tv,NULL);
	now = tv.tv_sec;

	changed = 0;
//      syslog(LOG_NOTICE,"check folders ...");

	zassert(pthread_mutex_lock(&folderlock));
	if (folderactions==0) {
		zassert(pthread_mutex_unlock(&folderlock));
//              syslog(LOG_NOTICE,"check folders: disabled");
		return;
	}
//      for (f=folderhead ; f ; f=f->next) {
//              syslog(LOG_NOTICE,"folder: %s, toremove:%u, damaged:%u, todel:%u, scanstate:%u",f->path,f->toremove,f->damaged,f->todel,f->scanstate);
//      }
	fptr = &folderhead;
	while ((f=*fptr)) {
		if (f->toremove) {
			switch (f->scanstate) {
			case SCST_SCANINPROGRESS:
				f->scanstate = SCST_SCANTERMINATE;
				break;
			case SCST_SCANFINISHED:
				zassert(pthread_join(f->scanthread,NULL));
				// no break - it's ok !!!
			case SCST_SENDNEEDED:
			case SCST_SCANNEEDED:
				f->scanstate = SCST_WORKING;
				// no break - it's ok !!!
			case SCST_WORKING:
				hdd_senddata(f,1);
				changed = 1;
				f->toremove = 0;
				break;
			}
			if (f->toremove==0) { // 0 here means 'removed', so delete it from data structures
				*fptr = f->next;
				syslog(LOG_NOTICE,"folder %s successfully removed",f->path);
				if (f->lfd>=0) {
					close(f->lfd);
				}
				free(f->path);
				free(f);
				testerreset = 1;
			} else {
				fptr = &(f->next);
			}
		} else {
			fptr = &(f->next);
		}
	}
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged || f->toremove) {
			continue;
		}
		switch (f->scanstate) {
		case SCST_SCANNEEDED:
			f->scanstate = SCST_SCANINPROGRESS;
			zassert(pthread_create(&(f->scanthread),&thattr,hdd_folder_scan,f));
			break;
		case SCST_SCANFINISHED:
			zassert(pthread_join(f->scanthread,NULL));
			f->scanstate = SCST_WORKING;
			hdd_refresh_usage(f);
			f->needrefresh = 0;
			f->lastrefresh = now;
			changed = 1;
			break;
		case SCST_SENDNEEDED:
			hdd_senddata(f,0);
			f->scanstate = SCST_WORKING;
			hdd_refresh_usage(f);
			f->needrefresh = 0;
			f->lastrefresh = now;
			changed = 1;
			break;
		case SCST_WORKING:
			err = 0;
			for (i=0 ; i<LASTERRSIZE; i++) {
				if (f->lasterrtab[i].timestamp+LASTERRTIME>=now && (f->lasterrtab[i].errornumber==EIO || f->lasterrtab[i].errornumber==EROFS)) {
					err++;
				}
			}
			if (err>=ERRORLIMIT && f->todel<2) {
				syslog(LOG_WARNING,"%u errors occurred in %u seconds on folder: %s",err,LASTERRTIME,f->path);
				hdd_senddata(f,1);
				f->damaged = 1;
				changed = 1;
			} else {
				if (f->needrefresh || f->lastrefresh+60<now) {
					hdd_refresh_usage(f);
					f->needrefresh = 0;
					f->lastrefresh = now;
					changed = 1;
				}
			}
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
	if (changed) {
		zassert(pthread_mutex_lock(&dclock));
		hddspacechanged = 1;
		zassert(pthread_mutex_unlock(&dclock));
	}
}

static inline void hdd_error_occured(chunk *c) {
	uint32_t i;
	folder *f;
	struct timeval tv;
	int errmem = errno;

	zassert(pthread_mutex_lock(&folderlock));
	gettimeofday(&tv,NULL);
	f = c->owner;
	i = f->lasterrindx;
	f->lasterrtab[i].chunkid = c->chunkid;
	f->lasterrtab[i].errornumber = errmem;
	f->lasterrtab[i].timestamp = tv.tv_sec;
	i = (i+1)%LASTERRSIZE;
	f->lasterrindx = i;
	zassert(pthread_mutex_unlock(&folderlock));

	zassert(pthread_mutex_lock(&dclock));
	errorcounter++;
	zassert(pthread_mutex_unlock(&dclock));

	errno = errmem;
}


/* interface */

#define CHUNKS_CUT_COUNT 10000
static uint32_t hdd_get_chunks_pos;

void hdd_get_chunks_begin() {
	zassert(pthread_mutex_lock(&hashlock));
	hdd_get_chunks_pos = 0;
}

void hdd_get_chunks_end() {
	zassert(pthread_mutex_unlock(&hashlock));
}

static uint32_t hdd_internal_get_chunks_next_list_count() {
	uint32_t res = 0;
	uint32_t i = 0;
	chunk *c;
	while (res<CHUNKS_CUT_COUNT && hdd_get_chunks_pos+i<HASHSIZE) {
		for (c=hashtab[hdd_get_chunks_pos+i] ; c ; c=c->next) {
			res++;
		}
		i++;
	}
	return res;
}

void hdd_get_chunks_next_list_data(std::vector<ChunkWithVersion>& chunks) {
	uint32_t res = 0;
	uint32_t v;
	chunk *c;
	sassert(chunks.empty());
	chunks.reserve(hdd_internal_get_chunks_next_list_count());
	while (res<CHUNKS_CUT_COUNT && hdd_get_chunks_pos<HASHSIZE) {
		for (c=hashtab[hdd_get_chunks_pos] ; c ; c=c->next) {
			v = c->version;
			if (c->todel) {
				v |= 0x80000000;
			}
			chunks.push_back(ChunkWithVersion{c->chunkid, v});
			res++;
		}
		hdd_get_chunks_pos++;
	}
}

void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount) {
	folder *f;
	uint64_t avail,total;
	uint64_t tdavail,tdtotal;
	uint32_t chunks,tdchunks;
	zassert(pthread_mutex_lock(&folderlock));
	avail = total = tdavail = tdtotal = 0ULL;
	chunks = tdchunks = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged || f->toremove) {
			continue;
		}
		if (f->todel==0) {
			if (f->scanstate==SCST_WORKING) {
				avail += f->avail;
				total += f->total;
			}
			chunks += f->chunkcount;
		} else {
			if (f->scanstate==SCST_WORKING) {
				tdavail += f->avail;
				tdtotal += f->total;
			}
			tdchunks += f->chunkcount;
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
	*usedspace = total-avail;
	*totalspace = total;
	*chunkcount = chunks;
	*tdusedspace = tdtotal-tdavail;
	*tdtotalspace = tdtotal;
	*tdchunkcount = tdchunks;
}

static inline void chunk_emptycrc(chunk *c) {
	c->crc = (uint8_t*)malloc(4096);
	passert(c->crc);
	memset(c->crc,0,4096);  // make valgrind happy
}

static inline int chunk_readcrc(chunk *c) {
	int ret;
	uint8_t hdr[20];
	const uint8_t *ptr;
	uint64_t chunkid;
	uint32_t version;
#ifdef USE_PIO
	if (pread(c->fd,hdr,20,0)!=20) {
		int errmem = errno;
		mfs_arg_errlog_silent(LOG_WARNING,"chunk_readcrc: file:%s - read error",c->filename);
		errno = errmem;
		return ERROR_IO;
	}
#else /* USE_PIO */
	lseek(c->fd,0,SEEK_SET);
	if (read(c->fd,hdr,20)!=20) {
		int errmem = errno;
		mfs_arg_errlog_silent(LOG_WARNING,"chunk_readcrc: file:%s - read error",c->filename);
		errno = errmem;
		return ERROR_IO;
	}
#endif /* USE_PIO */
	if (memcmp(hdr,MFSSIGNATURE "C 1.0",8)!=0) {
		syslog(LOG_WARNING,"chunk_readcrc: file:%s - wrong header",c->filename);
		errno = 0;
		return ERROR_IO;
	}
	ptr = hdr+8;
	chunkid = get64bit(&ptr);
	version = get32bit(&ptr);
	if (c->chunkid!=chunkid || c->version!=version) {
		syslog(LOG_WARNING,"chunk_readcrc: file:%s - wrong id/version in header (%016" PRIX64 "_%08" PRIX32 ")",c->filename,chunkid,version);
		errno = 0;
		return ERROR_IO;
	}
	c->crc = (uint8_t*)malloc(4096);
	passert(c->crc);
#ifdef USE_PIO
	ret = pread(c->fd,c->crc,4096,CHUNKHDRCRC);
#else /* USE_PIO */
	lseek(c->fd,CHUNKHDRCRC,SEEK_SET);
	ret = read(c->fd,c->crc,4096);
#endif /* USE_PIO */
	if (ret!=4096) {
		int errmem = errno;
		mfs_arg_errlog_silent(LOG_WARNING,"chunk_readcrc: file:%s - read error",c->filename);
		free(c->crc);
		c->crc = NULL;
		errno = errmem;
		return ERROR_IO;
	}
	hdd_stats_read(4096);
	errno = 0;
	return STATUS_OK;
}

static inline void chunk_freecrc(chunk *c) {
	free(c->crc);
	c->crc = NULL;
}

static inline int chunk_writecrc(chunk *c) {
	int ret;
	zassert(pthread_mutex_lock(&folderlock));
	c->owner->needrefresh = 1;
	zassert(pthread_mutex_unlock(&folderlock));
#ifdef USE_PIO
	ret = pwrite(c->fd,c->crc,4096,CHUNKHDRCRC);
#else /* USE_PIO */
	lseek(c->fd,CHUNKHDRCRC,SEEK_SET);
	ret = write(c->fd,c->crc,4096);
#endif /* USE_PIO */
	if (ret!=4096) {
		int errmem = errno;
		mfs_arg_errlog_silent(LOG_WARNING,"chunk_writecrc: file:%s - write error",c->filename);
		errno = errmem;
		return ERROR_IO;
	}
	hdd_stats_write(4096);
	return STATUS_OK;
}

void hdd_test_show_chunks(void) {
	uint32_t hashpos;
	chunk *c;
	zassert(pthread_mutex_lock(&hashlock));
	for (hashpos=0 ; hashpos<HASHSIZE ; hashpos++) {
		for (c=hashtab[hashpos] ; c ; c=c->next) {
			printf("chunk id:%" PRIu64 " version:%" PRIu32 " state:%" PRIu8 "\n",c->chunkid,c->version,c->state);
		}
	}
	zassert(pthread_mutex_unlock(&hashlock));
}

void hdd_test_show_openedchunks(void) {
	dopchunk *cc,*tcc;
	uint32_t dhashpos;
	chunk *c;

	printf("lock doplock\n");
	if (pthread_mutex_lock(&doplock)<0) {
		printf("lock error: %u\n",errno);
	}
	printf("lock ndoplock\n");
	if (pthread_mutex_lock(&ndoplock)<0) {
		printf("lock error: %u\n",errno);
	}
/* append new chunks */
	cc = newdopchunks;
	while (cc) {
		dhashpos = DHASHPOS(cc->chunkid);
		for (tcc=dophashtab[dhashpos] ; tcc && tcc->chunkid!=cc->chunkid ; tcc=tcc->next) {}
		if (tcc) {      // found - ignore
			tcc = cc;
			cc = cc->next;
			free(tcc);
		} else {        // not found - add
			tcc = cc;
			cc = cc->next;
			tcc->next = dophashtab[dhashpos];
			dophashtab[dhashpos] = tcc;
		}
	}
	newdopchunks = NULL;
	printf("unlock ndoplock\n");
	if (pthread_mutex_unlock(&ndoplock)<0) {
		printf("unlock error: %u\n",errno);
	}
/* show all */
	for (dhashpos=0 ; dhashpos<DHASHSIZE ; dhashpos++) {
		for (cc=dophashtab[dhashpos]; cc ; cc=cc->next) {
			c = hdd_chunk_find(cc->chunkid);
			if (c==NULL) {  // no chunk - delete entry
				printf("id: %" PRIu64 " - chunk doesn't exist\n",cc->chunkid);
			} else if (c->crcrefcount>0) {  // io in progress - skip entry
				printf("id: %" PRIu64 " - chunk in use (refcount:%u)\n",cc->chunkid,c->crcrefcount);
				hdd_chunk_release(c);
			} else {
#ifdef PRESERVE_BLOCK
				printf("id: %" PRIu64 " - fd:%d (steps:%u) crc:%p (steps:%u) block:%p,blockno:%u (steps:%u)\n",cc->chunkid,c->fd,c->opensteps,c->crc,c->crcsteps,c->block,c->blockno,c->blocksteps);
#else /* PRESERVE_BLOCK */
				printf("id: %" PRIu64 " - fd:%d (steps:%u) crc:%p (steps:%u)\n",cc->chunkid,c->fd,c->opensteps,c->crc,c->crcsteps);
#endif /* PRESERVE_BLOCK */
				hdd_chunk_release(c);
			}
		}
	}
	printf("unlock doplock\n");
	if (pthread_mutex_unlock(&doplock)<0) {
		printf("unlock error: %u\n",errno);
	}
}

void hdd_delayed_ops() {
	dopchunk **ccp,*cc,*tcc;
	uint32_t dhashpos;
	chunk *c;
	zassert(pthread_mutex_lock(&doplock));
	zassert(pthread_mutex_lock(&ndoplock));
/* append new chunks */
	cc = newdopchunks;
	while (cc) {
		dhashpos = DHASHPOS(cc->chunkid);
		for (tcc=dophashtab[dhashpos] ; tcc && tcc->chunkid!=cc->chunkid ; tcc=tcc->next) {}
		if (tcc) {      // found - ignore
			tcc = cc;
			cc = cc->next;
			free(tcc);
		} else {        // not found - add
			tcc = cc;
			cc = cc->next;
			tcc->next = dophashtab[dhashpos];
			dophashtab[dhashpos] = tcc;
		}
	}
	newdopchunks = NULL;
	zassert(pthread_mutex_unlock(&ndoplock));
/* check all */
	for (dhashpos=0 ; dhashpos<DHASHSIZE ; dhashpos++) {
		ccp = dophashtab+dhashpos;
		while ((cc=*ccp)) {
//                      printf("find chunk: %llu\n",cc->chunkid);
			c = hdd_chunk_tryfind(cc->chunkid);
//                      if (c!=NULL && c!=CHUNKLOCKED) {
//                              printf("found chunk: %llu (c->state:%u c->crcrefcount:%u)\n",cc->chunkid,c->state,c->crcrefcount);
//                      }
			if (c==NULL) {  // no chunk - delete entry
				*ccp = cc->next;
				free(cc);
			} else if (c==CHUNKLOCKED) {    // locked chunk - just ignore
				ccp = &(cc->next);
			} else if (c->crcrefcount>0) {  // io in progress - skip entry
				hdd_chunk_release(c);
				ccp = &(cc->next);
			} else {
#ifdef PRESERVE_BLOCK
				if (c->blocksteps>0) {
					c->blocksteps--;
				} else if (c->block!=NULL) {
					free(c->block);
					c->block = NULL;
					c->blockno = 0xFFFF;
				}
#endif /* PRESERVE_BLOCK */
				if (c->opensteps>0) {   // decrease counter
					c->opensteps--;
				} else if (c->fd>=0) {  // close descriptor
					if (close(c->fd)<0) {
						hdd_error_occured(c);   // uses and preserves errno !!!
						mfs_arg_errlog_silent(LOG_WARNING,"hdd_delayed_ops: file:%s - close error",c->filename);
						hdd_report_damaged_chunk(c->chunkid);
					}
					c->fd = -1;
				}
				if (c->crcsteps>0) {    // decrease counter
					c->crcsteps--;
				} else if (c->crc!=NULL) {      // free crc block
					if (c->crcchanged) {
						syslog(LOG_ERR,"serious error: crc changes lost (chunk:%016" PRIX64 "_%08" PRIX32 ")",c->chunkid,c->version);
					}
//                                      printf("chunk %llu - free crc record\n",c->chunkid);
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
	zassert(pthread_mutex_unlock(&doplock));
}

static inline uint64_t get_usectime() {
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return ((uint64_t)(tv.tv_sec))*1000000+tv.tv_usec;
}

static int hdd_io_begin(chunk *c,int newflag) {
	dopchunk *cc;
	int status;
	int add;

//      syslog(LOG_NOTICE,"chunk: %" PRIu64 " - before io",c->chunkid);
	hdd_chunk_testmove(c);
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
				if (c->todel<2) {
					c->fd = open(c->filename,O_RDWR);
				} else {
					c->fd = open(c->filename,O_RDONLY);
				}
			}
			if (c->fd<0) {
				int errmem = errno;
				mfs_arg_errlog_silent(LOG_WARNING,"hdd_io_begin: file:%s - open error",c->filename);
				errno = errmem;
				return ERROR_IO;
			}
		}
		if (c->crc==NULL) {
			if (newflag) {
				chunk_emptycrc(c);
			} else {
				status = chunk_readcrc(c);
				if (status!=STATUS_OK) {
					int errmem = errno;
					if (add) {
						close(c->fd);
						c->fd=-1;
					}
					mfs_arg_errlog_silent(LOG_WARNING,"hdd_io_begin: file:%s - read error",c->filename);
					errno = errmem;
					return status;
				}
			}
			c->crcchanged = 0;
		}
#ifdef PRESERVE_BLOCK
		if (c->block==NULL) {
			c->block = (uint8_t*)malloc(MFSBLOCKSIZE);
//                      syslog(LOG_WARNING,"chunk: %016" PRIX64 ", block:%p",c->chunkid,c->block);
			passert(c->block);
			c->blockno = 0xFFFF;
		}
#endif /* PRESERVE_BLOCK */
		if (add) {
			cc = (dopchunk*) malloc(sizeof(dopchunk));
			passert(cc);
			cc->chunkid = c->chunkid;
			zassert(pthread_mutex_lock(&ndoplock));
			cc->next = newdopchunks;
			newdopchunks = cc;
			zassert(pthread_mutex_unlock(&ndoplock));
		}
	}
	c->crcrefcount++;
	errno = 0;
	return STATUS_OK;
}

static int hdd_io_end(chunk *c) {
	int status;
	uint64_t ts,te;

//      syslog(LOG_NOTICE,"chunk: %" PRIu64 " - after io",c->chunkid);
	if (c->crcchanged) {
		status = chunk_writecrc(c);
		c->crcchanged = 0;
		if (status!=STATUS_OK) {
			int errmem = errno;
			mfs_arg_errlog_silent(LOG_WARNING,"hdd_io_end: file:%s - write error",c->filename);
			errno = errmem;
			return status;
		}
		ts = get_usectime();
#ifdef F_FULLFSYNC
		if (fcntl(c->fd,F_FULLFSYNC)<0) {
			int errmem = errno;
			mfs_arg_errlog_silent(LOG_WARNING,"hdd_io_end: file:%s - fsync (via fcntl) error",c->filename);
			errno = errmem;
			return ERROR_IO;
		}
#else
		if (fsync(c->fd)<0) {
			int errmem = errno;
			mfs_arg_errlog_silent(LOG_WARNING,"hdd_io_end: file:%s - fsync (direct call) error",c->filename);
			errno = errmem;
			return ERROR_IO;
		}
#endif
		te = get_usectime();
		hdd_stats_datafsync(c->owner,te-ts);
	}
	c->crcrefcount--;
	if (c->crcrefcount==0) {
		if (OPENSTEPS==0) {
			if (close(c->fd)<0) {
				int errmem = errno;
				c->fd = -1;
				mfs_arg_errlog_silent(LOG_WARNING,"hdd_io_end: file:%s - close error",c->filename);
				errno = errmem;
				return ERROR_IO;
			}
			c->fd = -1;
		} else {
			c->opensteps = OPENSTEPS;
		}
		c->crcsteps = CRCSTEPS;
#ifdef PRESERVE_BLOCK
		c->blocksteps = PRESERVESTEPS;
#endif
	}
	errno = 0;
	return STATUS_OK;
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_report_damaged_chunk(chunkid);
	}
	hdd_chunk_release(c);
//      if (status==STATUS_OK) {
//              syslog(LOG_NOTICE,"chunk %08" PRIX64 " opened",chunkid);
//      }
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_report_damaged_chunk(chunkid);
	}
	hdd_chunk_release(c);
//      if (status==STATUS_OK) {
//              syslog(LOG_NOTICE,"chunk %08" PRIX64 " closed",chunkid);
//      }
	return status;
}

int hdd_read(uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff) {
	chunk *c;
	int ret;
	const uint8_t *rcrcptr;
	uint32_t crc,bcrc,precrc,postcrc,combinedcrc;
	uint64_t ts,te;
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = malloc(MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
#endif /* PRESERVE_BLOCK */
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=MFSBLOCKSINCHUNK) {
		hdd_chunk_release(c);
		return ERROR_BNUMTOOBIG;
	}
	if (size>MFSBLOCKSIZE) {
		hdd_chunk_release(c);
		return ERROR_WRONGSIZE;
	}
	if ((offset>=MFSBLOCKSIZE) || (offset+size>MFSBLOCKSIZE)) {
		hdd_chunk_release(c);
		return ERROR_WRONGOFFSET;
	}
	if (blocknum>=c->blocks) {
		memset(buffer,0,size);
		if (size==MFSBLOCKSIZE) {
			crc = emptyblockcrc;
		} else {
			crc = mycrc32_zeroblock(0,size);
		}
		put32bit(&crcbuff,crc);
		hdd_chunk_release(c);
		return STATUS_OK;
	}
	if (offset==0 && size==MFSBLOCKSIZE) {
#ifdef PRESERVE_BLOCK
		if (c->blockno==blocknum) {
			memcpy(buffer,c->block,MFSBLOCKSIZE);
			ret = MFSBLOCKSIZE;
		} else {
#endif /* PRESERVE_BLOCK */
		ts = get_usectime();
#ifdef USE_PIO
		ret = pread(c->fd,buffer,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS),SEEK_SET);
		ret = read(c->fd,buffer,MFSBLOCKSIZE);
#endif /* USE_PIO */
		te = get_usectime();
		hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
#ifdef PRESERVE_BLOCK
			c->blockno = blocknum;
			memcpy(c->block,buffer,MFSBLOCKSIZE);
		}
#endif /* PRESERVE_BLOCK */
		crc = mycrc32(0,buffer,MFSBLOCKSIZE);
		rcrcptr = (c->crc)+(4*blocknum);
		bcrc = get32bit(&rcrcptr);
		if (bcrc!=crc) {
			errno = 0;
			hdd_error_occured(c);   // uses and preserves errno !!!
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - crc error",c->filename);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		if (ret!=MFSBLOCKSIZE) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"read_block_from_chunk: file:%s - read error",c->filename);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
	} else {
#ifdef PRESERVE_BLOCK
		if (c->blockno != blocknum) {
			ts = get_usectime();
#ifdef USE_PIO
			ret = pread(c->fd,c->block,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
#else /* USE_PIO */
			lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS),SEEK_SET);
			ret = read(c->fd,c->block,MFSBLOCKSIZE);
#endif /* USE_PIO */
			te = get_usectime();
			hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
			c->blockno = blocknum;
		} else {
			ret = MFSBLOCKSIZE;
		}
		precrc = mycrc32(0,c->block,offset);
		crc = mycrc32(0,c->block+offset,size);
		postcrc = mycrc32(0,c->block+offset+size,MFSBLOCKSIZE-(offset+size));
#else /* PRESERVE_BLOCK */
		ts = get_usectime();
#ifdef USE_PIO
		ret = pread(c->fd,blockbuffer,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS),SEEK_SET);
		ret = read(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* USE_PIO */
		te = get_usectime();
		hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
//              crc = mycrc32(0,blockbuffer+offset,size);       // first calc crc for piece
		precrc = mycrc32(0,blockbuffer,offset);
		crc = mycrc32(0,blockbuffer+offset,size);
		postcrc = mycrc32(0,blockbuffer+offset+size,MFSBLOCKSIZE-(offset+size));
#endif /* PRESERVE_BLOCK */
		if (offset==0) {
			combinedcrc = mycrc32_combine(crc,postcrc,MFSBLOCKSIZE-(offset+size));
		} else {
			combinedcrc = mycrc32_combine(precrc,crc,size);
			if ((offset+size)<MFSBLOCKSIZE) {
				combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			}
		}
		rcrcptr = (c->crc)+(4*blocknum);
		bcrc = get32bit(&rcrcptr);
		if (bcrc!=combinedcrc) {
			errno = 0;
			hdd_error_occured(c);   // uses and preserves errno !!!
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - crc error",c->filename);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		if (ret!=MFSBLOCKSIZE) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"read_block_from_chunk: file:%s - read error",c->filename);
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
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = malloc(MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
#endif /* PRESERVE_BLOCK */
	c = hdd_chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=MFSBLOCKSINCHUNK) {
		hdd_chunk_release(c);
		return ERROR_BNUMTOOBIG;
	}
	if (size>MFSBLOCKSIZE) {
		hdd_chunk_release(c);
		return ERROR_WRONGSIZE;
	}
	if ((offset>=MFSBLOCKSIZE) || (offset+size>MFSBLOCKSIZE)) {
		hdd_chunk_release(c);
		return ERROR_WRONGOFFSET;
	}
	crc = get32bit(&crcbuff);
	if (crc!=mycrc32(0,buffer,size)) {
		hdd_chunk_release(c);
		return ERROR_CRC;
	}
	if (offset==0 && size==MFSBLOCKSIZE) {
		if (blocknum>=c->blocks) {
			wcrcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				put32bit(&wcrcptr,emptyblockcrc);
			}
			c->blocks = blocknum+1;
		}
		ts = get_usectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,buffer,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS),SEEK_SET);
		ret = write(c->fd,buffer,MFSBLOCKSIZE);
#endif /* USE_PIO */
		te = get_usectime();
		hdd_stats_datawrite(c->owner,MFSBLOCKSIZE,te-ts);
		if (crc!=mycrc32(0,buffer,MFSBLOCKSIZE)) {
			errno = 0;
			hdd_error_occured(c);
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		wcrcptr = (c->crc)+(4*blocknum);
		put32bit(&wcrcptr,crc);
		c->crcchanged = 1;
		if (ret!=MFSBLOCKSIZE) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"write_block_to_chunk: file:%s - write error",c->filename);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		memcpy(c->block,buffer,MFSBLOCKSIZE);
		c->blockno = blocknum;
#endif /* PRESERVE_BLOCK */
	} else {
		if (blocknum<c->blocks) {
#ifdef PRESERVE_BLOCK
			if (c->blockno != blocknum) {
				ts = get_usectime();
#ifdef USE_PIO
				ret = pread(c->fd,c->block,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
#else /* USE_PIO */
				lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS),SEEK_SET);
				ret = read(c->fd,c->block,MFSBLOCKSIZE);
#endif /* USE_PIO */
				te = get_usectime();
				hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
				c->blockno = blocknum;
			} else {
				ret = MFSBLOCKSIZE;
			}
#else /* PRESERVE_BLOCK */
			ts = get_usectime();
#ifdef USE_PIO
			ret = pread(c->fd,blockbuffer,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
#else /* USE_PIO */
			lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS),SEEK_SET);
			ret = read(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* USE_PIO */
			te = get_usectime();
			hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
#endif /* PRESERVE_BLOCK */
			if (ret!=MFSBLOCKSIZE) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"write_block_to_chunk: file:%s - read error",c->filename);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			precrc = mycrc32(0,c->block,offset);
			chcrc = mycrc32(0,c->block+offset,size);
			postcrc = mycrc32(0,c->block+offset+size,MFSBLOCKSIZE-(offset+size));
#else /* PRESERVE_BLOCK */
			precrc = mycrc32(0,blockbuffer,offset);
			chcrc = mycrc32(0,blockbuffer+offset,size);
			postcrc = mycrc32(0,blockbuffer+offset+size,MFSBLOCKSIZE-(offset+size));
#endif /* PRESERVE_BLOCK */
			if (offset==0) {
				combinedcrc = mycrc32_combine(chcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			} else {
				combinedcrc = mycrc32_combine(precrc,chcrc,size);
				if ((offset+size)<MFSBLOCKSIZE) {
					combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
				}
			}
			rcrcptr = (c->crc)+(4*blocknum);
			bcrc = get32bit(&rcrcptr);
			if (bcrc!=combinedcrc) {
				errno = 0;
				hdd_error_occured(c);   // uses and preserves errno !!!
				syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_CRC;
			}
		} else {
			if (ftruncate(c->fd,CHUNKHDRSIZE+(((uint32_t)(blocknum+1))<<MFSBLOCKBITS))<0) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"write_block_to_chunk: file:%s - ftruncate error",c->filename);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
			wcrcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				put32bit(&wcrcptr,emptyblockcrc);
			}
			c->blocks = blocknum+1;
#ifdef PRESERVE_BLOCK
			memset(c->block,0,MFSBLOCKSIZE);
			c->blockno = blocknum;
#else /* PRESERVE_BLOCK */
			memset(blockbuffer,0,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
			precrc = mycrc32_zeroblock(0,offset);
			postcrc = mycrc32_zeroblock(0,MFSBLOCKSIZE-(offset+size));
		}
#ifdef PRESERVE_BLOCK
		memcpy(c->block+offset,buffer,size);
		ts = get_usectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,c->block+offset,size,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS)+offset);
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS)+offset,SEEK_SET);
		ret = write(c->fd,c->block+offset,size);
#endif /* USE_PIO */
		te = get_usectime();
		hdd_stats_datawrite(c->owner,size,te-ts);
		chcrc = mycrc32(0,c->block+offset,size);
#else /* PRESERVE_BLOCK */
		memcpy(blockbuffer+offset,buffer,size);
		ts = get_usectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,blockbuffer+offset,size,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS)+offset);
#else /* USE_PIO */
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS)+offset,SEEK_SET);
		ret = write(c->fd,blockbuffer+offset,size);
#endif /* USE_PIO */
		te = get_usectime();
		hdd_stats_datawrite(c->owner,size,te-ts);
		chcrc = mycrc32(0,blockbuffer+offset,size);
#endif /* PRESERVE_BLOCK */
		if (offset==0) {
			combinedcrc = mycrc32_combine(chcrc,postcrc,MFSBLOCKSIZE-(offset+size));
		} else {
			combinedcrc = mycrc32_combine(precrc,chcrc,size);
			if ((offset+size)<MFSBLOCKSIZE) {
				combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			}
		}
		wcrcptr = (c->crc)+(4*blocknum);
		put32bit(&wcrcptr,combinedcrc);
		c->crcchanged = 1;
		if (crc!=chcrc) {
			errno = 0;
			hdd_error_occured(c);   // uses and preserves errno !!!
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
		if (ret!=(int)size) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"write_block_to_chunk: file:%s - write error",c->filename);
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(c);
		return status;
	}
	*checksum = mycrc32(0,c->crc,4096);
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(c);
		return status;
	}
	memcpy(checksum_tab,c->crc,4096);
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}





/* chunk operations */

static int hdd_int_create(uint64_t chunkid,uint32_t version) {
	folder *f;
	chunk *c;
	int status;
	uint8_t *ptr;
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKHDRSIZE];
#else /* PRESERVE_BLOCK */
	uint8_t *hdrbuffer;
#endif /* PRESERVE_BLOCK */

	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f,chunkid,version);
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		return ERROR_CHUNKEXIST;
	}

#ifndef PRESERVE_BLOCK
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = malloc(CHUNKHDRSIZE);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
#endif /* PRESERVE_BLOCK */

	status = hdd_io_begin(c,1);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_delete(c);
		return ERROR_IO;
	}
	memset(hdrbuffer,0,CHUNKHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.0",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (write(c->fd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		mfs_arg_errlog_silent(LOG_WARNING,"create_newchunk: file:%s - write error",c->filename);
		hdd_io_end(c);
		unlink(c->filename);
		hdd_chunk_delete(c);
		return ERROR_IO;
	}
	hdd_stats_write(CHUNKHDRSIZE);
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename);
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
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = malloc(MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	lseek(c->fd,CHUNKHDRSIZE,SEEK_SET);
	ptr = c->crc;
	for (block=0 ; block<c->blocks ; block++) {
#ifdef PRESERVE_BLOCK
		retsize = read(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
		retsize = read(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
		if (retsize!=MFSBLOCKSIZE) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"test_chunk: file:%s - data read error",c->filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		hdd_stats_read(MFSBLOCKSIZE);
#ifdef PRESERVE_BLOCK
		c->blockno = block;
#endif
		bcrc = get32bit(&ptr);
#ifdef PRESERVE_BLOCK
		if (bcrc!=mycrc32(0,c->block,MFSBLOCKSIZE)) {
#else /* PRESERVE_BLOCK */
		if (bcrc!=mycrc32(0,blockbuffer,MFSBLOCKSIZE)) {
#endif /* PRESERVE_BLOCK */
			errno = 0;      // set anything to errno
			hdd_error_occured(c);   // uses and preserves errno !!!
			syslog(LOG_WARNING,"test_chunk: file:%s - crc error",c->filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			return ERROR_CRC;
		}
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}

static int hdd_int_duplicate(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion) {
	folder *f;
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
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = malloc(MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = malloc(CHUNKHDRSIZE);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
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
		copyversion = newversion;
	}
	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		hdd_chunk_release(oc);
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f,copychunkid,copyversion);
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		hdd_chunk_release(oc);
		return ERROR_CHUNKEXIST;
	}

	if (newversion!=version) {
		filenameleng = strlen(oc->filename);
		if (oc->filename[filenameleng-13]=='_') {       // new file name format
			newfilename = (char*) malloc(filenameleng+1);
			passert(newfilename);
			memcpy(newfilename,c->filename,filenameleng+1);
			sprintf(newfilename+filenameleng-12,"%08" PRIX32 ".mfs",newversion);
			if (rename(oc->filename,newfilename)<0) {
				hdd_error_occured(oc);  // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"duplicate_chunk: file:%s - rename error",oc->filename);
				free(newfilename);
				hdd_chunk_delete(c);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			free(oc->filename);
			oc->filename = newfilename;
		}
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;  //can't change file version
		}
		ptr = vbuff;
		put32bit(&ptr,newversion);
#ifdef USE_PIO
		if (pwrite(oc->fd,vbuff,4,16)!=4) {
#else /* USE_PIO */
		lseek(oc->fd,16,SEEK_SET);
		if (write(oc->fd,vbuff,4)!=4) {
#endif /* USE_PIO */
			hdd_error_occured(oc);  // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"duplicate_chunk: file:%s - write error",c->filename);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		hdd_stats_write(4);
		oc->version = newversion;
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(oc);
			return status;
		}
	}
	status = hdd_io_begin(c,1);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return status;
	}
	memset(hdrbuffer,0,CHUNKHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.0",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	memcpy(c->crc,oc->crc,4096);
	memcpy(hdrbuffer+1024,oc->crc,4096);
	if (write(c->fd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		mfs_arg_errlog_silent(LOG_WARNING,"duplicate_chunk: file:%s - hdr write error",c->filename);
		hdd_io_end(c);
		unlink(c->filename);
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
			memcpy(c->block,oc->block,MFSBLOCKSIZE);
			retsize = MFSBLOCKSIZE;
		} else {
#ifdef USE_PIO
			retsize = pread(oc->fd,c->block,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
#else /* USE_PIO */
			lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS),SEEK_SET);
			retsize = read(oc->fd,c->block,MFSBLOCKSIZE);
#endif /* USE_PIO */
		}
#else /* PRESERVE_BLOCK */
		retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
		if (retsize!=MFSBLOCKSIZE) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"duplicate_chunk: file:%s - data read error",oc->filename);
			hdd_io_end(c);
			unlink(c->filename);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		if (oc->blockno!=block) {
			hdd_stats_read(MFSBLOCKSIZE);
		}
		retsize = write(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
		hdd_stats_read(MFSBLOCKSIZE);
		retsize = write(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
		if (retsize!=MFSBLOCKSIZE) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"duplicate_chunk: file:%s - data write error",c->filename);
			hdd_io_end(c);
			unlink(c->filename);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;        //write error
		}
		hdd_stats_write(MFSBLOCKSIZE);
#ifdef PRESERVE_BLOCK
		c->blockno = block;
#endif /* PRESERVE_BLOCK */
	}
	status = hdd_io_end(oc);
	if (status!=STATUS_OK) {
		hdd_error_occured(oc);  // uses and preserves errno !!!
		hdd_io_end(c);
		unlink(c->filename);
		hdd_chunk_delete(c);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = oc->blocks;
	zassert(pthread_mutex_lock(&folderlock));
	c->owner->needrefresh = 1;
	zassert(pthread_mutex_unlock(&folderlock));
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
	if (c->filename[filenameleng-13]=='_') {        // new file name format
		newfilename = (char*) malloc(filenameleng+1);
		passert(newfilename);
		memcpy(newfilename,c->filename,filenameleng+1);
		sprintf(newfilename+filenameleng-12,"%08" PRIX32 ".mfs",newversion);
		if (rename(c->filename,newfilename)<0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"set_chunk_version: file:%s - rename error",c->filename);
			free(newfilename);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		free(c->filename);
		c->filename = newfilename;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		mfs_arg_errlog_silent(LOG_WARNING,"set_chunk_version: file:%s - open error",c->filename);
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		mfs_arg_errlog_silent(LOG_WARNING,"set_chunk_version: file:%s - write error",c->filename);
		hdd_io_end(c);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	hdd_stats_write(4);
	c->version = newversion;
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
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
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = malloc(MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
#endif /* !PRESERVE_BLOCK */
	if (length>MFSCHUNKSIZE) {
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
	if (c->filename[filenameleng-13]=='_') {        // new file name format
		newfilename = (char*) malloc(filenameleng+1);
		passert(newfilename);
		memcpy(newfilename,c->filename,filenameleng+1);
		sprintf(newfilename+filenameleng-12,"%08" PRIX32 ".mfs",newversion);
		if (rename(c->filename,newfilename)<0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"truncate_chunk: file:%s - rename error",c->filename);
			free(newfilename);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		free(c->filename);
		c->filename = newfilename;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;  //can't change file version
	}
	ptr = vbuff;
	put32bit(&ptr,newversion);
#ifdef USE_PIO
	if (pwrite(c->fd,vbuff,4,16)!=4) {
#else /* USE_PIO */
	lseek(c->fd,16,SEEK_SET);
	if (write(c->fd,vbuff,4)!=4) {
#endif /* USE_PIO */
		hdd_error_occured(c);   // uses and preserves errno !!!
		mfs_arg_errlog_silent(LOG_WARNING,"truncate_chunk: file:%s - write error",c->filename);
		hdd_io_end(c);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	hdd_stats_write(4);
	c->version = newversion;
	// step 2. truncate
	blocks = ((length+MFSBLOCKMASK)>>MFSBLOCKBITS);
	if (blocks>c->blocks) {
		if (ftruncate(c->fd,CHUNKHDRSIZE+(blocks<<MFSBLOCKBITS))<0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error",c->filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		ptr = (c->crc)+(4*(c->blocks));
		for (i=c->blocks ; i<blocks ; i++) {
			put32bit(&ptr,emptyblockcrc);
		}
		c->crcchanged = 1;
	} else {
		uint32_t blocknum = length>>MFSBLOCKBITS;
		uint32_t blockpos = length&MFSCHUNKBLOCKMASK;
		uint32_t blocksize = length&MFSBLOCKMASK;
		if (ftruncate(c->fd,CHUNKHDRSIZE+length)<0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error",c->filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		if (blocksize>0) {
			if (ftruncate(c->fd,CHUNKHDRSIZE+(blocks<<MFSBLOCKBITS))<0) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error",c->filename);
				hdd_io_end(c);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (c->blockno>=blocks) {
				c->blockno = 0xFFFF;    // invalidate truncated block
			}
			if (c->blockno!=(blockpos>>MFSBLOCKBITS)) {

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
				hdd_error_occured(c);   // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"truncate_chunk: file:%s - read error",c->filename);
				hdd_io_end(c);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
			hdd_stats_read(blocksize);
#ifdef PRESERVE_BLOCK
			}
			memset(c->block+blocksize,0,MFSBLOCKSIZE-blocksize);
			c->blockno = blockpos>>MFSBLOCKBITS;
			i = mycrc32_zeroexpanded(0,c->block,blocksize,MFSBLOCKSIZE-blocksize);
#else /* PRESERVE_BLOCK */
			i = mycrc32_zeroexpanded(0,blockbuffer,blocksize,MFSBLOCKSIZE-blocksize);
#endif /* PRESERVE_BLOCK */
			ptr = (c->crc)+(4*blocknum);
			put32bit(&ptr,i);
			c->crcchanged = 1;
		}
	}
	if (c->blocks != blocks) {
		zassert(pthread_mutex_lock(&folderlock));
		c->owner->needrefresh = 1;
		zassert(pthread_mutex_unlock(&folderlock));
	}
	c->blocks = blocks;
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
	}
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_duptrunc(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
	folder *f;
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
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = malloc(MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = malloc(CHUNKHDRSIZE);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
#endif /* PRESERVE_BLOCK */

	if (length>MFSCHUNKSIZE) {
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
		copyversion = newversion;
	}
	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		hdd_chunk_release(oc);
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f,copychunkid,copyversion);
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		hdd_chunk_release(oc);
		return ERROR_CHUNKEXIST;
	}

	if (newversion!=version) {
		filenameleng = strlen(oc->filename);
		if (oc->filename[filenameleng-13]=='_') {       // new file name format
			newfilename = (char*) malloc(filenameleng+1);
			passert(newfilename);
			memcpy(newfilename,c->filename,filenameleng+1);
			sprintf(newfilename+filenameleng-12,"%08" PRIX32 ".mfs",newversion);
			if (rename(oc->filename,newfilename)<0) {
				hdd_error_occured(oc);  // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"duplicate_chunk: file:%s - rename error",oc->filename);
				free(newfilename);
				hdd_chunk_delete(c);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			free(oc->filename);
			oc->filename = newfilename;
		}
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;  //can't change file version
		}
		ptr = vbuff;
		put32bit(&ptr,newversion);
#ifdef USE_PIO
		if (pwrite(oc->fd,vbuff,4,16)!=4) {
#else /* USE_PIO */
		lseek(oc->fd,16,SEEK_SET);
		if (write(oc->fd,vbuff,4)!=4) {
#endif /* USE_PIO */
			hdd_error_occured(oc);  // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - write error",c->filename);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		hdd_stats_write(4);
		oc->version = newversion;
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_report_damaged_chunk(chunkid);
			hdd_chunk_release(oc);
			return status;
		}
	}
	status = hdd_io_begin(c,1);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return status;
	}
	blocks = ((length+MFSBLOCKMASK)>>MFSBLOCKBITS);
	memset(hdrbuffer,0,CHUNKHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.0",8);
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
				memcpy(c->block,oc->block,MFSBLOCKSIZE);
				retsize = MFSBLOCKSIZE;
			} else {
#ifdef USE_PIO
				retsize = pread(oc->fd,c->block,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
#else /* USE_PIO */
				lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS),SEEK_SET);
				retsize = read(oc->fd,c->block,MFSBLOCKSIZE);
#endif /* USE_PIO */
			}
#else /* PRESERVE_BLOCK */
			retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
			if (retsize!=MFSBLOCKSIZE) {
				hdd_error_occured(oc);  // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data read error",oc->filename);
				hdd_io_end(c);
				unlink(c->filename);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (oc->blockno!=block) {
				hdd_stats_read(MFSBLOCKSIZE);
			}
			retsize = write(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
			hdd_stats_read(MFSBLOCKSIZE);
			retsize = write(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
			if (retsize!=MFSBLOCKSIZE) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data write error",c->filename);
				hdd_io_end(c);
				unlink(c->filename);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_write(MFSBLOCKSIZE);
#ifdef PRESERVE_BLOCK
			c->blockno = block;
#endif /* PRESERVE_BLOCK */
		}
		if (ftruncate(c->fd,CHUNKHDRSIZE+(((uint32_t)blocks)<<MFSBLOCKBITS))<0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - ftruncate error",c->filename);
			hdd_io_end(c);
			unlink(c->filename);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;        //write error
		}
		ptr = hdrbuffer+CHUNKHDRCRC+4*(oc->blocks);
		for (block=oc->blocks ; block<blocks ; block++) {
			put32bit(&ptr,emptyblockcrc);
		}
	} else { // shrinking
		uint32_t blocksize = (length&MFSBLOCKMASK);
		if (blocksize==0) { // aligned shring
			for (block=0 ; block<blocks ; block++) {
#ifdef PRESERVE_BLOCK
				if (oc->blockno==block) {
					memcpy(c->block,oc->block,MFSBLOCKSIZE);
					retsize = MFSBLOCKSIZE;
				} else {
#ifdef USE_PIO
					retsize = pread(oc->fd,c->block,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
#else /* USE_PIO */
					lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS),SEEK_SET);
					retsize = read(oc->fd,c->block,MFSBLOCKSIZE);
#endif /* USE_PIO */
				}
#else /* PRESERVE_BLOCK */
				retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
				if (retsize!=MFSBLOCKSIZE) {
					hdd_error_occured(oc);  // uses and preserves errno !!!
					mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data read error",oc->filename);
					hdd_io_end(c);
					unlink(c->filename);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_report_damaged_chunk(chunkid);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
#ifdef PRESERVE_BLOCK
				if (oc->blockno!=block) {
					hdd_stats_read(MFSBLOCKSIZE);
				}
				retsize = write(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
				hdd_stats_read(MFSBLOCKSIZE);
				retsize = write(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
				if (retsize!=MFSBLOCKSIZE) {
					hdd_error_occured(c);   // uses and preserves errno !!!
					mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data write error",c->filename);
					hdd_io_end(c);
					unlink(c->filename);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
				hdd_stats_write(MFSBLOCKSIZE);
#ifdef PRESERVE_BLOCK
				c->blockno = block;
#endif /* PRESERVE_BLOCK */
			}
		} else { // misaligned shrink
			for (block=0 ; block<blocks-1 ; block++) {
#ifdef PRESERVE_BLOCK
				if (oc->blockno==block) {
					memcpy(c->block,oc->block,MFSBLOCKSIZE);
					retsize = MFSBLOCKSIZE;
				} else {
#ifdef USE_PIO
					retsize = pread(oc->fd,c->block,MFSBLOCKSIZE,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
#else /* USE_PIO */
					lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS),SEEK_SET);
					retsize = read(oc->fd,c->block,MFSBLOCKSIZE);
#endif /* USE_PIO */
				}
#else /* PRESERVE_BLOCK */
				retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
				if (retsize!=MFSBLOCKSIZE) {
					hdd_error_occured(oc);  // uses and preserves errno !!!
					mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data read error",oc->filename);
					hdd_io_end(c);
					unlink(c->filename);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_report_damaged_chunk(chunkid);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
#ifdef PRESERVE_BLOCK
				if (oc->blockno!=block) {
					hdd_stats_read(MFSBLOCKSIZE);
				}
				retsize = write(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
				hdd_stats_read(MFSBLOCKSIZE);
				retsize = write(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
				if (retsize!=MFSBLOCKSIZE) {
					hdd_error_occured(c);   // uses and preserves errno !!!
					mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data write error",c->filename);
					hdd_io_end(c);
					unlink(c->filename);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return ERROR_IO;        //write error
				}
				hdd_stats_write(MFSBLOCKSIZE);
			}
			block = blocks-1;
#ifdef PRESERVE_BLOCK
			if (oc->blockno==block) {
				memcpy(c->block,oc->block,blocksize);
				retsize = blocksize;
			} else {
#ifdef USE_PIO
				retsize = pread(oc->fd,c->block,blocksize,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
#else /* USE_PIO */
				lseek(oc->fd,CHUNKHDRSIZE+(((uint32_t)block)<<MFSBLOCKBITS),SEEK_SET);
				retsize = read(oc->fd,c->block,blocksize);
#endif /* USE_PIO */
			}
#else /* PRESERVE_BLOCK */
			retsize = read(oc->fd,blockbuffer,blocksize);
#endif /* PRESERVE_BLOCK */
			if (retsize!=(signed)blocksize) {
				hdd_error_occured(oc);  // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data read error",oc->filename);
				hdd_io_end(c);
				unlink(c->filename);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (oc->blockno!=block) {
				hdd_stats_read(blocksize);
			}
			memset(c->block+blocksize,0,MFSBLOCKSIZE-blocksize);
			retsize = write(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
			hdd_stats_read(blocksize);
			memset(blockbuffer+blocksize,0,MFSBLOCKSIZE-blocksize);
			retsize = write(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
			if (retsize!=MFSBLOCKSIZE) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - data write error",c->filename);
				hdd_io_end(c);
				unlink(c->filename);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_write(MFSBLOCKSIZE);
			ptr = hdrbuffer+CHUNKHDRCRC+4*(blocks-1);
#ifdef PRESERVE_BLOCK
			crc = mycrc32_zeroexpanded(0,c->block,blocksize,MFSBLOCKSIZE-blocksize);
#else /* PRESERVE_BLOCK */
			crc = mycrc32_zeroexpanded(0,blockbuffer,blocksize,MFSBLOCKSIZE-blocksize);
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		mfs_arg_errlog_silent(LOG_WARNING,"duptrunc_chunk: file:%s - hdr write error",c->filename);
		hdd_io_end(c);
		unlink(c->filename);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return ERROR_IO;
	}
	hdd_stats_write(CHUNKHDRSIZE);
	status = hdd_io_end(oc);
	if (status!=STATUS_OK) {
		hdd_error_occured(oc);  // uses and preserves errno !!!
		hdd_io_end(c);
		unlink(c->filename);
		hdd_chunk_delete(c);
		hdd_report_damaged_chunk(chunkid);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = blocks;
	zassert(pthread_mutex_lock(&folderlock));
	c->owner->needrefresh = 1;
	zassert(pthread_mutex_unlock(&folderlock));
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
		hdd_error_occured(c);   // uses and preserves errno !!!
		mfs_arg_errlog_silent(LOG_WARNING,"delete_chunk: file:%s - unlink error",c->filename);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	hdd_chunk_delete(c);
	return STATUS_OK;
}

/* all chunk operations in one call */
// newversion>0 && length==0xFFFFFFFF && copychunkid==0   -> change version
// newversion>0 && length==0xFFFFFFFF && copycnunkid>0    -> duplicate
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid==0    -> truncate
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid>0     -> duplicate and truncate
// newversion==0 && length==0                             -> delete
// newversion==0 && length==1                             -> create
// newversion==0 && length==2                             -> check chunk contents
int hdd_chunkop(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
	zassert(pthread_mutex_lock(&statslock));
	if (newversion>0) {
		if (length==0xFFFFFFFF) {
			if (copychunkid==0) {
				stats_version++;
			} else {
				stats_duplicate++;
			}
		} else if (length<=MFSCHUNKSIZE) {
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
	zassert(pthread_mutex_unlock(&statslock));
	if (newversion>0) {
		if (length==0xFFFFFFFF) {
			if (copychunkid==0) {
				return hdd_int_version(chunkid,version,newversion);
			} else {
				return hdd_int_duplicate(chunkid,version,newversion,copychunkid,copyversion);
			}
		} else if (length<=MFSCHUNKSIZE) {
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

void* hdd_tester_thread(void* arg) {
	folder *f,*of;
	chunk *c;
	uint64_t chunkid;
	uint32_t version;
	uint32_t freq;
	uint32_t cnt;
	uint64_t st,en;
	char *path;

	f = folderhead;
	freq = HDDTestFreq;
	cnt = 0;
	for (;;) {
		st = get_usectime();
		path = NULL;
		chunkid = 0;
		version = 0;
		zassert(pthread_mutex_lock(&folderlock));
		zassert(pthread_mutex_lock(&hashlock));
		zassert(pthread_mutex_lock(&testlock));
		if (testerreset) {
			testerreset = 0;
			f = folderhead;
			freq = HDDTestFreq;
			cnt = 0;
		}
		cnt++;
		if (cnt<freq || freq==0 || folderactions==0 || folderhead==NULL) {
			path = NULL;
		} else {
			cnt = 0;
			of = f;
			do {
				f = f->next;
				if (f==NULL) {
					f = folderhead;
				}
			} while ((f->damaged || f->todel || f->toremove || f->scanstate!=SCST_WORKING) && of!=f);
			if (of==f && (f->damaged || f->todel || f->toremove || f->scanstate!=SCST_WORKING)) {   // all folders are unavailable
				path = NULL;
			} else {
				c = f->testhead;
				if (c && c->state==CH_AVAIL) {
					chunkid = c->chunkid;
					version = c->version;
					path = strdup(c->filename);
					passert(path);
				}
			}
		}
		zassert(pthread_mutex_unlock(&testlock));
		zassert(pthread_mutex_unlock(&hashlock));
		zassert(pthread_mutex_unlock(&folderlock));
		if (path) {
			syslog(LOG_NOTICE,"testing chunk: %s",path);
			if (hdd_int_test(chunkid,version)!=STATUS_OK) {
				hdd_report_damaged_chunk(chunkid);
			}
			free(path);
		}
		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));
		en = get_usectime();
		if (en>st) {
			en-=st;
			if (en<1000000) {
				usleep(1000000-en);
			}
		}
	}
	return arg;
}

void hdd_testshuffle(folder *f) {
	uint32_t i,j,chunksno;
	chunk **csorttab,*c;
	zassert(pthread_mutex_lock(&testlock));
	chunksno = 0;
	for (c=f->testhead ; c ; c=c->testnext) {
		chunksno++;
	}
	if (chunksno>0) {
		csorttab = (chunk**) malloc(sizeof(chunk*)*chunksno);
		passert(csorttab);
		chunksno = 0;
		for (c=f->testhead ; c ; c=c->testnext) {
			csorttab[chunksno++] = c;
		}
		if (chunksno>1) {
			for (i=0 ; i<chunksno-1 ; i++) {
				j = i+rndu32_ranged(chunksno-i);
				if (j!=i) {
					c = csorttab[i];
					csorttab[i] = csorttab[j];
					csorttab[j] = c;
				}
			}
		}
	} else {
		csorttab = NULL;
	}
	f->testhead = NULL;
	f->testtail = &(f->testhead);
	for (i=0 ; i<chunksno ; i++) {
		c = csorttab[i];
		c->testnext = NULL;
		c->testprev = f->testtail;
		*(c->testprev) = c;
		f->testtail = &(c->testnext);
	}
	if (csorttab) {
		free(csorttab);
	}
	zassert(pthread_mutex_unlock(&testlock));
}

/* initialization */

static inline int hdd_check_filename(const char *fname,uint64_t *chunkid,uint32_t *version) {
	uint64_t namechunkid;
	uint32_t nameversion;
	char ch;
	uint32_t i;

	if (strncmp(fname,"chunk_",6)!=0) {
		return -1;
	}
	namechunkid = 0;
	nameversion = 0;
	for (i=6 ; i<22 ; i++) {
		ch = fname[i];
		if (ch>='0' && ch<='9') {
			ch-='0';
		} else if (ch>='A' && ch<='F') {
			ch-='A'-10;
		} else {
			return -1;
		}
		namechunkid *= 16;
		namechunkid += ch;
	}
	if (fname[22]!='_') {
		return -1;
	}
	for (i=23 ; i<31 ; i++) {
		ch = fname[i];
		if (ch>='0' && ch<='9') {
			ch-='0';
		} else if (ch>='A' && ch<='F') {
			ch-='A'-10;
		} else {
			return -1;
		}
		nameversion *= 16;
		nameversion += ch;
	}
	if (strcmp(fname+31,".mfs")!=0) {
		return -1;
	}
	*chunkid = namechunkid;
	*version = nameversion;
	return 0;
}

static inline void hdd_add_chunk(folder *f,const char *fullname,uint64_t chunkid,uint32_t version,uint8_t todel) {
	folder *prevf;
	chunk *c;

	prevf = NULL;
	c = hdd_chunk_get(chunkid,CH_NEW_AUTO);
	if (c->filename!=NULL) {        // already have this chunk
		if (version <= c->version) {    // current chunk is older
			if (todel<2) { // this is R/W fs?
				unlink(fullname); // if yes then remove file
			}
		} else {
			prevf = c->owner;
			if (c->todel<2) { // current chunk is on R/W fs?
				unlink(c->filename); // if yes then remove file
			}
			free(c->filename);
			c->filename = strdup(fullname);
			passert(c->filename);
			c->version = version;
			c->blocks = 0; // (sb.st_size - CHUNKHDRSIZE) / MFSBLOCKSIZE;
			c->owner = f;
			c->todel = todel;
			zassert(pthread_mutex_lock(&testlock));
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
			zassert(pthread_mutex_unlock(&testlock));
		}
	} else {
		c->filename = strdup(fullname);
		passert(c->filename);
		c->version = version;
		c->blocks = 0;
		c->owner = f;
		c->todel = todel;
		zassert(pthread_mutex_lock(&testlock));
		c->testprev = f->testtail;
		*(c->testprev) = c;
		f->testtail = &(c->testnext);
		zassert(pthread_mutex_unlock(&testlock));
		hdd_report_new_chunk(c->chunkid,c->version|(todel?0x80000000:0));
	}
	hdd_chunk_release(c);
	zassert(pthread_mutex_lock(&folderlock));
	if (prevf) {
		prevf->chunkcount--;
	}
	f->chunkcount++;
	zassert(pthread_mutex_unlock(&folderlock));
}

void* hdd_folder_scan(void *arg) {
	folder *f = (folder*)arg;
	DIR *dd;
	struct dirent *de,*destorage;
	uint16_t subf;
	char *fullname,*oldfullname;
	uint8_t plen,oldplen;
	uint64_t namechunkid;
	uint32_t nameversion;
	uint32_t tcheckcnt;
	uint8_t scanterm,todel;
	uint8_t lastperc,currentperc;
	uint32_t lasttime,currenttime,begintime;

	begintime = time(NULL);

	zassert(pthread_mutex_lock(&folderlock));
	todel = f->todel;
	hdd_refresh_usage(f);
	zassert(pthread_mutex_unlock(&folderlock));

	plen = strlen(f->path);
	oldplen = plen;

	/* size of name added to size of structure because on some os'es d_name has size of 1 byte */
	destorage = (struct dirent*)malloc(sizeof(struct dirent)+pathconf(f->path,_PC_NAME_MAX)+1);
	passert(destorage);

	fullname = (char*) malloc(plen+39);
	passert(fullname);

	memcpy(fullname,f->path,plen);
	fullname[plen]='\0';
	if (todel==0) {
		mkdir(fullname,0755);
	}

	fullname[plen++]='_';
	fullname[plen++]='_';
	fullname[plen++]='/';
	fullname[plen]='\0';

	scanterm = 0;

	zassert(pthread_mutex_lock(&dclock));
	hddspacechanged = 1;
	zassert(pthread_mutex_unlock(&dclock));

	if (todel==0) {
		for (subf=0 ; subf<256 ; subf++) {
			fullname[plen-3]="0123456789ABCDEF"[subf>>4];
			fullname[plen-2]="0123456789ABCDEF"[subf&15];
			mkdir(fullname,0755);
		}

/* move chunks from "X/name" to "XX/name" */

		oldfullname = (char*) malloc(oldplen+38);
		passert(oldfullname);
		memcpy(oldfullname,f->path,oldplen);
		oldfullname[oldplen++]='_';
		oldfullname[oldplen++]='/';
		oldfullname[oldplen]='\0';

		for (subf=0 ; subf<16 ; subf++) {
			oldfullname[oldplen-2]="0123456789ABCDEF"[subf];
			oldfullname[oldplen]='\0';
			dd = opendir(oldfullname);
			if (dd==NULL) {
				continue;
			}
			while (readdir_r(dd,destorage,&de)==0 && de!=NULL) {
				if (hdd_check_filename(de->d_name,&namechunkid,&nameversion)<0) {
					continue;
				}
				memcpy(oldfullname+oldplen,de->d_name,36);
				memcpy(fullname+plen,de->d_name,36);
				fullname[plen-3]="0123456789ABCDEF"[(namechunkid>>4)&15];
				fullname[plen-2]="0123456789ABCDEF"[namechunkid&15];
				rename(oldfullname,fullname);
			}
			oldfullname[oldplen]='\0';
			rmdir(oldfullname);
			closedir(dd);
		}
		free(oldfullname);

	}
/* scan new file names */

	tcheckcnt = 0;
	lastperc = 0;
	lasttime = time(NULL);
	for (subf=0 ; subf<256 && scanterm==0 ; subf++) {
		fullname[plen-3]="0123456789ABCDEF"[subf>>4];
		fullname[plen-2]="0123456789ABCDEF"[subf&15];
		fullname[plen]='\0';
		dd = opendir(fullname);
		if (dd) {
			while (readdir_r(dd,destorage,&de)==0 && de!=NULL && scanterm==0) {
				if (hdd_check_filename(de->d_name,&namechunkid,&nameversion)<0) {
					continue;
				}
				memcpy(fullname+plen,de->d_name,36);
				hdd_add_chunk(f,fullname,namechunkid,nameversion,todel);
				tcheckcnt++;
				if (tcheckcnt>=1000) {
					zassert(pthread_mutex_lock(&folderlock));
					if (f->scanstate==SCST_SCANTERMINATE) {
						scanterm = 1;
					}
					zassert(pthread_mutex_unlock(&folderlock));
					// usleep(100000); - slow down scanning (also change 1000 in 'if' to something much smaller) - for tests
					tcheckcnt = 0;
				}
			}
			closedir(dd);
		}
			currenttime = time(NULL);
			currentperc = ((subf*100.0)/256.0);
			if (currentperc>lastperc && currenttime>lasttime) {
				lastperc=currentperc;
				lasttime=currenttime;
				zassert(pthread_mutex_lock(&folderlock));
				f->scanprogress = currentperc;
				zassert(pthread_mutex_unlock(&folderlock));
				zassert(pthread_mutex_lock(&dclock));
				hddspacechanged = 1; // report chunk count to master
				zassert(pthread_mutex_unlock(&dclock));
				syslog(LOG_NOTICE,"scanning folder %s: %" PRIu8 "%% (%" PRIu32 "s)",f->path,lastperc,currenttime-begintime);
			}
	}
	free(fullname);
	free(destorage);
//      fprintf(stderr,"hdd space manager: %s: %" PRIu32 " chunks found\n",f->path,f->chunkcount);

	hdd_testshuffle(f);

	zassert(pthread_mutex_lock(&folderlock));
		if (f->scanstate==SCST_SCANTERMINATE) {
			syslog(LOG_NOTICE,"scanning folder %s: interrupted",f->path);
		} else {
			syslog(LOG_NOTICE,"scanning folder %s: complete (%" PRIu32 "s)",f->path,(uint32_t)(time(NULL))-begintime);
		}
	f->scanstate = SCST_SCANFINISHED;
	f->scanprogress = 100;
	zassert(pthread_mutex_unlock(&folderlock));
	return NULL;
}

void* hdd_folders_thread(void *arg) {
	for (;;) {
		hdd_check_folders();
		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));
		sleep(1);
	}
	return arg;
}

void* hdd_delayed_thread(void *arg) {
	for (;;) {
		hdd_delayed_ops();
		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));
		sleep(DELAYEDSTEP);
	}
	return arg;
}

void hdd_term(void) {
	uint32_t i;
	folder *f,*fn;
	chunk *c,*cn;
	dopchunk *dc,*dcn;
	cntcond *cc,*ccn;
	lostchunk *lc,*lcn;
	newchunk *nc,*ncn;
	damagedchunk *dmc,*dmcn;

	zassert(pthread_attr_destroy(&thattr));
	zassert(pthread_mutex_lock(&termlock));
	i = term; // if term is non zero here then it means that threads have not been started, so do not join with them
	term = 1;
	zassert(pthread_mutex_unlock(&termlock));
	if (i==0) {
		zassert(pthread_join(testerthread,NULL));
		zassert(pthread_join(foldersthread,NULL));
		zassert(pthread_join(delayedthread,NULL));
	}
	zassert(pthread_mutex_lock(&folderlock));
	i = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->scanstate==SCST_SCANINPROGRESS) {
			f->scanstate = SCST_SCANTERMINATE;
		}
		if (f->scanstate==SCST_SCANTERMINATE || f->scanstate==SCST_SCANFINISHED) {
			i++;
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
//      syslog(LOG_NOTICE,"waiting for scanning threads (%" PRIu32 ")",i);
	while (i>0) {
		usleep(10000); // not very elegant solution.
		zassert(pthread_mutex_lock(&folderlock));
		for (f=folderhead ; f ; f=f->next) {
			if (f->scanstate==SCST_SCANFINISHED) {
				zassert(pthread_join(f->scanthread,NULL));
				f->scanstate = SCST_WORKING;    // any state - to prevent calling pthread_join again
				i--;
			}
		}
		zassert(pthread_mutex_unlock(&folderlock));
	}
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=hashtab[i] ; c ; c=cn) {
			cn = c->next;
			if (c->state==CH_AVAIL) {
				if (c->crcchanged) {
					syslog(LOG_WARNING,"hdd_term: CRC not flushed - writing now");
					if (chunk_writecrc(c)!=STATUS_OK) {
						mfs_arg_errlog_silent(LOG_WARNING,"hdd_term: file:%s - write error",c->filename);
					}
				}
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
			} else {
				syslog(LOG_WARNING,"hdd_term: locked chunk !!!");
			}
		}
	}
	for (f=folderhead ; f ; f=fn) {
		fn = f->next;
		if (f->lfd>=0) {
			close(f->lfd);
		}
		free(f->path);
		free(f);
	}
	for (i=0 ; i<DHASHSIZE ; i++) {
		for (dc=dophashtab[i] ; dc ; dc=dcn) {
			dcn = dc->next;
			free(dc);
		}
	}
	for (dc=newdopchunks ; dc ; dc=dcn) {
		dcn = dc->next;
		free(dc);
	}
	for (cc=cclist ; cc ; cc=ccn) {
		ccn = cc->next;
		if (cc->wcnt) {
			syslog(LOG_WARNING,"hddspacemgr (atexit): used cond !!!");
		} else {
			zassert(pthread_cond_destroy(&(cc->cond)));
		}
		free(cc);
	}
	for (nc=newchunks ; nc ; nc=ncn) {
		ncn = nc->next;
		free(nc);
	}
	for (lc=lostchunks ; lc ; lc=lcn) {
		lcn = lc->next;
		free(lc);
	}
	for (dmc=damagedchunks ; dmc ; dmc=dmcn) {
		dmcn = dmc->next;
		free(dmc);
	}
}

int hdd_size_parse(const char *str,uint64_t *ret) {
	uint64_t val,frac,fracdiv;
	double drval,mult;
	int f;
	val=0;
	frac=0;
	fracdiv=1;
	f=0;
	while (*str>='0' && *str<='9') {
		f=1;
		val*=10;
		val+=(*str-'0');
		str++;
	}
	if (*str=='.') {        // accept format ".####" (without 0)
		str++;
		while (*str>='0' && *str<='9') {
			fracdiv*=10;
			frac*=10;
			frac+=(*str-'0');
			str++;
		}
		if (fracdiv==1) {       // if there was '.' expect number afterwards
			return -1;
		}
	} else if (f==0) {      // but not empty string
		return -1;
	}
	if (str[0]=='\0' || (str[0]=='B' && str[1]=='\0')) {
		mult=1.0;
	} else if (str[0]!='\0' && (str[1]=='\0' || (str[1]=='B' && str[2]=='\0'))) {
		switch(str[0]) {
		case 'k':
			mult=1e3;
			break;
		case 'M':
			mult=1e6;
			break;
		case 'G':
			mult=1e9;
			break;
		case 'T':
			mult=1e12;
			break;
		case 'P':
			mult=1e15;
			break;
		case 'E':
			mult=1e18;
			break;
		default:
			return -1;
		}
	} else if (str[0]!='\0' && str[1]=='i' && (str[2]=='\0' || (str[2]=='B' && str[3]=='\0'))) {
		switch(str[0]) {
		case 'K':
			mult=1024.0;
			break;
		case 'M':
			mult=1048576.0;
			break;
		case 'G':
			mult=1073741824.0;
			break;
		case 'T':
			mult=1099511627776.0;
			break;
		case 'P':
			mult=1125899906842624.0;
			break;
		case 'E':
			mult=1152921504606846976.0;
			break;
		default:
			return -1;
		}
	} else {
		return -1;
	}
	drval = round(((double)frac/(double)fracdiv+(double)val)*mult);
	if (drval>18446744073709551615.0) {
		return -2;
	} else {
		*ret = drval;
	}
	return 1;
}

int hdd_parseline(char *hddcfgline) {
	uint32_t l,p;
	int lfd,td;
	char *pptr;
	char *lockfname;
	struct stat sb;
	folder *f;
	uint8_t lockneeded;
	uint64_t limit;
	uint8_t lmode;

	if (hddcfgline[0]=='#') {
		return 0;
	}
	l = strlen(hddcfgline);
	while (l>0 && (hddcfgline[l-1]=='\r' || hddcfgline[l-1]=='\n' || hddcfgline[l-1]==' ' || hddcfgline[l-1]=='\t')) {
		l--;
	}
	if (l==0) {
		return 0;
	}
	p = l;
	while (p>0 && hddcfgline[p-1]!=' ' && hddcfgline[p-1]!='\t') {
		p--;
	}
	lmode = 0;
	if (p>0) {
		if (hddcfgline[p]=='-') {
			if (hdd_size_parse(hddcfgline+p+1,&limit)>=0) {
				lmode = 1;
			}
		} if ((hddcfgline[p]>='0' && hddcfgline[p]<='9') || hddcfgline[p]=='.') {
			if (hdd_size_parse(hddcfgline+p,&limit)>=0) {
				lmode = 2;
			}
		}
		if (lmode) {
			l = p;
			while (l>0 && (hddcfgline[l-1]==' ' || hddcfgline[l-1]=='\t')) {
				l--;
			}
			if (l==0) {
				return 0;
			}
		}
	}
	if (hddcfgline[l-1]!='/') {
		hddcfgline[l]='/';
		hddcfgline[l+1]='\0';
		l++;
	} else {
		hddcfgline[l]='\0';
	}
	if (hddcfgline[0]=='*') {
		td = 1;
		pptr = hddcfgline+1;
		l--;
	} else {
		td = 0;
		pptr = hddcfgline;
	}
	zassert(pthread_mutex_lock(&folderlock));
	lockneeded = 1;
	for (f=folderhead ; f && lockneeded ; f=f->next) {
		if (strcmp(f->path,pptr)==0) {
			lockneeded = 0;
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));

	if (lmode==1) { // sanity checks
		if (limit<0x4000000) {
			mfs_arg_syslog(LOG_WARNING,"hdd space manager: limit on '%s' < chunk size - leaving so small space on hdd is not recommended",pptr);
		} else {
			struct statvfs fsinfo;

			if (statvfs(pptr,&fsinfo)<0) {
				mfs_arg_errlog(LOG_NOTICE,"hdd space manager: statvfs on '%s'",pptr);
			} else {
				uint64_t size = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));
				if (limit > size) {
					mfs_arg_syslog(LOG_WARNING,"hdd space manager: space to be left free on '%s' (%" PRIu64 ") is greater than real volume size (%" PRIu64 ") !!!",pptr,limit,size);
				}
			}
		}
	}
	if (lmode==2) { // sanity checks
		if (limit==0) {
			mfs_arg_syslog(LOG_WARNING,"hdd space manager: limit on '%s' set to zero - using real volume size",pptr);
			lmode = 0;
		} else {
			struct statvfs fsinfo;

			if (statvfs(pptr,&fsinfo)<0) {
				mfs_arg_errlog(LOG_NOTICE,"hdd space manager: statvfs on '%s'",pptr);
			} else {
				uint64_t size = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));
				if (limit > size) {
					mfs_arg_syslog(LOG_WARNING,"hdd space manager: limit on '%s' (%" PRIu64 ") is greater than real volume size (%" PRIu64 ") - using real volume size",pptr,limit,size);
					lmode = 0;
				}
			}
		}
	}

	lockfname = (char*)malloc(l+6);
	passert(lockfname);
	memcpy(lockfname,pptr,l);
	memcpy(lockfname+l,".lock",6);
	lfd = open(lockfname,O_RDWR|O_CREAT|O_TRUNC,0640);
	if (lfd<0 && errno==EROFS && td) {
		free(lockfname);
		td = 2;
	} else {
		if (lfd<0) {
			mfs_arg_errlog(LOG_ERR,"hdd space manager: can't create lock file '%s'",lockfname);
			free(lockfname);
			return -1;
		}
		if (lockneeded && lockf(lfd,F_TLOCK,0)<0) {
			if (errno==EAGAIN) {
				mfs_arg_syslog(LOG_ERR,"hdd space manager: data folder '%s' already locked (used by another process)",pptr);
			} else {
				mfs_arg_errlog(LOG_NOTICE,"hdd space manager: lockf '%s' error",lockfname);
			}
			free(lockfname);
			close(lfd);
			return -1;
		}
		if (fstat(lfd,&sb)<0) {
			mfs_arg_errlog(LOG_NOTICE,"hdd space manager: fstat '%s' error",lockfname);
			free(lockfname);
			close(lfd);
			return -1;
		}
		free(lockfname);
		if (lockneeded) {
			zassert(pthread_mutex_lock(&folderlock));
			for (f=folderhead ; f ; f=f->next) {
				if (f->devid==sb.st_dev) {
					if (f->lockinode==sb.st_ino) {
						mfs_arg_syslog(LOG_ERR,"hdd space manager: data folders '%s' and '%s have the same lockfile !!!",pptr,f->path);
						zassert(pthread_mutex_unlock(&folderlock));
						close(lfd);
						return -1;
					} else {
						mfs_arg_syslog(LOG_WARNING,"hdd space manager: data folders '%s' and '%s' are on the same physical device (could lead to unexpected behaviours)",pptr,f->path);
					}
				}
			}
			zassert(pthread_mutex_unlock(&folderlock));
		}
	}
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		if (strcmp(f->path,pptr)==0) {
			f->toremove = 0;
			if (f->damaged) {
				f->scanstate = SCST_SCANNEEDED;
				f->scanprogress = 0;
				f->damaged = 0;
				f->avail = 0ULL;
				f->total = 0ULL;
				if (lmode==1) {
					f->leavefree = limit;
				} else {
					f->leavefree = LeaveFree;
				}
				if (lmode==2) {
					f->sizelimit = limit;
				} else {
					f->sizelimit = 0;
				}
				f->chunkcount = 0;
				f->cstat.clear();
				for (l=0 ; l<STATSHISTORY ; l++) {
					f->stats[l].clear();
				}
				f->statspos = 0;
				for (l=0 ; l<LASTERRSIZE ; l++) {
					f->lasterrtab[l].chunkid = 0ULL;
					f->lasterrtab[l].timestamp = 0;
				}
				f->lasterrindx = 0;
				f->lastrefresh = 0;
				f->needrefresh = 1;
			} else {
				if ((f->todel==0 && td>0) || (f->todel>0 && td==0)) {
					// the change is important - chunks need to be send to master again
					f->scanstate = SCST_SENDNEEDED;
				}
			}
			f->todel = td;
			zassert(pthread_mutex_unlock(&folderlock));
			if (lfd>=0) {
				close(lfd);
			}
			return 1;
		}
	}
	f = (folder*)malloc(sizeof(folder));
	passert(f);
	f->todel = td;
	f->damaged = 0;
	f->scanstate = SCST_SCANNEEDED;
	f->scanprogress = 0;
	f->path = strdup(pptr);
	passert(f->path);
	f->toremove = 0;
	if (lmode==1) {
		f->leavefree = limit;
	} else {
		f->leavefree = LeaveFree;
	}
	if (lmode==2) {
		f->sizelimit = limit;
	} else {
		f->sizelimit = 0;
	}
	f->avail = 0ULL;
	f->total = 0ULL;
	f->chunkcount = 0;
	f->cstat.clear();
	for (l=0 ; l<STATSHISTORY ; l++) {
		f->stats[l].clear();
	}
	f->statspos = 0;
	for (l=0 ; l<LASTERRSIZE ; l++) {
		f->lasterrtab[l].chunkid = 0ULL;
		f->lasterrtab[l].timestamp = 0;
	}
	f->lasterrindx = 0;
	f->lastrefresh = 0;
	f->needrefresh = 1;
	f->devid = sb.st_dev;
	f->lockinode = sb.st_ino;
	f->lfd = lfd;
	f->testhead = NULL;
	f->testtail = &(f->testhead);
	f->carry = (double)(random()&0x7FFFFFFF)/(double)(0x7FFFFFFF);
	f->next = folderhead;
	folderhead = f;
	testerreset = 1;
	zassert(pthread_mutex_unlock(&folderlock));
	return 2;
}

int hdd_folders_reinit(void) {
	folder *f;
	FILE *fd;
	char buff[1000];
	char *hddfname;
	int ret,datadef;

	if (!cfg_isdefined("HDD_CONF_FILENAME")) {
		hddfname = strdup(ETC_PATH "/mfs/mfshdd.cfg");
		passert(hddfname);
		fd = fopen(hddfname,"r");
		if (!fd) {
			free(hddfname);
			hddfname = strdup(ETC_PATH "/mfshdd.cfg");
			fd = fopen(hddfname,"r");
			if (fd) {
				mfs_syslog(LOG_WARNING,"default sysconf path has changed - please move mfshdd.cfg from " ETC_PATH "/ to " ETC_PATH "/mfs/");
			}
		}
	} else {
		hddfname = cfg_getstr("HDD_CONF_FILENAME", ETC_PATH "/mfs/mfshdd.cfg");
		fd = fopen(hddfname,"r");
	}

	if (!fd) {
		free(hddfname);
		return -1;
	}

	ret = 0;

	zassert(pthread_mutex_lock(&folderlock));
	folderactions = 0; // stop folder actions
	for (f=folderhead ; f ; f=f->next) {
		f->toremove = 1;
	}
	zassert(pthread_mutex_unlock(&folderlock));

	while (fgets(buff,999,fd)) {
		buff[999] = 0;
		if (hdd_parseline(buff)<0) {
			ret = -1;
		}

	}
	fclose(fd);

	zassert(pthread_mutex_lock(&folderlock));
	datadef = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->toremove==0) {
			datadef = 1;
			if (f->scanstate==SCST_SCANNEEDED) {
				syslog(LOG_NOTICE,"hdd space manager: folder %s will be scanned",f->path);
			} else if (f->scanstate==SCST_SENDNEEDED) {
				syslog(LOG_NOTICE,"hdd space manager: folder %s will be resend",f->path);
			} else {
				syslog(LOG_NOTICE,"hdd space manager: folder %s didn't change",f->path);
			}
		} else {
			syslog(LOG_NOTICE,"hdd space manager: folder %s will be removed",f->path);
		}
	}
	folderactions = 1; // continue folder actions
	zassert(pthread_mutex_unlock(&folderlock));

	if (datadef==0) {
		mfs_arg_syslog(LOG_ERR,"hdd space manager: no hdd space defined in %s file",hddfname);
		ret = -1;
	}

	free(hddfname);

	return ret;
}

void hdd_reload(void) {
	char *LeaveFreeStr;

	zassert(pthread_mutex_lock(&testlock));
	HDDTestFreq = cfg_getuint32("HDD_TEST_FREQ",10);
	zassert(pthread_mutex_unlock(&testlock));

	LeaveFreeStr = cfg_getstr("HDD_LEAVE_SPACE_DEFAULT","256MiB");
	if (hdd_size_parse(LeaveFreeStr,&LeaveFree)<0) {
		syslog(LOG_NOTICE,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT parse error - left unchanged");
	}
	free(LeaveFreeStr);
	if (LeaveFree<0x4000000) {
		syslog(LOG_NOTICE,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT < chunk size - leaving so small space on hdd is not recommended");
	}

	syslog(LOG_NOTICE,"reloading hdd data ...");
	hdd_folders_reinit();
}

int hdd_late_init(void) {
	zassert(pthread_mutex_lock(&termlock));
	term = 0;
	zassert(pthread_mutex_unlock(&termlock));

	zassert(pthread_create(&testerthread,&thattr,hdd_tester_thread,NULL));
	zassert(pthread_create(&foldersthread,&thattr,hdd_folders_thread,NULL));
	zassert(pthread_create(&delayedthread,&thattr,hdd_delayed_thread,NULL));
	return 0;
}

int hdd_init(void) {
	uint32_t hp;
	folder *f;
	char *LeaveFreeStr;

	// this routine is called at the beginning from the main thread so no locks are necessary here
	for (hp=0 ; hp<HASHSIZE ; hp++) {
		hashtab[hp] = NULL;
	}
	for (hp=0 ; hp<DHASHSIZE ; hp++) {
		dophashtab[hp] = NULL;
	}

#ifndef PRESERVE_BLOCK
	zassert(pthread_key_create(&hdrbufferkey,free));
	zassert(pthread_key_create(&blockbufferkey,free));
#endif /* PRESERVE_BLOCK */

	emptyblockcrc = mycrc32_zeroblock(0,MFSBLOCKSIZE);

	LeaveFreeStr = cfg_getstr("HDD_LEAVE_SPACE_DEFAULT","256MiB");
	if (hdd_size_parse(LeaveFreeStr,&LeaveFree)<0) {
		fprintf(stderr,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT parse error - using default (256MiB)\n");
		LeaveFree = 0x10000000;
	}
	free(LeaveFreeStr);
	if (LeaveFree<0x4000000) {
		fprintf(stderr,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT < chunk size - leaving so small space on hdd is not recommended\n");
	}

	if (hdd_folders_reinit()<0) {
		return -1;
	}

	zassert(pthread_attr_init(&thattr));
	zassert(pthread_attr_setstacksize(&thattr,0x100000));
	zassert(pthread_attr_setdetachstate(&thattr,PTHREAD_CREATE_JOINABLE));

	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		fprintf(stderr,"hdd space manager: path to scan: %s\n",f->path);
	}
	zassert(pthread_mutex_unlock(&folderlock));
	fprintf(stderr,"hdd space manager: start background hdd scanning (searching for available chunks)\n");

	HDDTestFreq = cfg_getuint32("HDD_TEST_FREQ",10);

	main_reloadregister(hdd_reload);
	main_timeregister(TIMEMODE_RUN_LATE,60,0,hdd_diskinfo_movestats);
	main_destructregister(hdd_term);

	zassert(pthread_mutex_lock(&termlock));
	term = 1;
	zassert(pthread_mutex_unlock(&termlock));

	return 0;
}
