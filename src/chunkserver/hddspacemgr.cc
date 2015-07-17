/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <atomic>
#include <list>
#include <string>
#include <thread>
#include <vector>

#include "chunkserver/chunk.h"
#include "chunkserver/chunk_filename_parser.h"
#include "chunkserver/chunk_signature.h"
#include "common/cfg.h"
#include "common/cwrap.h"
#include "common/crc.h"
#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/debug_log.h"
#include "common/disk_info.h"
#include "common/exceptions.h"
#include "common/list.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/MFSCommunication.h"
#include "common/moosefs_vector.h"
#include "common/random.h"
#include "common/serialization.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "common/unique_queue.h"
#include "common/wrong_crc_notifier.h"
#include "devtools/TracePrinter.h"
#include "devtools/request_log.h"

/* system every DELAYEDSTEP seconds searches opened/crc_loaded chunk list for chunks to be closed/free crc */
#define DELAYEDSTEP 2

#define OPENDELAY 5
#define CRCDELAY 100
#define OPENSTEPS (OPENDELAY/DELAYEDSTEP)+1
#define CRCSTEPS (CRCDELAY/DELAYEDSTEP)+1

#define LOSTCHUNKSBLOCKSIZE 1024
#define NEWCHUNKSBLOCKSIZE 4096 // TODO consider sending more chunks in one packet

#define ERRORLIMIT 2
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

typedef std::vector<ChunkWithVersionAndType> NewChunks;

typedef struct dopchunk {
	uint64_t chunkid;
	struct dopchunk *next;
} dopchunk;

static uint32_t HDDTestFreq = 10;

/// Number of bytes which should be addded to each disk's used space
static uint64_t gLeaveFree;

/// Default value for HDD_LEAVE_SPACE_DEFAULT
static const char gLeaveSpaceDefaultDefaultStrValue[] = "4GiB";

/// Value of HDD_ADVISE_NO_CACHE from config
static std::atomic_bool gAdviseNoCache;

static std::atomic<bool> MooseFSChunkFormat;

static std::atomic<bool> PerformFsync;

/* folders data */
static folder *folderhead = NULL;

/* chunk hash */
static Chunk* hashtab[HASHSIZE];

/* extra chunk info */
static dopchunk *dophashtab[DHASHSIZE];
//static dopchunk *dopchunks = NULL;
static dopchunk *newdopchunks = NULL;

// master reports
static damagedchunk *damagedchunks = NULL;
static lostchunk *lostchunks = NULL;
static std::list<NewChunks> gNewChunks;
static uint32_t errorcounter = 0;
static int hddspacechanged = 0;

static pthread_attr_t thattr;

static pthread_t foldersthread, delayedthread, testerthread;
static std::thread test_chunk_thread;

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

static pthread_key_t hdrbufferkey;
static pthread_key_t blockbufferkey;

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
	TRACETHIS1(chunkid);
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
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
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

void hdd_report_new_chunk(uint64_t chunkid, uint32_t version, ChunkType type) {
	TRACETHIS();
	zassert(pthread_mutex_lock(&dclock));
	if (gNewChunks.empty() || gNewChunks.back().size() >= NEWCHUNKSBLOCKSIZE) {
		gNewChunks.push_back(NewChunks());
		gNewChunks.back().reserve(NEWCHUNKSBLOCKSIZE);
	}
	gNewChunks.back().push_back(ChunkWithVersionAndType(chunkid, version, type));
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_get_new_chunks(std::vector<ChunkWithVersionAndType>& chunks) {
	TRACETHIS();
	sassert(chunks.empty());
	zassert(pthread_mutex_lock(&dclock));
	if (!gNewChunks.empty()) {
		chunks.swap(gNewChunks.front());
		gNewChunks.pop_front();
	}
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_errorcounter(void) {
	TRACETHIS();
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = errorcounter;
	errorcounter = 0;
	zassert(pthread_mutex_unlock(&dclock));
	return result;
}

int hdd_spacechanged(void) {
	TRACETHIS();
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = hddspacechanged;
	hddspacechanged = 0;
	zassert(pthread_mutex_unlock(&dclock));
	return result;
}

void hdd_stats(uint64_t *br,uint64_t *bw,uint32_t *opr,uint32_t *opw,uint64_t *dbr,uint64_t *dbw,uint32_t *dopr,uint32_t *dopw,uint64_t *rtime,uint64_t *wtime) {
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
	zassert(pthread_mutex_lock(&statslock));
	stats_opr++;
	stats_bytesr += size;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_write(uint32_t size) {
	TRACETHIS();
	zassert(pthread_mutex_lock(&statslock));
	stats_opw++;
	stats_bytesw += size;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_dataread(folder *f,uint32_t size,int64_t rtime) {
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
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

static inline void hdd_chunk_remove(Chunk *c) {
	TRACETHIS();
	Chunk **cptr,*cp;
	uint32_t hashpos = HASHPOS(c->chunkid);
	cptr = &(hashtab[hashpos]);
	while ((cp=*cptr)) {
		if (c==cp) {
			*cptr = cp->next;
			if (cp->fd>=0) {
				close(cp->fd);
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
			delete cp;
			return;
		}
		cptr = &(cp->next);
	}
}

static void hdd_chunk_release(Chunk *c) {
	TRACETHIS();
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

static int hdd_chunk_getattr(Chunk *c) {
	TRACETHIS1(c->chunkid);
	struct stat sb;
	if (stat(c->filename().c_str(), &sb)<0) {
		return -1;
	}
	if ((sb.st_mode & S_IFMT) != S_IFREG) {
		return -1;
	}
	if (!c->isFileSizeValid(sb.st_size)) {
		return -1;
	}
	c->setBlockCountFromFizeSize(sb.st_size);
	c->validattr = 1;
	return 0;
}

static Chunk* hdd_chunk_tryfind(uint64_t chunkid) {
	TRACETHIS1(chunkid);
	uint32_t hashpos = HASHPOS(chunkid);
	Chunk *c;
	zassert(pthread_mutex_lock(&hashlock));
	for (c=hashtab[hashpos] ; c && c->chunkid!=chunkid ; c=c->next) {}
	if (c!=NULL) {
		if (c->state==CH_LOCKED) {
			c = (Chunk*) CHUNKLOCKED;
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

static void hdd_chunk_delete(Chunk *c);

static Chunk* hdd_chunk_get(
		uint64_t chunkid,
		ChunkType chunkType,
		uint8_t cflag,
		ChunkFormat format) {
	TRACETHIS2(chunkid, (unsigned)cflag);
	uint32_t hashpos = HASHPOS(chunkid);
	Chunk *c;
	cntcond *cc;
	zassert(pthread_mutex_lock(&hashlock));
	c = hashtab[hashpos];
	while (c) {
		if (c->chunkid == chunkid && c->type() == chunkType) {
			break;
		}
		c = c->next;
	}
	if (c==NULL) {
		if (cflag!=CH_NEW_NONE) {
			if (format == ChunkFormat::MOOSEFS) {
				c = new MooseFSChunk(chunkid, chunkType, CH_LOCKED);
			} else {
				sassert(format == ChunkFormat::INTERLEAVED);
				c = new InterleavedChunk(chunkid, chunkType, CH_LOCKED);
			}
			passert(c);
			c->next = hashtab[hashpos];
			hashtab[hashpos] = c;
		}
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
					unlink(c->filename().c_str());
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
				IF_MOOSEFS_CHUNK(mc, c) {
					mc->clearCrc();
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
				c->blocks = 0;
				c->refcount = 0;
				c->wasChanged = false;
				c->opensteps = 0;
				c->fd = -1;
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

static void hdd_chunk_delete(Chunk *c) {
	TRACETHIS();
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

static Chunk* hdd_chunk_create(
		folder *f,
		uint64_t chunkid,
		ChunkType chunkType,
		uint32_t version,
		ChunkFormat chunkFormat) {
	TRACETHIS();
	Chunk *c;

	if (chunkFormat == ChunkFormat::IMPROPER) {
		chunkFormat = MooseFSChunkFormat ?
				ChunkFormat::MOOSEFS :
				ChunkFormat::INTERLEAVED;
	}
	c = hdd_chunk_get(chunkid, chunkType, CH_NEW_EXCLUSIVE, chunkFormat);
	if (c==NULL) {
		return NULL;
	}
	c->version = version;
	f->needrefresh = 1;
	f->chunkcount++;
	c->owner = f;
	c->setFilename(c->generateFilenameForVersion(version));
	zassert(pthread_mutex_lock(&testlock));
	c->testnext = NULL;
	c->testprev = f->testtail;
	(*c->testprev) = c;
	f->testtail = &(c->testnext);
	zassert(pthread_mutex_unlock(&testlock));
	return c;
}

static inline Chunk* hdd_chunk_find(uint64_t chunkId, ChunkType chunkType) {
	LOG_AVG_TILL_END_OF_SCOPE0("chunk_find");
	return hdd_chunk_get(chunkId, chunkType, CH_NEW_NONE, ChunkFormat::IMPROPER);
}

static void hdd_chunk_testmove(Chunk *c) {
	TRACETHIS();
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
	TRACETHIS();
	if (f->sizelimit) {
		uint32_t knownblocks;
		uint32_t knowncount;
		uint64_t calcsize;
		Chunk *c;
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
			// now calcsize is an estimated number of blocks, calculate number of bytes
			calcsize *= MFSBLOCKSIZE;
			calcsize += static_cast<uint64_t>(f->chunkcount) * MFSHDRSIZE;
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
	TRACETHIS();
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
	TRACETHIS();
	uint32_t i;
	uint8_t todel;
	Chunk **cptr,*c;

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
						IF_MOOSEFS_CHUNK(mc, c) {
							mc->clearCrc();
						}
						if (c->testnext) {
							c->testnext->testprev = c->testprev;
						} else {
							c->owner->testtail = c->testprev;
						}
						*(c->testprev) = c->testnext;
						delete c;
					} else if (c->state==CH_LOCKED) {
						cptr = &(c->next);
						c->state = CH_TOBEDELETED;
					}
				} else {
					hdd_report_new_chunk(c->chunkid,
						c->version|((c->todel)?0x80000000:0), c->type());
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
	TRACETHIS();
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

static inline void hdd_error_occured(Chunk *c) {
	TRACETHIS();
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

#define CHUNKS_CUT_COUNT 1000
static uint32_t hdd_get_chunks_pos;

void hdd_get_chunks_begin() {
	TRACETHIS();
	zassert(pthread_mutex_lock(&hashlock));
	hdd_get_chunks_pos = 0;
}

void hdd_get_chunks_end() {
	TRACETHIS();
	zassert(pthread_mutex_unlock(&hashlock));
}

static uint32_t hdd_internal_get_chunks_next_list_count() {
	TRACETHIS();
	uint32_t res = 0;
	uint32_t i = 0;
	Chunk *c;
	while (res<CHUNKS_CUT_COUNT && hdd_get_chunks_pos+i<HASHSIZE) {
		for (c=hashtab[hdd_get_chunks_pos+i] ; c ; c=c->next) {
			res++;
		}
		i++;
	}
	return res;
}

void hdd_get_chunks_next_list_data(std::vector<ChunkWithVersionAndType>& chunks) {
	TRACETHIS();
	uint32_t res = 0;
	uint32_t v;
	Chunk *c;
	sassert(chunks.empty());
	chunks.reserve(hdd_internal_get_chunks_next_list_count());
	while (res<CHUNKS_CUT_COUNT && hdd_get_chunks_pos<HASHSIZE) {
		for (c=hashtab[hdd_get_chunks_pos] ; c ; c=c->next) {
			v = c->version;
			if (c->todel) {
				v |= 0x80000000;
			}
			chunks.push_back(ChunkWithVersionAndType(c->chunkid, v, c->type()));
			res++;
		}
		hdd_get_chunks_pos++;
	}
}

void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount) {
	TRACETHIS();
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

static inline int hdd_int_chunk_readcrc(MooseFSChunk *c) {
	TRACETHIS();
	ChunkSignature chunkSignature;
	if (!chunkSignature.readFromDescriptor(c->fd, c->getSignatureOffset())) {
		int errmem = errno;
		lzfs_silent_errlog(LOG_WARNING,
				"chunk_readcrc: file:%s - read error", c->filename().c_str());
		errno = errmem;
		return ERROR_IO;
	}
	if (!chunkSignature.hasValidSignatureId()) {
		syslog(LOG_WARNING,
				"chunk_readcrc: file:%s - wrong header", c->filename().c_str());
		errno = 0;
		return ERROR_IO;
	}
	if (c->chunkid != chunkSignature.chunkId()
			|| c->version != chunkSignature.chunkVersion()
			|| c->type().chunkTypeId() != chunkSignature.chunkType().chunkTypeId()) {
		syslog(LOG_WARNING,
				"chunk_readcrc: file:%s - wrong id/version/type in header "
				"(%016" PRIX64 "_%08" PRIX32 ", typeId %" PRIu8 ")",
				c->filename().c_str(),
				chunkSignature.chunkId(),
				chunkSignature.chunkVersion(),
				chunkSignature.chunkType().chunkTypeId());
		errno = 0;
		return ERROR_IO;
	}

	c->initEmptyCrc();
#ifndef ENABLE_CRC /* if NOT defined */
	uint8_t* crcArrayPtr = c->crc->data();
	for (int i = 0; i < MFSBLOCKSINCHUNK; ++i) {
		put32bit(&crcArrayPtr, mycrc32_zeroblock(0, 0));
	}
#else /* if ENABLE_CRC defined */
	int ret;
	ret = pread(c->fd, c->crc->data(), c->getCrcBlockSize(), c->getCrcOffset());
	if ((size_t)ret != c->getCrcBlockSize()) {
		int errmem = errno;
		lzfs_silent_errlog(LOG_WARNING,
				"chunk_readcrc: file:%s - read error", c->filename().c_str());
		c->crc.reset();
		errno = errmem;
		return ERROR_IO;
	}
#endif /* ENABLE_CRC */
	hdd_stats_read(c->getCrcBlockSize());
	errno = 0;
	return STATUS_OK;
}

static inline int chunk_writecrc(MooseFSChunk *c) {
	TRACETHIS();
	zassert(pthread_mutex_lock(&folderlock));
	c->owner->needrefresh = 1;
	zassert(pthread_mutex_unlock(&folderlock));
	ssize_t ret = pwrite(c->fd, c->crc->data(), c->getCrcBlockSize(), c->getCrcOffset());
	if (ret != static_cast<ssize_t>(c->getCrcBlockSize())) {
		int errmem = errno;
		lzfs_silent_errlog(LOG_WARNING,
				"chunk_writecrc: file:%s - write error", c->filename().c_str());
		errno = errmem;
		return ERROR_IO;
	}
	hdd_stats_write(c->getCrcBlockSize());
	return STATUS_OK;
}

void hdd_delayed_ops() {
	TRACETHIS();
	dopchunk **ccp,*cc,*tcc;
	uint32_t dhashpos;
	Chunk *c;
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
			} else if (c->refcount>0) {  // io in progress - skip entry
				hdd_chunk_release(c);
				ccp = &(cc->next);
			} else {
				if (c->opensteps>0) {   // decrease counter
					c->opensteps--;
				} else if (c->fd>=0) {  // close descriptor
#ifdef LIZARDFS_HAVE_POSIX_FADVISE
					if (gAdviseNoCache) {
						posix_fadvise(c->fd,0,0,POSIX_FADV_DONTNEED);
					}
#endif /* LIZARDFS_HAVE_POSIX_FADVISE */
					if (close(c->fd)<0) {
						hdd_error_occured(c);   // uses and preserves errno !!!
						lzfs_silent_errlog(LOG_WARNING,"hdd_delayed_ops: file:%s - close error", c->filename().c_str());
						hdd_report_damaged_chunk(c->chunkid);
					}
					c->fd = -1;
				}
				bool crcEmpty = true;
				IF_MOOSEFS_CHUNK(mc, c) {
					if (mc->crcsteps > 0) {
						mc->crcsteps--;
					} else {
						mc->clearCrc();
					}
					crcEmpty = (mc->crc == nullptr);
				}
				if (c->fd < 0 && crcEmpty) {
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
	TRACETHIS();
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return ((uint64_t)(tv.tv_sec))*1000000+tv.tv_usec;
}

static int hdd_io_begin(Chunk *c,int newflag) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_io_begin");
	TRACETHIS();
	dopchunk *cc;
	int status;

//      syslog(LOG_NOTICE,"chunk: %" PRIu64 " - before io",c->chunkid);
	hdd_chunk_testmove(c);
	if (c->refcount==0) {
		bool add = (c->fd < 0);
		IF_MOOSEFS_CHUNK(mc, c) {
			add = add && (mc->crc == nullptr);
		}

		if (c->fd<0) {
			if (newflag) {
				c->fd = open(c->filename().c_str(), O_RDWR | O_TRUNC | O_CREAT, 0666);
			} else {
				if (c->todel<2) {
					c->fd = open(c->filename().c_str(), O_RDWR);
				} else {
					c->fd = open(c->filename().c_str(), O_RDONLY);
				}
			}
			if (c->fd<0) {
				int errmem = errno;
				lzfs_silent_errlog(LOG_WARNING,"hdd_io_begin: file:%s - open error", c->filename().c_str());
				errno = errmem;
				return ERROR_IO;
			}
		}

		IF_MOOSEFS_CHUNK(mc, c) {
			if (mc->crc == nullptr) {
				if (newflag) {
					mc->initEmptyCrc();
				} else {
					mc->readaheadHeader();
					status = hdd_int_chunk_readcrc(mc);
					if (status != STATUS_OK) {
						int errmem = errno;
						if (add) {
							close(c->fd);
							c->fd = -1;
						}
						lzfs_silent_errlog(LOG_WARNING,
								"hdd_io_begin: file:%s - read error", c->filename().c_str());
						errno = errmem;
						return status;
					}
				}
			}
		}

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
	c->refcount++;
	errno = 0;
	return STATUS_OK;
}

static int hdd_io_end(Chunk *c) {
	TRACETHIS1(c->chunkid);
	uint64_t ts,te;

	if (c->wasChanged) {
		IF_MOOSEFS_CHUNK(mc, c) {
			int status = chunk_writecrc(mc);
			PRINTTHIS(status);
			if (status != STATUS_OK) {
				int errmem = errno;
				lzfs_silent_errlog(LOG_WARNING, "hdd_io_end: file:%s - write error",
						c->filename().c_str());
				errno = errmem;
				return status;
			}
		}
		if (PerformFsync) {
			ts = get_usectime();
#ifdef F_FULLFSYNC
			if (fcntl(c->fd,F_FULLFSYNC)<0) {
				int errmem = errno;
				lzfs_silent_errlog(LOG_WARNING,
						"hdd_io_end: file:%s - fsync (via fcntl) error", c->filename().c_str());
				errno = errmem;
				return ERROR_IO;
			}
#else
			if (fsync(c->fd)<0) {
				int errmem = errno;
				lzfs_silent_errlog(LOG_WARNING,
						"hdd_io_end: file:%s - fsync (direct call) error", c->filename().c_str());
				errno = errmem;
				return ERROR_IO;
			}
#endif
			te = get_usectime();
			hdd_stats_datafsync(c->owner,te-ts);
		}
		c->wasChanged = false;
	}

	if (c->refcount <= 0) {
		lzfs_silent_syslog(LOG_WARNING, "hdd_io_end: refcount = 0 - This should never happen!");
		errno = 0;
		return STATUS_OK;
	}
	c->refcount--;
	if (c->refcount==0) {
		if (OPENSTEPS==0) {
			if (close(c->fd)<0) {
				int errmem = errno;
				c->fd = -1;
				lzfs_silent_errlog(LOG_WARNING,
						"hdd_io_end: file:%s - close error", c->filename().c_str());
				errno = errmem;
				return ERROR_IO;
			}
			c->fd = -1;
		} else {
			c->opensteps = OPENSTEPS;
		}
		IF_MOOSEFS_CHUNK(mc, c) {
			mc->crcsteps = CRCSTEPS;
		}
	}
	errno = 0;
	return STATUS_OK;
}

/* I/O operations */

int hdd_open(uint64_t chunkid, ChunkType chunkType) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_open");
	TRACETHIS1(chunkid);
	int status;
	Chunk *c;
	c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	status = hdd_io_begin(c,0);
	PRINTTHIS(status);
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

int hdd_close(uint64_t chunkid, ChunkType chunkType) {
	TRACETHIS1(chunkid);
	int status;
	Chunk *c;
	c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	status = hdd_io_end(c);
	PRINTTHIS(status);
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

/**
 * Get thread specific buffer
 */
uint8_t* hdd_get_block_buffer() {
	auto blockbuffer = (uint8_t*)pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = (uint8_t*)malloc(kHddBlockSize);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
	return blockbuffer;
}

uint8_t* hdd_get_header_buffer() {
	uint8_t* hdrbuffer = (uint8_t*)pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = (uint8_t*)malloc(MooseFSChunk::kMaxHeaderSize);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
	return hdrbuffer;
}

int hdd_read_crc_and_block(Chunk* c, uint16_t blocknum, OutputBuffer* outputBuffer) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_read_block");
	TRACETHIS2(c->chunkid, blocknum);
	int bytesRead = 0;
	uint64_t ts, te;

	if (blocknum >= MFSBLOCKSINCHUNK) {
		return ERROR_BNUMTOOBIG;
	}

	if (blocknum >= c->blocks) {
		uint8_t crcBuff[sizeof(uint32_t)];
		uint8_t* tmp = crcBuff;
		put32bit(&tmp, emptyblockcrc);
		bytesRead = outputBuffer->copyIntoBuffer(crcBuff, sizeof(uint32_t));
		static const std::vector<uint8_t> zeros_block(MFSBLOCKSIZE, 0);
		bytesRead += outputBuffer->copyIntoBuffer(zeros_block);
		if (static_cast<uint32_t>(bytesRead) != kHddBlockSize) {
			return ERROR_IO;
		}
	} else {
		int32_t toBeRead = c->chunkFormat() == ChunkFormat::INTERLEAVED
				? kHddBlockSize : MFSBLOCKSIZE;
		ts = get_usectime();
		off_t off = c->getBlockOffset(blocknum);

		IF_MOOSEFS_CHUNK(mc, c) {
			sassert(c->chunkFormat() == ChunkFormat::MOOSEFS);
			uint8_t crcBuff[sizeof(uint32_t)];
			memcpy(crcBuff, mc->crc->data() + blocknum * sizeof(uint32_t), sizeof(uint32_t));
			outputBuffer->copyIntoBuffer(crcBuff, sizeof(uint32_t));
			bytesRead = outputBuffer->copyIntoBuffer(c->fd, MFSBLOCKSIZE, &off);
		} else do {
			sassert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
			uint8_t* crcBuff = hdd_get_block_buffer();
			uint8_t* data = crcBuff + 4;
			auto containsZerosOnly = [](uint8_t* buffer, uint32_t size) {
				return buffer[0] == 0 && !memcmp(buffer, buffer + 1, size - 1);
			};
			bytesRead = pread(c->fd, crcBuff, 4, off);
			if (bytesRead != 4) {
				break;
			}
			if (containsZerosOnly(crcBuff, 4)) {
				// It looks like this is a sparse file with an empty block. Let's check it
				// and if that's the case let's recompute the CRC

				bytesRead = pread(c->fd, data, MFSBLOCKSIZE, off + 4);
				if (bytesRead != MFSBLOCKSIZE) {
					break;
				}
				if (containsZerosOnly(data, MFSBLOCKSIZE)) {
					// It's indeed a sparse block, recompute the CRC in order to provide
					// backward compatibility
					put32bit(&crcBuff, emptyblockcrc);
				}
				bytesRead = outputBuffer->copyIntoBuffer(hdd_get_block_buffer(), kHddBlockSize);
			} else {
				bytesRead = outputBuffer->copyIntoBuffer(c->fd, kHddBlockSize, &off);
			}
		} while (false);

		te = get_usectime();
		hdd_stats_dataread(c->owner, toBeRead, te-ts);

		if (bytesRead != toBeRead) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"read_block_from_chunk: file:%s - read error", c->filename().c_str());
			hdd_report_damaged_chunk(c->chunkid);
			return ERROR_IO;
		}
	}

	return STATUS_OK;
}

/**
 * Unlike other hdd_... functions this one reports errors to syslog. That's due to the fact that
 * nobody else looks at these errors and we don't want to lose information about them.
 */
int hdd_prefetch_blocks(uint64_t chunkid, ChunkType chunkType, uint32_t firstBlock,
		uint16_t nrOfBlocks) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_prefetch_blocks");

	auto status = hdd_open(chunkid, chunkType);
	if (status != STATUS_OK) {
		syslog(LOG_WARNING, "error opening chunk for prefetching: %" PRIu64 " - %s", chunkid,
				mfsstrerr(status));
		return status;
	}

	Chunk* c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		syslog(LOG_WARNING, "error finding chunk for prefetching: %" PRIu64 " - %s", chunkid,
				mfsstrerr(status));
		return ERROR_NOCHUNK;
	}
	SimpleOutputBuffer buffer = SimpleOutputBuffer(kHddBlockSize);
	for (auto block = firstBlock; block < firstBlock + nrOfBlocks && block < c->blocks; block++) {
		buffer.clear();
		status |= hdd_read_crc_and_block(c, block, &buffer);
		if (status != STATUS_OK) {
			syslog(LOG_WARNING, "error prefetching chunk: %" PRIu64 " - %s", chunkid,
					mfsstrerr(status));
			break;
		}
	}
	hdd_chunk_release(c);

	DEBUG_LOG("chunkserver.hdd_prefetch_blocks")
			<< "chunk:" << chunkid
			<< " status:" << uint16_t(status)
			<< " firstBlock:" << firstBlock
			<< " nrOfBlocks:" << nrOfBlocks;

	status |= hdd_close(chunkid, chunkType);
	if (status != STATUS_OK) {
		syslog(LOG_WARNING, "error closing prefetched chunk: %" PRIu64 " - %s", chunkid,
				mfsstrerr(status));
	}
	return status;
}

static void hdd_prefetch(Chunk& chunk, uint16_t firstBlock, uint32_t numberOfBlocks) {
	if (numberOfBlocks > 0) {
		auto blockSize = chunk.chunkFormat() == ChunkFormat::MOOSEFS ?
				MFSBLOCKSIZE : kHddBlockSize;
		posix_fadvise(chunk.fd, chunk.getBlockOffset(firstBlock),
				uint32_t(numberOfBlocks) * blockSize, POSIX_FADV_WILLNEED);
	}
}

int hdd_read(uint64_t chunkid, uint32_t version, ChunkType chunkType,
		uint32_t offset, uint32_t size, uint32_t maxBlocksToBeReadBehind,
		uint32_t blocksToBeReadAhead, OutputBuffer* outputBuffer) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_read");
	TRACETHIS3(chunkid, offset, size);

	uint32_t offsetWithinBlock = offset % MFSBLOCKSIZE;
	if ((size == 0) || ((offsetWithinBlock + size) > MFSBLOCKSIZE)) {
		return ERROR_WRONGSIZE;
	}

	Chunk* c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	uint16_t block = offset / MFSBLOCKSIZE;

	// Ask OS for an appropriate read ahead and (if requested and needed) read some blocks
	// that were possibly skipped in a sequential file read
	if (c->blockExpectedToBeReadNext < block && maxBlocksToBeReadBehind > 0) {
		// We were asked to read some possibly skipped blocks.
		uint16_t firstBlockToRead = c->blockExpectedToBeReadNext;
		// Try to prevent all possible overflows:
		if (firstBlockToRead + maxBlocksToBeReadBehind < block) {
			firstBlockToRead = block - maxBlocksToBeReadBehind;
		}
		sassert(firstBlockToRead < block);
		hdd_prefetch(*c, firstBlockToRead, blocksToBeReadAhead + block - firstBlockToRead);
		SimpleOutputBuffer buffer = SimpleOutputBuffer(
				kHddBlockSize * (block - firstBlockToRead));
		for (uint16_t b = firstBlockToRead; b < block; ++b) {
			hdd_read_crc_and_block(c, b, &buffer);
		}
	} else {
		hdd_prefetch(*c, block, blocksToBeReadAhead);
	}
	c->blockExpectedToBeReadNext = std::max<uint16_t>(block + 1, c->blockExpectedToBeReadNext);


	// Put checksum of the requested data followed by data itself into buffer.
	// If possible (in case when whole block is read) try to put data directly
	// into passed outputBuffer, otherwise use temporary buffer to recompute
	// the checksum
	uint8_t crcBuff[sizeof(uint32_t)];
	int status = STATUS_OK;
	if (size == MFSBLOCKSIZE) {
		status = hdd_read_crc_and_block(c, block, outputBuffer);
	} else {
		SimpleOutputBuffer tmp(kHddBlockSize);
		status = hdd_read_crc_and_block(c, block, &tmp);
		uint8_t* crcBuffPointer = crcBuff;
		put32bit(&crcBuffPointer, mycrc32(0, tmp.data() + serializedSize(uint32_t()) + offsetWithinBlock, size));
		outputBuffer->copyIntoBuffer(crcBuff, sizeof(uint32_t));
		outputBuffer->copyIntoBuffer(tmp.data() + serializedSize(uint32_t()) + offsetWithinBlock, size);
	}

	PRINTTHIS(status);
	hdd_chunk_release(c);
	return status;
}

/**
 * A way of handling sparse files. If block is filled with zeros and crcBuffer is filled with
 * zeros as well, rewrite the crcBuffer so that it stores proper CRC.
 */
void hdd_int_recompute_crc_if_block_empty(uint8_t* block, uint8_t* crcBuffer) {
	const uint8_t* tmpPtr = crcBuffer;
	uint32_t crc = get32bit(&tmpPtr);

	recompute_crc_if_block_empty(block, crc);
	uint8_t* tmpPtr2 = crcBuffer;
	put32bit(&tmpPtr2, crc);
}

/**
 * Returns number of read bytes on success, -1 on failure.
 * Assumes blockBuffer can fit both data and CRC.
 */
int hdd_int_read_block_and_crc(Chunk* c, uint8_t* blockBuffer, uint8_t* crcBuffer,
		uint16_t blocknum, const char* errorMsg) {
	IF_MOOSEFS_CHUNK(mc, c) {
		sassert(c->chunkFormat() == ChunkFormat::MOOSEFS);
		memcpy(crcBuffer, mc->getCrcBuffer(blocknum), serializedSize(uint32_t()));
		if (pread(c->fd, blockBuffer, MFSBLOCKSIZE, c->getBlockOffset(blocknum))
				!= MFSBLOCKSIZE) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"%s: file:%s - read error", errorMsg, c->filename().c_str());
			hdd_report_damaged_chunk(c->chunkid);
			return -1;
		}
		return MFSBLOCKSIZE;
	} else {
		sassert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
		if (pread(c->fd, blockBuffer, kHddBlockSize, c->getBlockOffset(blocknum))
				!= kHddBlockSize) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"%s: file:%s - read error", errorMsg, c->filename().c_str());
			hdd_report_damaged_chunk(c->chunkid);
			return -1;
		}
		memcpy(crcBuffer, blockBuffer, 4);
		memmove(blockBuffer, blockBuffer + 4, MFSBLOCKSIZE);
		hdd_int_recompute_crc_if_block_empty(blockBuffer, crcBuffer);
		return kHddBlockSize;
	}
}

/**
 * Returns number of written bytes on success, -1 on failure.
 */
bool hdd_int_write_partial_block_and_crc(
		Chunk* c,
		const uint8_t* buffer,
		uint32_t offset,
		uint32_t size,
		const uint8_t* crcBuff,
		uint16_t blockNum,
		const char* errorMsg) {
	const int crcSize = serializedSize(uint32_t());
	IF_MOOSEFS_CHUNK(mc, c) {
		sassert(c->chunkFormat() == ChunkFormat::MOOSEFS);
		auto ret = pwrite(c->fd, buffer, size, c->getBlockOffset(blockNum) + offset);
		if (ret != size) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"%s: file:%s - write error", errorMsg, c->filename().c_str());
			hdd_report_damaged_chunk(c->chunkid);
			return -1;
		}
		memcpy(mc->getCrcBuffer(blockNum), crcBuff, crcSize);
		return size;
	} else {
		sassert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
		auto ret = pwrite(c->fd, crcBuff, crcSize, c->getBlockOffset(blockNum));
		if (ret != crcSize) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"%s: file:%s - crc write error", errorMsg, c->filename().c_str());
			hdd_report_damaged_chunk(c->chunkid);
			return -1;
		}
		ret = pwrite(c->fd, buffer, size, c->getBlockOffset(blockNum) + offset + crcSize);
		if (ret != size) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"%s: file:%s - write error", errorMsg, c->filename().c_str());
			hdd_report_damaged_chunk(c->chunkid);
			return -1;
		}
		return crcSize + size;
	}
}

bool hdd_int_write_block_and_crc(
		Chunk* c,
		const uint8_t* buffer,
		const uint8_t* crcBuff,
		uint16_t blockNum,
		const char* errorMsg) {
	return hdd_int_write_partial_block_and_crc(
			c, buffer, 0, MFSBLOCKSIZE, crcBuff, blockNum, errorMsg);
}

int hdd_write(uint64_t chunkid, uint32_t version, ChunkType chunkType,
		uint16_t blocknum, uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_write");
	TRACETHIS3(chunkid, offset, size);
	Chunk *c;
	uint32_t precrc, postcrc, combinedcrc, chcrc;
	uint64_t ts,te;
	uint8_t *blockbuffer;
	blockbuffer = hdd_get_block_buffer();
	c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (blocknum >= c->maxBlocksInFile()) {
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
	if (crc!=mycrc32(0,buffer,size)) {
		hdd_chunk_release(c);
		return ERROR_CRC;
	}
	uint8_t crcBuff[sizeof(uint32_t)];
	c->wasChanged = true;
	if (offset==0 && size==MFSBLOCKSIZE) {
		if (blocknum>=c->blocks) {
			uint16_t prevBlocks = c->blocks;
			c->blocks = blocknum + 1;
			IF_MOOSEFS_CHUNK(mc, c) {
				for (uint16_t i = prevBlocks; i < blocknum; i++) {
					uint8_t *wcrcptr = mc->getCrcBuffer(i);
					put32bit(&wcrcptr, emptyblockcrc);
				}
			}
		}
		ts = get_usectime();
		uint8_t* crcBuffPointer = crcBuff;
		put32bit(&crcBuffPointer, crc);
		memcpy(blockbuffer, crcBuff, 4);
		memcpy(blockbuffer + 4, buffer, MFSBLOCKSIZE);

		auto written = hdd_int_write_block_and_crc(
				c, buffer, crcBuff, blocknum, "write_block_to_chunk");
		if (written < 0) {
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		te = get_usectime();
		hdd_stats_datawrite(c->owner, written, te - ts);
	} else {
		if (blocknum<c->blocks) {
			ts = get_usectime();
			auto readBytes = hdd_int_read_block_and_crc(
					c, blockbuffer, crcBuff, blocknum, "write_block_to_chunk");
			if (readBytes < 0) {
				hdd_chunk_release(c);
				return ERROR_IO;
			}
			te = get_usectime();
			hdd_stats_dataread(c->owner,readBytes,te-ts);
			precrc = mycrc32(0,blockbuffer,offset);
			chcrc = mycrc32(0,blockbuffer+offset,size);
			postcrc = mycrc32(0,blockbuffer+offset+size,MFSBLOCKSIZE-(offset+size));
			if (offset==0) {
				combinedcrc = mycrc32_combine(chcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			} else {
				combinedcrc = mycrc32_combine(precrc,chcrc,size);
				if ((offset+size)<MFSBLOCKSIZE) {
					combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
				}
			}
			const uint8_t* crcBuffPointer = crcBuff;
			const uint8_t** tmpPtr = &crcBuffPointer;
			if (get32bit(tmpPtr) != combinedcrc) {
				errno = 0;
				hdd_error_occured(c);   // uses and preserves errno !!!
				syslog(LOG_WARNING,
						"write_block_to_chunk: file:%s - crc error", c->filename().c_str());
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_CRC;
			}
		} else {
			if (ftruncate(c->fd, c->getFileSizeFromBlockCount(blocknum + 1)) < 0) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"write_block_to_chunk: file:%s - ftruncate error", c->filename().c_str());
				hdd_report_damaged_chunk(chunkid);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
			uint16_t prevBlocks = c->blocks;
			c->blocks = blocknum + 1;
			IF_MOOSEFS_CHUNK(mc, c) {
				for (uint16_t i = prevBlocks; i < blocknum; i++) {
					uint8_t *wcrcptr = mc->getCrcBuffer(i);
					put32bit(&wcrcptr, emptyblockcrc);
				}
			}
			precrc = mycrc32_zeroblock(0,offset);
			postcrc = mycrc32_zeroblock(0,MFSBLOCKSIZE-(offset+size));
		}
		ts = get_usectime();
		if (offset==0) {
			combinedcrc = mycrc32_combine(crc,postcrc,MFSBLOCKSIZE-(offset+size));
		} else {
			combinedcrc = mycrc32_combine(precrc,crc,size);
			if ((offset+size)<MFSBLOCKSIZE) {
				combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			}
		}
		uint8_t* crcBuffPointer = crcBuff;
		put32bit(&crcBuffPointer, combinedcrc);
		auto written = hdd_int_write_partial_block_and_crc(
				c, buffer, offset, size, crcBuff, blocknum, "write_block_to_chunk");
		if (written < 0) {
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		te = get_usectime();
		hdd_stats_datawrite(c->owner,written,te-ts);
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}



/* chunk info */

int hdd_check_version(uint64_t chunkid, uint32_t version) {
	TRACETHIS2(chunkid, version);
	Chunk *c;
	c = hdd_chunk_find(chunkid, ChunkType::getStandardChunkType());
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		PRINTTHIS(ERROR_WRONGVERSION);
		return ERROR_WRONGVERSION;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}

int hdd_get_blocks(uint64_t chunkid, ChunkType chunkType, uint32_t version, uint16_t *blocks) {
	TRACETHIS1(chunkid);
	Chunk *c;
	c = hdd_chunk_find(chunkid, chunkType);
	*blocks = 0;
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

/* chunk operations */
static int hdd_chunk_overwrite_version(Chunk* c, uint32_t newVersion) {
	IF_MOOSEFS_CHUNK(mc, c) {
		(void)mc;
		std::vector<uint8_t> buffer;
		serialize(buffer, newVersion);
		if (pwrite(c->fd, buffer.data(), buffer.size(), ChunkSignature::kVersionOffset)
				!= static_cast<ssize_t>(buffer.size())) {
			return ERROR_IO;
		}
		hdd_stats_write(buffer.size());
	}
	c->version = newVersion;
	return STATUS_OK;
}

static int hdd_int_create(uint64_t chunkid, uint32_t version, ChunkType chunkType) {
	TRACETHIS2(chunkid, version);
	folder *f;
	Chunk *c;
	int status;

	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f, chunkid, chunkType, version, ChunkFormat::IMPROPER);
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		return ERROR_CHUNKEXIST;
	}

	status = hdd_io_begin(c,1);
	PRINTTHIS(status);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_delete(c);
		return ERROR_IO;
	}

	IF_MOOSEFS_CHUNK(mc, c) {
		memset(hdd_get_header_buffer(), 0, mc->getHeaderSize());
		uint8_t *ptr = hdd_get_header_buffer();
		serialize(&ptr, ChunkSignature(chunkid, version, chunkType));
		if (write(c->fd, hdd_get_header_buffer(), mc->getHeaderSize()) != static_cast<ssize_t>(mc->getHeaderSize())) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"create_newchunk: file:%s - write error", c->filename().c_str());
			hdd_io_end(c);
			unlink(c->filename().c_str());
			hdd_chunk_delete(c);
			return ERROR_IO;
		}
		hdd_stats_write(mc->getHeaderSize());
	}
	status = hdd_io_end(c);
	PRINTTHIS(status);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename().c_str());
		hdd_chunk_delete(c);
		return status;
	}
	hdd_chunk_release(c);
	return STATUS_OK;
}

static int hdd_int_test(uint64_t chunkid, uint32_t version, ChunkType chunkType) {
	TRACETHIS2(chunkid, version);
	uint16_t block;
		int status;
	Chunk *c;
	uint8_t *blockbuffer;
	blockbuffer = hdd_get_block_buffer();
	c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,0);
	PRINTTHIS(status);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	uint8_t crcBuff[sizeof(uint32_t)];
	status = STATUS_OK; // will be overwritten in the loop below if the test fails
	for (block=0 ; block<c->blocks ; block++) {
		auto readBytes = hdd_int_read_block_and_crc(c, blockbuffer, crcBuff, block, "test_chunk");
		if (readBytes < 0) {
			status = ERROR_IO;
			break;
		}
		hdd_stats_read(readBytes);
		const uint8_t* crcBuffPointer = crcBuff;
		if (get32bit(&crcBuffPointer) != mycrc32(0, blockbuffer, MFSBLOCKSIZE)) {
			errno = 0;      // set anything to errno
			hdd_error_occured(c);   // uses and preserves errno !!!
			syslog(LOG_WARNING, "test_chunk: file:%s - crc error", c->filename().c_str());
			status = ERROR_CRC;
			break;
		}
	}
#ifdef LIZARDFS_HAVE_POSIX_FADVISE
	// Always advise the OS that tested chunks should not be cached. Don't rely on
	// hdd_delayed_ops to do it for us, because it may be disabled using a config file.
	posix_fadvise(c->fd,0,0,POSIX_FADV_DONTNEED);
#endif /* LIZARDFS_HAVE_POSIX_FADVISE */
	if (status != STATUS_OK) {
		// test failed -- chunk is damaged
		hdd_io_end(c);
		hdd_chunk_release(c);
		return status;
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

static int hdd_int_duplicate(uint64_t chunkId, uint32_t chunkVersion, uint32_t chunkNewVersion,
		ChunkType chunkType, uint64_t copyChunkId, uint32_t copyChunkVersion) {
	TRACETHIS();
	folder *f;
	uint16_t block;
	int32_t retsize;
	int status;
	Chunk *c,*oc;
	uint8_t *blockbuffer;
	blockbuffer = hdd_get_block_buffer();

	oc = hdd_chunk_find(chunkId, chunkType);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->version!=chunkVersion && chunkVersion>0) {
		hdd_chunk_release(oc);
		return ERROR_WRONGVERSION;
	}
	if (copyChunkVersion==0) {
		copyChunkVersion = chunkNewVersion;
	}
	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		hdd_chunk_release(oc);
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f, copyChunkId, chunkType, copyChunkVersion, oc->chunkFormat());
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		hdd_chunk_release(oc);
		return ERROR_CHUNKEXIST;
	}
	sassert(c->chunkFormat() == oc->chunkFormat());

	if (chunkNewVersion != chunkVersion) {
		if (c->renameChunkFile(c->generateFilenameForVersion(chunkNewVersion)) < 0) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - rename error", oc->filename().c_str());
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		status = hdd_io_begin(oc, 0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;  //can't change file version
		}
		status = hdd_chunk_overwrite_version(oc, chunkNewVersion);
		if (status != STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - write error", c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_report_damaged_chunk(chunkId);
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
	int32_t blockSize = c->chunkFormat() == ChunkFormat::MOOSEFS ? MFSBLOCKSIZE : kHddBlockSize;
	IF_MOOSEFS_CHUNK(mc, c) {
		MooseFSChunk* moc = dynamic_cast<MooseFSChunk*>(oc);
		sassert(moc != nullptr);
		memset(hdd_get_header_buffer(), 0, mc->getHeaderSize());
		uint8_t *ptr = hdd_get_header_buffer();
		serialize(&ptr, ChunkSignature(copyChunkId, copyChunkVersion, chunkType));
		memcpy(mc->crc->data(), moc->crc->data(), mc->getCrcBlockSize());
		memcpy(hdd_get_header_buffer() + mc->getCrcOffset(), moc->crc->data(), mc->getCrcBlockSize());
		if (write(mc->fd, hdd_get_header_buffer(), mc->getHeaderSize()) != static_cast<ssize_t>(mc->getHeaderSize())) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - hdr write error", c->filename().c_str());
			hdd_io_end(c);
			unlink(c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		hdd_stats_write(mc->getHeaderSize());
	}
	lseek(oc->fd, c->getBlockOffset(0), SEEK_SET);
	for (block=0 ; block<oc->blocks ; block++) {
		retsize = read(oc->fd,blockbuffer,blockSize);
		if (retsize!=blockSize) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - data read error", c->filename().c_str());
			hdd_io_end(c);
			unlink(c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_report_damaged_chunk(chunkId);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		hdd_stats_read(blockSize);
		retsize = write(c->fd,blockbuffer,blockSize);
		if (retsize!=blockSize) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - data write error", c->filename().c_str());
			hdd_io_end(c);
			unlink(c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;        //write error
		}
		hdd_stats_write(blockSize);
	}
	status = hdd_io_end(oc);
	if (status!=STATUS_OK) {
		hdd_error_occured(oc);  // uses and preserves errno !!!
		hdd_io_end(c);
		unlink(c->filename().c_str());
		hdd_chunk_delete(c);
		hdd_report_damaged_chunk(chunkId);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename().c_str());
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

static int hdd_int_version(uint64_t chunkid, uint32_t version, uint32_t newversion,
		ChunkType chunkType) {
	TRACETHIS();
	int status;
	Chunk *c;
	c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (c->renameChunkFile(c->generateFilenameForVersion(newversion)) < 0) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"set_chunk_version: file:%s - rename error", c->filename().c_str());
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"set_chunk_version: file:%s - open error", c->filename().c_str());
		hdd_chunk_release(c);
		return status;
	}
	status = hdd_chunk_overwrite_version(c, newversion);
	if (status != STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"set_chunk_version: file:%s - write error", c->filename().c_str());
		hdd_io_end(c);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
	}
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_truncate(uint64_t chunkId, ChunkType chunkType, uint32_t oldVersion,
		uint32_t newVersion, uint32_t length) {
	TRACETHIS4(chunkId, oldVersion, newVersion, length);
	int status;
	Chunk *c;
	uint32_t blocks;
	uint32_t i;
	uint8_t *blockbuffer;
	blockbuffer = hdd_get_block_buffer();
	if (length>MFSCHUNKSIZE) {
		return ERROR_WRONGSIZE;
	}
	c = hdd_chunk_find(chunkId, chunkType);

	// step 1 - change version
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=oldVersion && oldVersion>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (c->renameChunkFile(c->generateFilenameForVersion(newVersion)) < 0) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"truncate_chunk: file:%s - rename error", c->filename().c_str());
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	status = hdd_io_begin(c,0);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;  //can't change file version
	}
	status = hdd_chunk_overwrite_version(c, newVersion);
	if (status != STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"truncate_chunk: file:%s - write error", c->filename().c_str());
		hdd_io_end(c);
		hdd_chunk_release(c);
		return ERROR_IO;
	}
	c->wasChanged = true;

	// step 2. truncate
	blocks = ((length + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE);
	if (blocks>c->blocks) {
		IF_MOOSEFS_CHUNK(mc, c) {
			uint8_t crcBuff[4];
			auto* ptr = crcBuff;
			put32bit(&ptr, emptyblockcrc);
			for (auto block = c->blocks; block < blocks; block++) {
				memcpy(mc->crc->data() + block * 4, crcBuff, 4);
			}
		}
		if (ftruncate(c->fd, c->getFileSizeFromBlockCount(blocks)) < 0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"truncate_chunk: file:%s - ftruncate error", c->filename().c_str());
			hdd_io_end(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
	} else {
		uint32_t fullBlocks = length / MFSBLOCKSIZE;
		uint32_t lastPartialBlockSize = length - fullBlocks * MFSBLOCKSIZE;
		if (lastPartialBlockSize > 0) {
			auto len = c->getFileSizeFromBlockCount(fullBlocks) + lastPartialBlockSize;
			if (c->chunkFormat() == ChunkFormat::INTERLEAVED) {
				len += 4;
			}
			if (ftruncate(c->fd, len) < 0) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"truncate_chunk: file:%s - ftruncate error", c->filename().c_str());
				hdd_io_end(c);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
		}
		if (ftruncate(c->fd, c->getFileSizeFromBlockCount(blocks)) < 0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"truncate_chunk: file:%s - ftruncate error", c->filename().c_str());
			hdd_io_end(c);
			hdd_chunk_release(c);
			return ERROR_IO;
		}
		if (lastPartialBlockSize>0) {
			auto offset = c->getBlockOffset(fullBlocks);
			if (c->chunkFormat() == ChunkFormat::INTERLEAVED) {
				offset += 4;
			}
			if (pread(c->fd, blockbuffer, lastPartialBlockSize, offset)
					!= static_cast<ssize_t>(lastPartialBlockSize)) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"truncate_chunk: file:%s - read error", c->filename().c_str());
				hdd_io_end(c);
				hdd_chunk_release(c);
				return ERROR_IO;
			}
			hdd_stats_read(lastPartialBlockSize);
			i = mycrc32_zeroexpanded(0,blockbuffer,lastPartialBlockSize,MFSBLOCKSIZE-lastPartialBlockSize);
			uint8_t crcBuff[sizeof(uint32_t)];
			uint8_t* crcBuffPointer = crcBuff;
			put32bit(&crcBuffPointer, i);
			IF_MOOSEFS_CHUNK(mc, c) {
				sassert(c->chunkFormat() == ChunkFormat::MOOSEFS);
				memcpy(mc->crc->data() + fullBlocks * 4, crcBuff, 4);
				uint8_t crcBuff[4];
				auto* ptr = crcBuff;
				put32bit(&ptr, emptyblockcrc);
				for (auto block = fullBlocks + 1; block < c->blocks; block++) {
					memcpy(mc->crc->data() + block * 4, crcBuff, 4);
				}
			} else {
				sassert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
				if (pwrite(c->fd, crcBuff, 4, c->getBlockOffset(fullBlocks)) != 4) {
					hdd_error_occured(c);   // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
							"truncate_chunk: file:%s - write crc error", c->filename().c_str());
					hdd_report_damaged_chunk(chunkId);
					hdd_chunk_release(c);
					return ERROR_IO;
				}
			}
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

static int hdd_int_duptrunc(uint64_t chunkId, uint32_t chunkVersion, uint32_t chunkNewVersion,
		ChunkType chunkType, uint64_t copyChunkId, uint32_t copyChunkVersion,
		uint32_t copyChunkLength) {
	TRACETHIS();
	folder *f;
	uint16_t block;
	uint16_t blocks;
	int32_t retsize;
	uint32_t crc;
	int status;
	Chunk *c,*oc;
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = hdd_get_block_buffer();
	hdrbuffer = hdd_get_header_buffer();

	if (copyChunkLength>MFSCHUNKSIZE) {
		return ERROR_WRONGSIZE;
	}
	oc = hdd_chunk_find(chunkId, chunkType);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->version!=chunkVersion && chunkVersion>0) {
		hdd_chunk_release(oc);
		return ERROR_WRONGVERSION;
	}
	if (copyChunkVersion==0) {
		copyChunkVersion = chunkNewVersion;
	}
	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		hdd_chunk_release(oc);
		return ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f, copyChunkId, chunkType, copyChunkVersion, oc->chunkFormat());
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		hdd_chunk_release(oc);
		return ERROR_CHUNKEXIST;
	}

	if (chunkNewVersion!=chunkVersion) {
		if (oc->renameChunkFile(oc->generateFilenameForVersion(chunkNewVersion)) < 0) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - rename error", oc->filename().c_str());
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;  //can't change file version
		}
		status = hdd_chunk_overwrite_version(oc, chunkNewVersion);
		if (status != STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duptrunc_chunk: file:%s - write error", c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_report_damaged_chunk(chunkId);
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
	MooseFSChunk* mc = dynamic_cast<MooseFSChunk*>(c);
	MooseFSChunk* moc = dynamic_cast<MooseFSChunk*>(oc);
	sassert((mc == nullptr && moc == nullptr) || (mc != nullptr && moc != nullptr));
	blocks = (copyChunkLength + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE;
	int32_t blockSize = c->chunkFormat() == ChunkFormat::MOOSEFS ? MFSBLOCKSIZE : kHddBlockSize;
	if (mc) {
		memset(hdrbuffer, 0, mc->getHeaderSize());
		uint8_t *ptr = hdrbuffer;
		serialize(&ptr, ChunkSignature(copyChunkId, copyChunkVersion, chunkType));
		memcpy(hdrbuffer + mc->getCrcOffset(), moc->crc->data(), mc->getCrcBlockSize());
	}
	lseek(c->fd, c->getBlockOffset(0), SEEK_SET);
	lseek(oc->fd, c->getBlockOffset(0), SEEK_SET);
	if (blocks>oc->blocks) { // expanding
		for (block=0 ; block<oc->blocks ; block++) {
			retsize = read(oc->fd,blockbuffer,blockSize);
			if (retsize!=blockSize) {
				hdd_error_occured(oc);  // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_report_damaged_chunk(chunkId);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_read(blockSize);
			retsize = write(c->fd,blockbuffer,blockSize);
			if (retsize!=blockSize) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"duptrunc_chunk: file:%s - data write error", c->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_write(blockSize);
		}
		if (mc) {
			for (block = oc->blocks; block < blocks; block++) {
				auto* ptr = hdrbuffer + mc->getCrcOffset() + 4 * block;
				put32bit(&ptr, emptyblockcrc);
			}
		}
		if (ftruncate(c->fd, c->getFileSizeFromBlockCount(blocks))<0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duptrunc_chunk: file:%s - ftruncate error", c->filename().c_str());
			hdd_io_end(c);
			unlink(c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;        //write error
		}
	} else { // shrinking
		uint32_t lastBlockSize = copyChunkLength - (copyChunkLength / MFSBLOCKSIZE) * MFSBLOCKSIZE;
		if (lastBlockSize==0) { // aligned shrink
			for (block=0 ; block<blocks ; block++) {
				retsize = read(oc->fd,blockbuffer,blockSize);
				if (retsize!=blockSize) {
					hdd_error_occured(oc);  // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
							"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
					hdd_io_end(c);
					unlink(c->filename().c_str());
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_report_damaged_chunk(chunkId);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
				hdd_stats_read(blockSize);
				retsize = write(c->fd,blockbuffer,blockSize);
				if (retsize!=blockSize) {
					hdd_error_occured(c);   // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
							"duptrunc_chunk: file:%s - data write error", c->filename().c_str());
					hdd_io_end(c);
					unlink(c->filename().c_str());
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
				hdd_stats_write(blockSize);
			}
		} else { // misaligned shrink
			for (block=0 ; block<blocks-1 ; block++) {
				retsize = read(oc->fd,blockbuffer,blockSize);
				if (retsize!=blockSize) {
					hdd_error_occured(oc);  // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
							"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
					hdd_io_end(c);
					unlink(c->filename().c_str());
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_report_damaged_chunk(chunkId);
					hdd_chunk_release(oc);
					return ERROR_IO;
				}
				hdd_stats_read(blockSize);
				retsize = write(c->fd,blockbuffer,blockSize);
				if (retsize!=blockSize) {
					hdd_error_occured(c);   // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
							"duptrunc_chunk: file:%s - data write error", c->filename().c_str());
					hdd_io_end(c);
					unlink(c->filename().c_str());
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return ERROR_IO;        //write error
				}
				hdd_stats_write(blockSize);
			}
			block = blocks-1;
			auto toBeRead = c->chunkFormat() == ChunkFormat::MOOSEFS ? lastBlockSize : lastBlockSize + 4;
			retsize = read(oc->fd, blockbuffer, toBeRead);
			if (retsize!=(signed)toBeRead) {
				hdd_error_occured(oc);  // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
					"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_report_damaged_chunk(chunkId);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_read(toBeRead);
			if (c->chunkFormat() == ChunkFormat::INTERLEAVED) {
				crc = mycrc32_zeroexpanded(0, blockbuffer + 4, lastBlockSize, MFSBLOCKSIZE - lastBlockSize);
				uint8_t* crcBuffPointer = blockbuffer;
				put32bit(&crcBuffPointer, crc);
			} else {
				auto* ptr = hdrbuffer + mc->getCrcOffset() + 4 * block;
				auto crc = mycrc32_zeroexpanded(0, blockbuffer, lastBlockSize, MFSBLOCKSIZE - lastBlockSize);
				put32bit(&ptr,crc);
			}
			memset(blockbuffer + toBeRead, 0, MFSBLOCKSIZE - lastBlockSize);
			retsize = write(c->fd, blockbuffer, blockSize);
			if (retsize!=blockSize) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"duptrunc_chunk: file:%s - data write error", c->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return ERROR_IO;
			}
			hdd_stats_write(blockSize);
		}
	}
	if (mc) {
		memcpy(mc->crc->data(), hdrbuffer + mc->getCrcOffset(), mc->getCrcBlockSize());
		lseek(mc->fd,0,SEEK_SET);
		if (write(mc->fd, hdrbuffer, mc->getHeaderSize()) != static_cast<ssize_t>(mc->getHeaderSize())) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duptrunc_chunk: file:%s - hdr write error", c->filename().c_str());
			hdd_io_end(c);
			unlink(c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return ERROR_IO;
		}
		hdd_stats_write(mc->getHeaderSize());
	}
	status = hdd_io_end(oc);
	if (status!=STATUS_OK) {
		hdd_error_occured(oc);  // uses and preserves errno !!!
		hdd_io_end(c);
		unlink(c->filename().c_str());
		hdd_chunk_delete(c);
		hdd_report_damaged_chunk(chunkId);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename().c_str());
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


static int hdd_int_delete(uint64_t chunkid, uint32_t version, ChunkType chunkType) {
	TRACETHIS();
	Chunk *c;
	c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return ERROR_WRONGVERSION;
	}
	if (unlink(c->filename().c_str()) < 0) {
		uint8_t err = errno;
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"delete_chunk: file:%s - unlink error", c->filename().c_str());
		if (err == ENOENT) {
			hdd_chunk_delete(c);
		} else {
			hdd_chunk_release(c);
		}
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
int hdd_chunkop(uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
		uint32_t chunkNewVersion, uint64_t copyChunkId, uint32_t copyChunkVersion,
		uint32_t length) {
	TRACETHIS();
	zassert(pthread_mutex_lock(&statslock));
	if (chunkNewVersion>0) {
		if (length==0xFFFFFFFF) {
			if (copyChunkId==0) {
				stats_version++;
			} else {
				stats_duplicate++;
			}
		} else if (length<=MFSCHUNKSIZE) {
			if (copyChunkId==0) {
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
	if (chunkNewVersion>0) {
		if (length==0xFFFFFFFF) {
			if (copyChunkId==0) {
				return hdd_int_version(chunkId, chunkVersion, chunkNewVersion, chunkType);
			} else {
				return hdd_int_duplicate(chunkId, chunkVersion, chunkNewVersion, chunkType,
						copyChunkId, copyChunkVersion);
			}
		} else if (length<=MFSCHUNKSIZE) {
			if (copyChunkId==0) {
				return hdd_int_truncate(chunkId, chunkType, chunkVersion, chunkNewVersion, length);
			} else {
				return hdd_int_duptrunc(chunkId, chunkVersion, chunkNewVersion, chunkType,
						copyChunkId, copyChunkVersion, length);
			}
		} else {
			return ERROR_EINVAL;
		}
	} else {
		if (length==0) {
			return hdd_int_delete(chunkId, chunkVersion, chunkType);
		} else if (length==1) {
			return hdd_int_create(chunkId, chunkVersion, chunkType);
		} else if (length==2) {
			return hdd_int_test(chunkId, chunkVersion, chunkType);
		} else {
			return ERROR_EINVAL;
		}
	}
}

static UniqueQueue<ChunkWithVersionAndType> test_chunk_queue;

static void hdd_test_chunk_thread() {
	bool terminate = false;
	while (!terminate) {
		Timeout time(std::chrono::seconds(1));
		try {
			ChunkWithVersionAndType chunk = test_chunk_queue.get();
			std::string name = chunk.toString();
			if (hdd_int_test(chunk.id, chunk.version, chunk.type) != STATUS_OK) {
				syslog(LOG_NOTICE, "Chunk %s corrupted (detected by a client)",
						name.c_str());
				hdd_report_damaged_chunk(chunk.id);
			} else {
				syslog(LOG_NOTICE, "Chunk %s spuriously reported as corrupted",
						name.c_str());
			}
		} catch (UniqueQueueEmptyException&) {
			// hooray, nothing to do
		}
		// rate-limit to 1/sec
		usleep(time.remaining_us());
		zassert(pthread_mutex_lock(&termlock));
		terminate = term;
		zassert(pthread_mutex_unlock(&termlock));
	};
}

void hdd_test_chunk(ChunkWithVersionAndType chunk) {
	test_chunk_queue.put(chunk);
}

void* hdd_tester_thread(void* arg) {
	TRACETHIS();
	folder *f,*of;
	Chunk *c;
	uint64_t chunkid;
	uint32_t version;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint32_t freq;
	uint32_t cnt;
	uint64_t st,en;
	std::string path;

	f = folderhead;
	freq = HDDTestFreq;
	cnt = 0;
	for (;;) {
		st = get_usectime();
		path.clear();
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
			path.clear();
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
				path.clear();
			} else {
				c = f->testhead;
				if (c && c->state==CH_AVAIL) {
					chunkid = c->chunkid;
					version = c->version;
					chunkType = c->type();
					path = c->filename();
				}
			}
		}
		zassert(pthread_mutex_unlock(&testlock));
		zassert(pthread_mutex_unlock(&hashlock));
		zassert(pthread_mutex_unlock(&folderlock));
		if (!path.empty()) {
			zassert(pthread_mutex_lock(&statslock));
			stats_test++;
			zassert(pthread_mutex_unlock(&statslock));
			if (hdd_int_test(chunkid, version, chunkType) != STATUS_OK) {
				hdd_report_damaged_chunk(chunkid);
			}
			path.clear();
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
	TRACETHIS();
	uint32_t i,j,chunksno;
	Chunk **csorttab,*c;
	zassert(pthread_mutex_lock(&testlock));
	chunksno = 0;
	for (c=f->testhead ; c ; c=c->testnext) {
		chunksno++;
	}
	if (chunksno>0) {
		csorttab = (Chunk**) malloc(sizeof(Chunk*)*chunksno);
		passert(csorttab);
		chunksno = 0;
		for (c=f->testhead ; c ; c=c->testnext) {
			csorttab[chunksno++] = c;
		}
		if (chunksno>1) {
			for (i=0 ; i<chunksno-1 ; i++) {
				j = i+rnd_ranged<uint32_t>(chunksno-i);
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

static inline void hdd_add_chunk(folder *f,
		const std::string& fullname,
		uint64_t chunkId,
		ChunkFormat chunkFormat,
		uint32_t version,
		ChunkType chunkType,
		uint8_t todel) {
	TRACETHIS();
	folder *prevf;
	Chunk *c;

	prevf = NULL;
	c = hdd_chunk_get(chunkId, chunkType, CH_NEW_AUTO, chunkFormat);
	if (!c->filename().empty()) {
		// already have this chunk
		if (version <= c->version) {
			// current chunk is older
			if (todel < 2) { // this is R/W fs?
				unlink(fullname.c_str());
			}
		} else {
			prevf = c->owner;
			if (c->todel<2) { // current chunk is on R/W fs?
				unlink(c->filename().c_str()); // if yes then remove file
			}
			c->version = version;
			c->blocks = 0;
			c->owner = f;
			c->todel = todel;
			c->setFilename(c->generateFilenameForVersion(version));
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
		c->version = version;
		c->blocks = 0;
		c->owner = f;
		c->todel = todel;
		c->setFilename(c->generateFilenameForVersion(version));
		sassert(c->filename() == fullname);
		zassert(pthread_mutex_lock(&testlock));
		c->testprev = f->testtail;
		*(c->testprev) = c;
		f->testtail = &(c->testnext);
		zassert(pthread_mutex_unlock(&testlock));
		hdd_report_new_chunk(c->chunkid, c->version|(todel?0x80000000:0), c->type());
	}
	hdd_chunk_release(c);
	zassert(pthread_mutex_lock(&folderlock));
	if (prevf) {
		prevf->chunkcount--;
	}
	f->chunkcount++;
	zassert(pthread_mutex_unlock(&folderlock));
}

// Moves chunks from
//    XY/chunk_??????????????XY_...
// to
//    chunksAB/chunk_??????????AB????_...
void hdd_folder_migrate_directories(folder *f) {
	// Allocate memory for directory entries as suggested in man readdir_r
	long nameMax = pathconf(f->path, _PC_NAME_MAX);
	if (nameMax == -1) {
		nameMax = 255;
	}
	std::vector<uint8_t> direntry(offsetof(struct dirent, d_name) + nameMax + 1);
	struct dirent *destorage = reinterpret_cast<struct dirent*>(direntry.data());

	// Look for old folders and move all chunk files from then to new destinations
	bool migrateMessageWasPrinted = false;
	for (unsigned oldSubfolderNumber = 0; oldSubfolderNumber < 256; ++oldSubfolderNumber) {
		char oldSubfolderName[3];
		sprintf(oldSubfolderName, "%02X", oldSubfolderNumber);
		std::string oldSubfolderPath = std::string(f->path) + oldSubfolderName + "/";
		cdirectory_t dd(opendir(oldSubfolderPath.c_str()));
		if (dd) {
			if (!migrateMessageWasPrinted) {
				syslog(LOG_NOTICE, "Migrating data from %s?? to %schunks??", f->path, f->path);
				migrateMessageWasPrinted = true;
			}
			struct dirent *de;
			while (readdir_r(dd.get(), destorage, &de) == 0 && de != nullptr) {
				ChunkFilenameParser filenameParser(de->d_name);
				if (filenameParser.parse() != ChunkFilenameParser::Status::OK) {
					continue;
				}
				std::string newSubfolderPath = std::string(f->path) +
						Chunk::getSubfolderNameGivenChunkId(filenameParser.chunkId()) + "/";
				std::string oldChunkPath = oldSubfolderPath + de->d_name;
				std::string newChunkPath = newSubfolderPath + de->d_name;
				if (rename(oldChunkPath.c_str(), newChunkPath.c_str()) != 0) {
					syslog(LOG_WARNING, "Can't migrate %s to %s: %s",
							oldChunkPath.c_str(), newChunkPath.c_str(), strerr(errno));
					// Probably something is really wrong (ro fs, wrong permissions,
					// new dirs on a different mountpoint) -- don't try to move any chunks more.
					return;
				}
			}
			if (rmdir(oldSubfolderPath.c_str()) != 0) {
				syslog(LOG_WARNING, "Can't remove old directory %s: %s",
						oldSubfolderPath.c_str(), strerr(errno));
			}
		}
	}
}

void* hdd_folder_scan(void *arg) {
	TRACETHIS();
	folder *f = (folder*)arg;
	DIR *dd;
	struct dirent *de,*destorage;
	uint32_t tcheckcnt;
	uint8_t todel;
	uint8_t lastperc,currentperc;
	uint32_t lasttime,currenttime,begintime;

	begintime = time(NULL);

	zassert(pthread_mutex_lock(&folderlock));
	todel = f->todel;
	hdd_refresh_usage(f);
	zassert(pthread_mutex_unlock(&folderlock));

	/* size of name added to size of structure because on some os'es d_name has size of 1 byte */
	destorage = (struct dirent*)malloc(sizeof(struct dirent)+pathconf(f->path,_PC_NAME_MAX)+1);
	passert(destorage);

	if (todel==0) {
		mkdir(f->path, 0755);
	}

	zassert(pthread_mutex_lock(&dclock));
	hddspacechanged = 1;
	zassert(pthread_mutex_unlock(&dclock));

	if (todel==0) {
		for (unsigned subfolderNumber = 0;
				subfolderNumber < Chunk::kNumberOfSubfolders;
				++subfolderNumber) {
			std::string subfolderPath =
					f->path + Chunk::getSubfolderNameGivenNumber(subfolderNumber);
			mkdir(subfolderPath.c_str(), 0755);
		}
		hdd_folder_migrate_directories(f);
	} else if (access((std::string(f->path) + "00").c_str(), F_OK) == 0) {
		syslog(LOG_WARNING, "Old chunk directories exist on a hdd marked as 'to remove' (%s). "
				"This files will not be migrated which makes them not available for reading!",
				f->path);
	}

	/* scan new file names */
	bool scanterm = false;
	tcheckcnt = 0;
	lastperc = 0;
	lasttime = time(NULL);
	for (unsigned subfolderNumber = 0;
			subfolderNumber < Chunk::kNumberOfSubfolders && !scanterm;
			++subfolderNumber) {
		std::string subfolderPath =
				f->path + Chunk::getSubfolderNameGivenNumber(subfolderNumber) + "/";
		dd = opendir(subfolderPath.c_str());
		if (dd) {
			while (readdir_r(dd,destorage,&de)==0 && de!=NULL && !scanterm) {
				ChunkFilenameParser filenameParser(de->d_name);
				if (filenameParser.parse() != ChunkFilenameParser::Status::OK) {
					continue;
				}
				if (Chunk::getSubfolderNumber(filenameParser.chunkId()) != subfolderNumber) {
					syslog(LOG_WARNING, "Chunk %s%s placed in a wrong directory; skipping it.",
							subfolderPath.c_str(), de->d_name);
					continue;
				}
				hdd_add_chunk(f,
						subfolderPath + de->d_name,
						filenameParser.chunkId(),
						filenameParser.chunkFormat(),
						filenameParser.chunkVersion(),
						filenameParser.chunkType(),
						todel);
				tcheckcnt++;
				if (tcheckcnt>=1000) {
					zassert(pthread_mutex_lock(&folderlock));
					if (f->scanstate==SCST_SCANTERMINATE) {
						scanterm = true;
					}
					zassert(pthread_mutex_unlock(&folderlock));
					tcheckcnt = 0;
				}
			}
			closedir(dd);
		}
		currenttime = time(NULL);
		currentperc = (subfolderNumber * 100.0) / 256.0;
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
	free(destorage);
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
	TRACETHIS();
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
	TRACETHIS();
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
	TRACETHIS();
	uint32_t i;
	folder *f,*fn;
	Chunk *c,*cn;
	dopchunk *dc,*dcn;
	cntcond *cc,*ccn;
	lostchunk *lc,*lcn;
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
		try {
			test_chunk_thread.join();
		} catch (std::system_error &e) {
			syslog(LOG_NOTICE, "Failed to join test chunk thread: %s", e.what());
		}
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
				MooseFSChunk* mc = dynamic_cast<MooseFSChunk*>(c);
				if (c->wasChanged && mc) {
					syslog(LOG_WARNING,"hdd_term: CRC not flushed - writing now");
					if (chunk_writecrc(mc)!=STATUS_OK) {
						lzfs_silent_errlog(LOG_WARNING,
								"hdd_term: file:%s - write error", c->filename().c_str());
					}
				}
				if (c->fd>=0) {
					close(c->fd);
				}
				delete c;
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
	TRACETHIS();
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
	TRACETHIS();
	uint32_t l,p;
	int damaged,lfd,td;
	char *pptr;
	struct stat sb;
	folder *f;
	uint8_t lockneeded;
	uint64_t limit;
	uint8_t lmode;

	damaged = 0;
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
			lzfs_pretty_syslog(LOG_WARNING,"hdd space manager: limit on '%s' < chunk size - leaving so small space on hdd is not recommended",pptr);
		} else {
			struct statvfs fsinfo;

			if (statvfs(pptr,&fsinfo)<0) {
				lzfs_pretty_errlog(LOG_NOTICE,"hdd space manager: statvfs on '%s'",pptr);
			} else {
				uint64_t size = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));
				if (limit > size) {
					lzfs_pretty_syslog(LOG_WARNING,"hdd space manager: space to be left free on '%s' (%" PRIu64 ") is greater than real volume size (%" PRIu64 ")",pptr,limit,size);
				}
			}
		}
	}
	if (lmode==2) { // sanity checks
		if (limit==0) {
			lzfs_pretty_syslog(LOG_WARNING,"hdd space manager: limit on '%s' set to zero - using real volume size",pptr);
			lmode = 0;
		} else {
			struct statvfs fsinfo;

			if (statvfs(pptr,&fsinfo)<0) {
				lzfs_pretty_errlog(LOG_NOTICE,"hdd space manager: statvfs on '%s'",pptr);
			} else {
				uint64_t size = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));
				if (limit > size) {
					lzfs_pretty_syslog(LOG_WARNING,"hdd space manager: limit on '%s' (%" PRIu64 ") is greater than real volume size (%" PRIu64 ") - using real volume size",pptr,limit,size);
					lmode = 0;
				}
			}
		}
	}

	std::string dataDir(pptr, l);
	std::string lockfname = dataDir + ".lock";
	lfd = open(lockfname.c_str(),O_RDWR|O_CREAT|O_TRUNC,0640);
	if (lfd<0 && errno==EROFS && td) {
		td = 2;
	} else if (lfd<0) {
		lzfs_pretty_errlog(LOG_WARNING, "can't create lock file %s, marking hdd as damaged",
				lockfname.c_str());
		damaged = 1;
	} else if (lockneeded && lockf(lfd,F_TLOCK,0)<0) {
		int err = errno;
		close(lfd);
		if (err==EAGAIN) {
			throw InitializeException(
					"data folder " + dataDir + " already locked by another process");
		} else {
			lzfs_pretty_syslog(LOG_WARNING, "lockf(%s) failed, marking hdd as damaged: %s",
					lockfname.c_str(), strerr(err));
			damaged = 1;
		}
	} else if (fstat(lfd,&sb)<0) {
		int err = errno;
		close(lfd);
		lzfs_pretty_syslog(LOG_WARNING, "fstat(%s) failed, marking hdd as damaged: %s",
				lockfname.c_str(), strerr(err));
		damaged = 1;
	} else if (lockneeded) {
		zassert(pthread_mutex_lock(&folderlock));
		for (f=folderhead ; f ; f=f->next) {
			if (f->lfd>=0 && f->devid==sb.st_dev) {
				if (f->lockinode==sb.st_ino) {
					std::string fPath = f->path;
					zassert(pthread_mutex_unlock(&folderlock));
					close(lfd);
					throw InitializeException("data folders '" + dataDir + "' and "
							"'" + fPath + "' have the same lockfile");
				} else {
					lzfs_pretty_syslog(LOG_WARNING,
							"data folders '%s' and '%s' are on the same "
							"physical device (could lead to unexpected behaviours)",
							dataDir.c_str(), f->path);
				}
			}
		}
		zassert(pthread_mutex_unlock(&folderlock));
	}
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		if (strcmp(f->path,pptr)==0) {
			f->toremove = 0;
			if (f->damaged) {
				f->scanstate = SCST_SCANNEEDED;
				f->scanprogress = 0;
				f->damaged = damaged;
				f->avail = 0ULL;
				f->total = 0ULL;
				if (lmode==1) {
					f->leavefree = limit;
				} else {
					f->leavefree = gLeaveFree;
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
	f->damaged = damaged;
	f->scanstate = SCST_SCANNEEDED;
	f->scanprogress = 0;
	f->path = strdup(pptr);
	passert(f->path);
	f->toremove = 0;
	if (lmode==1) {
		f->leavefree = limit;
	} else {
		f->leavefree = gLeaveFree;
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
	if (damaged) {
		f->lfd = -1;
	} else {
		f->lfd = lfd;
		f->devid = sb.st_dev;
		f->lockinode = sb.st_ino;
	}
	f->testhead = NULL;
	f->testtail = &(f->testhead);
	f->carry = (double)(random()&0x7FFFFFFF)/(double)(0x7FFFFFFF);
	f->next = folderhead;
	folderhead = f;
	testerreset = 1;
	zassert(pthread_mutex_unlock(&folderlock));
	return 2;
}

static void hdd_folders_reinit(void) {
	TRACETHIS();
	folder *f;
	cstream_t fd;
	std::string hddfname;

	hddfname = cfg_get("HDD_CONF_FILENAME", ETC_PATH "/mfshdd.cfg");
	fd.reset(fopen(hddfname.c_str(),"r"));
	if (!fd) {
		throw InitializeException("can't open hdd config file " + hddfname +": " +
				strerr(errno) + " - new file can be created using " +
				ETC_PATH "/mfshdd.cfg.dist");
	}
	lzfs_pretty_syslog(LOG_INFO, "hdd configuration file %s opened", hddfname.c_str());

	zassert(pthread_mutex_lock(&folderlock));
	folderactions = 0; // stop folder actions
	for (f=folderhead ; f ; f=f->next) {
		f->toremove = 1;
	}
	zassert(pthread_mutex_unlock(&folderlock));

	char buff[1000];
	while (fgets(buff,999,fd.get())) {
		buff[999] = 0;
		hdd_parseline(buff);
	}
	fd.reset();

	zassert(pthread_mutex_lock(&folderlock));
	bool anyDiskAvailable = false;
	for (f=folderhead ; f ; f=f->next) {
		if (f->toremove==0) {
			anyDiskAvailable = true;
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

	if (!anyDiskAvailable) {
		throw InitializeException("no data paths defined in the " + hddfname + " file");
	}
}

void hdd_int_set_chunk_format() {
	ChunkFormat defaultChunkFormat = MooseFSChunkFormat ?
			ChunkFormat::MOOSEFS :
			ChunkFormat::INTERLEAVED;
	ChunkFormat newFormat =
			(cfg_getint32("CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT", 1) != 0)
			? ChunkFormat::MOOSEFS
			: ChunkFormat::INTERLEAVED;
	if (newFormat == ChunkFormat::MOOSEFS) {
		if (defaultChunkFormat != ChunkFormat::MOOSEFS) {
			MooseFSChunkFormat = true;
			syslog(LOG_INFO,"new chunks format set to 'MOOSEFS' format");
		}
	} else {
		if (defaultChunkFormat != ChunkFormat::INTERLEAVED) {
			MooseFSChunkFormat = false;
			syslog(LOG_INFO,"new chunks format set to 'INTERLEAVED' format");
		}
	}
}

void hdd_reload(void) {
	TRACETHIS();
	gAdviseNoCache = cfg_getuint32("HDD_ADVISE_NO_CACHE", 0);

	PerformFsync = cfg_getuint32("PERFORM_FSYNC", 1);

	zassert(pthread_mutex_lock(&testlock));
	HDDTestFreq = cfg_getuint32("HDD_TEST_FREQ",10);
	zassert(pthread_mutex_unlock(&testlock));

	hdd_int_set_chunk_format();
	char *LeaveFreeStr = cfg_getstr("HDD_LEAVE_SPACE_DEFAULT", gLeaveSpaceDefaultDefaultStrValue);
	if (hdd_size_parse(LeaveFreeStr,&gLeaveFree)<0) {
		syslog(LOG_NOTICE,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT parse error - left unchanged");
	}
	free(LeaveFreeStr);
	if (gLeaveFree<0x4000000) {
		syslog(LOG_NOTICE,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT < chunk size - leaving so small space on hdd is not recommended");
	}

	syslog(LOG_NOTICE,"reloading hdd data ...");
	try {
		hdd_folders_reinit();
	} catch (const Exception& ex) {
		lzfs_pretty_syslog(LOG_ERR, "%s", ex.what());
	}
}

int hdd_late_init(void) {
	TRACETHIS();
	zassert(pthread_mutex_lock(&termlock));
	term = 0;
	zassert(pthread_mutex_unlock(&termlock));
	zassert(pthread_create(&testerthread,&thattr,hdd_tester_thread,NULL));
	zassert(pthread_create(&foldersthread,&thattr,hdd_folders_thread,NULL));
	zassert(pthread_create(&delayedthread,&thattr,hdd_delayed_thread,NULL));
	try {
		gWrongCrcNotifier.init(0);
	} catch (std::system_error &e) {
		syslog(LOG_ERR, "Failed to create wrong CRC notifier thread: %s", e.what());
		abort();
	}
	try {
		test_chunk_thread = std::thread(hdd_test_chunk_thread);
	} catch (std::system_error &e) {
		syslog(LOG_ERR, "Failed to create test chunk thread: %s", e.what());
		abort();
	}
	return 0;
}

int hdd_init(void) {
	TRACETHIS();
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

	zassert(pthread_key_create(&hdrbufferkey,free));
	zassert(pthread_key_create(&blockbufferkey,free));

	emptyblockcrc = mycrc32_zeroblock(0,MFSBLOCKSIZE);

	PerformFsync = cfg_getuint32("PERFORM_FSYNC", 1);

	uint64_t leaveSpaceDefaultDefaultValue = 0;
	sassert(hdd_size_parse(gLeaveSpaceDefaultDefaultStrValue, &leaveSpaceDefaultDefaultValue) >= 0);
	sassert(leaveSpaceDefaultDefaultValue > 0);
	LeaveFreeStr = cfg_getstr("HDD_LEAVE_SPACE_DEFAULT", gLeaveSpaceDefaultDefaultStrValue);
	if (hdd_size_parse(LeaveFreeStr,&gLeaveFree)<0) {
		lzfs_pretty_syslog(LOG_WARNING,
				"%s: HDD_LEAVE_SPACE_DEFAULT parse error - using default (%s)",
				cfg_filename().c_str(), gLeaveSpaceDefaultDefaultStrValue);
		gLeaveFree = leaveSpaceDefaultDefaultValue;
	}
	free(LeaveFreeStr);
	if (gLeaveFree<0x4000000) {
		lzfs_pretty_syslog(LOG_WARNING,
				"%s: HDD_LEAVE_SPACE_DEFAULT < chunk size - "
				"leaving so small space on hdd is not recommended",
				cfg_filename().c_str());
	}

	/* this can throw exception*/
	hdd_folders_reinit();

	zassert(pthread_attr_init(&thattr));
	zassert(pthread_attr_setstacksize(&thattr,0x100000));
	zassert(pthread_attr_setdetachstate(&thattr,PTHREAD_CREATE_JOINABLE));

	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		lzfs_pretty_syslog(LOG_INFO, "hdd space manager: path to scan: %s",f->path);
	}
	zassert(pthread_mutex_unlock(&folderlock));
	lzfs_pretty_syslog(LOG_INFO, "hdd space manager: start background hdd scanning "
				"(searching for available chunks)");

	gAdviseNoCache = cfg_getuint32("HDD_ADVISE_NO_CACHE", 0);
	HDDTestFreq = cfg_getuint32("HDD_TEST_FREQ",10);

	MooseFSChunkFormat = true;
	hdd_int_set_chunk_format();
	main_reloadregister(hdd_reload);
	main_timeregister(TIMEMODE_RUN_LATE,60,0,hdd_diskinfo_movestats);
	main_destructregister(hdd_term);

	zassert(pthread_mutex_lock(&termlock));
	term = 1;
	zassert(pthread_mutex_unlock(&termlock));

	return 0;
}

HddspacemgrChunkFileCreator::HddspacemgrChunkFileCreator(
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType)
		: ChunkFileCreator(chunkId, chunkVersion, chunkType),
		  isCreated_(false),
		  isOpen_(false),
		  isCommited_(false) {
}

HddspacemgrChunkFileCreator::~HddspacemgrChunkFileCreator() {
	if (isOpen_) {
		hdd_close(chunkId(), chunkType());
	}
	if (isCreated_ && !isCommited_) {
		hdd_delete(chunkId(), 0, chunkType());
	}
}

void HddspacemgrChunkFileCreator::create() {
	sassert(!isCreated_);
	uint8_t status = hdd_create(chunkId(), 0, chunkType());
	if (status == STATUS_OK) {
		isCreated_ = true;
	} else {
		throw Exception("failed to create chunk", status);
	}
	status = hdd_open(chunkId(), chunkType());
	if (status == STATUS_OK) {
		isOpen_ = true;
	} else {
		throw Exception("failed to open created chunk", status);
	}
}

void HddspacemgrChunkFileCreator::write(uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer) {
	sassert(isOpen_ && !isCommited_);
	uint16_t blocknum = offset / MFSBLOCKSIZE;
	offset = offset % MFSBLOCKSIZE;
	uint8_t status = hdd_write(chunkId(), 0, chunkType(), blocknum, offset, size, crc, buffer);
	if (status != STATUS_OK) {
		throw Exception("failed to write chunk", status);
	}
}

void HddspacemgrChunkFileCreator::commit() {
	sassert(isOpen_ && !isCommited_);
	uint8_t status = hdd_close(chunkId(), chunkType());
	if (status == STATUS_OK) {
		isOpen_ = false;
	} else {
		throw Exception("failed to close chunk", status);
	}
	status = hdd_version(chunkId(), 0, chunkType(), chunkVersion());
	if (status == STATUS_OK) {
		isCommited_ = true;
	} else {
		throw Exception("failed to set chunk's version", status);
	}
}
