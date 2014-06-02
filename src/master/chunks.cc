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

#include "config.h"
#include "master/chunks.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include "common/datapack.h"
#include "common/hashfn.h"
#include "common/massert.h"
#include "common/MFSCommunication.h"
#include "master/checksum.h"
#include "master/chunk_goal_counters.h"
#include "master/filesystem.h"

#ifdef METARESTORE
#  include <time.h>
#else
#  include "common/cfg.h"
#  include "common/main.h"
#  include "common/random.h"
#  include "master/matoclserv.h"
#  include "master/matocsserv.h"
#  include "master/topology.h"
#endif

#define USE_SLIST_BUCKETS 1
#define USE_FLIST_BUCKETS 1
#define USE_CHUNK_BUCKETS 1

#define MINLOOPTIME 1
#define MAXLOOPTIME 7200
#define MAXCPS 10000000
#define MINCPS 10000

#define HASHSIZE 0x100000
#define HASHPOS(chunkid) (((uint32_t)chunkid)&0xFFFFF)

#ifndef METARESTORE

enum {JOBS_INIT,JOBS_EVERYLOOP,JOBS_EVERYSECOND};

/* chunk.operation */
enum {NONE,CREATE,SET_VERSION,DUPLICATE,TRUNCATE,DUPTRUNC};
/* slist.valid */
/* INVALID - wrong version / or got info from chunkserver (IO error etc.)  ->  to delete */
/* DEL - deletion in progress */
/* BUSY - operation in progress */
/* VALID - ok */
/* TDBUSY - to delete + BUSY */
/* TDVALID - want to be deleted */
enum {INVALID,DEL,BUSY,VALID,TDBUSY,TDVALID};

struct slist {
	void *ptr;
	uint8_t valid;
	uint32_t version;
	slist *next;

	bool is_busy() const {
		return valid == BUSY || valid == TDBUSY;
	}
	bool is_valid() const {
		return valid != INVALID && valid != DEL;
	}
	bool is_todel() const {
		return valid == TDVALID || valid == TDBUSY;
	}

	void mark_busy() {
		switch (valid) {
		case VALID:
			valid = BUSY;
			break;
		case TDVALID:
			valid = TDBUSY;
			break;
		default:
			sassert(!"slist::mark_busy(): wrong state");
		}
	}
	void unmark_busy() {
		switch (valid) {
		case BUSY:
			valid = VALID;
			break;
		case TDBUSY:
			valid = TDVALID;
			break;
		default:
			sassert(!"slist::unmark_busy(): wrong state");
		}
	}
	void mark_todel() {
		switch (valid) {
		case VALID:
			valid = TDVALID;
			break;
		case BUSY:
			valid = TDBUSY;
			break;
		default:
			sassert(!"slist::mark_todel(): wrong state");
		}
	}
	void unmark_todel() {
		switch (valid) {
		case TDVALID:
			valid = VALID;
			break;
		case TDBUSY:
			valid = BUSY;
			break;
		default:
			sassert(!"slist::unmark_todel(): wrong state");
		}
	}
};

#ifdef USE_SLIST_BUCKETS
#define SLIST_BUCKET_SIZE 5000

struct slist_bucket {
	slist bucket[SLIST_BUCKET_SIZE];
	uint32_t firstfree;
	slist_bucket *next;
};

static slist_bucket *sbhead = NULL;
static slist *slfreehead = NULL;
#endif /* USE_SLIST_BUCKET */

#endif /* METARESTORE */

#ifndef METARESTORE
static inline slist* slist_malloc();
static inline void slist_free(slist *p);
#endif

class chunk {
public:
	uint64_t chunkid;
	uint64_t checksum;
	chunk *next;
#ifndef METARESTORE
	slist *slisthead;
#endif
	uint32_t version;
	uint32_t lockedto;
private: // public/private sections are mixed here to make the struct as small as possible
	ChunkGoalCounters goalCounters_;
#ifndef METARESTORE
	uint8_t allValidCopies_, regularValidCopies_;
	uint8_t goalInStats_;
#endif
public:
#ifndef METARESTORE
	uint8_t needverincrease:1;
	uint8_t interrupted:1;
	uint8_t operation:4;
#endif
	static uint64_t count;
	static uint64_t allValidCopies[11][11], regularValidCopies[11][11];

	uint8_t goal() const {
		return goalCounters_.combinedGoal();
	}

	// number of files this chunk belongs to
	uint8_t fileCount() const {
		return goalCounters_.fileCount();
	}

	// called when this chunks becomes a part of a file with the given goal
	void addFileWithGoal(uint8_t goal) {
		goalCounters_.addFile(goal);
#ifndef METARESTORE
		updateStats();
#endif
	}

	// called when a file that this chunks belongs to is removed
	void removeFileWithGoal(uint8_t goal) {
		goalCounters_.removeFile(goal);
#ifndef METARESTORE
		updateStats();
#endif
	}

	// called when a file that this chunks belongs to changes goal
	void changeFileGoal(uint8_t prevGoal, uint8_t newGoal) {
		goalCounters_.changeFileGoal(prevGoal, newGoal);
#ifndef METARESTORE
		updateStats();
#endif
	}

#ifndef METARESTORE
	// This method should be called on a new chunk
	void initStats() {
		count++;
		allValidCopies_ = regularValidCopies_ = 0;
		goalInStats_ = 0;
		addToStats();
		updateStats();
	}

	// This method should be called when a chunk is removed
	void freeStats() {
		count--;
		removeFromStats();
	}

	void updateStats() {
		removeFromStats();
		allValidCopies_ = regularValidCopies_ = 0;
		for (slist* s = slisthead; s != nullptr; s = s->next) {
			if (!s->is_valid()) {
				continue;
			}
			allValidCopies_++;
			if (!s->is_todel()) {
				regularValidCopies_++;
			}
		}
		addToStats();
	}

	uint8_t allValidCopiesCount() const {
		return allValidCopies_;
	}

	bool isLocked() const {
		return lockedto >= main_time();
	}

	void markCopyAsHavingWrongVersion(slist *s) {
		s->valid = INVALID;
		updateStats();
	}

	void invalidateCopy(slist *s) {
		s->valid = INVALID;
		s->version = 0;
		updateStats();
	}

	void deleteCopy(slist *s) {
		s->valid = DEL;
		updateStats();
	}

	void unlinkCopy(slist *s, slist **prev_next) {
		*prev_next = s->next;
		slist_free(s);
		updateStats();
	}

	slist* addCopyNoStatsUpdate(void *ptr, uint8_t valid, uint32_t version) {
		slist *s = slist_malloc();
		s->ptr = ptr;
		s->valid = valid;
		s->version = version;
		s->next = slisthead;
		slisthead = s;
		return s;
	}

	slist* addCopy(void *ptr, uint8_t valid, uint32_t version) {
		slist *s = addCopyNoStatsUpdate(ptr, valid, version);
		updateStats();
		return s;
	}

private:
	void removeFromStats() {
		uint8_t limitedGoal = std::min<uint8_t>(10, goalInStats_);
		uint8_t limitedAvc = std::min<uint8_t>(10, allValidCopies_);
		uint8_t limitedRvc = std::min<uint8_t>(10, regularValidCopies_);
		allValidCopies[limitedGoal][limitedAvc]--;
		regularValidCopies[limitedGoal][limitedRvc]--;
	}

	void addToStats() {
		goalInStats_ = goal();
		uint8_t limitedGoal = std::min<uint8_t>(10, goalInStats_);
		uint8_t limitedAvc = std::min<uint8_t>(10, allValidCopies_);
		uint8_t limitedRvc = std::min<uint8_t>(10, regularValidCopies_);
		allValidCopies[limitedGoal][limitedAvc]++;
		regularValidCopies[limitedGoal][limitedRvc]++;
	}
#endif
};

#ifndef METARESTORE
uint64_t chunk::count = 0;
uint64_t chunk::allValidCopies[11][11] = {{0}};
uint64_t chunk::regularValidCopies[11][11] = {{0}};
#endif

#ifdef USE_CHUNK_BUCKETS
#define CHUNK_BUCKET_SIZE 20000
struct chunk_bucket {
	chunk bucket[CHUNK_BUCKET_SIZE];
	uint32_t firstfree;
	chunk_bucket *next;
};

static chunk_bucket *cbhead = NULL;
static chunk *chfreehead = NULL;
#endif /* USE_CHUNK_BUCKETS */

static chunk *chunkhash[HASHSIZE];
static uint64_t nextchunkid=1;
#define LOCKTIMEOUT 120

#define UNUSED_DELETE_TIMEOUT (86400*7)

#ifndef METARESTORE

static uint32_t ReplicationsDelayDisconnect=3600;
static uint32_t ReplicationsDelayInit=300;

static uint32_t MaxWriteRepl;
static uint32_t MaxReadRepl;
static uint32_t MaxDelSoftLimit;
static uint32_t MaxDelHardLimit;
static double TmpMaxDelFrac;
static uint32_t TmpMaxDel;
static uint32_t HashSteps;
static uint32_t HashCPS;
static double AcceptableDifference;

static uint32_t jobshpos;
static uint32_t jobsrebalancecount;
static uint32_t jobsnorepbefore;

static uint32_t starttime;

struct job_info {
	uint32_t del_invalid;
	uint32_t del_unused;
	uint32_t del_diskclean;
	uint32_t del_overgoal;
	uint32_t copy_undergoal;
};

struct loop_info {
	job_info done,notdone;
	uint32_t copy_rebalance;
};

static loop_info chunksinfo = {{0,0,0,0,0},{0,0,0,0,0},0};
static uint32_t chunksinfo_loopstart=0,chunksinfo_loopend=0;

#endif

static uint64_t lastchunkid=0;
static chunk* lastchunkptr=NULL;

#ifndef METARESTORE
static uint32_t stats_deletions=0;
static uint32_t stats_replications=0;

void chunk_stats(uint32_t *del,uint32_t *repl) {
	*del = stats_deletions;
	*repl = stats_replications;
	stats_deletions = 0;
	stats_replications = 0;
}

#endif

static uint64_t gChunksChecksum;

static uint64_t chunk_checksum(const chunk* c) {
	if (c == nullptr) {
		return 0;
	}
	uint64_t checksum = 64517419147637ULL;
	hashCombine(checksum, c->chunkid, c->version, c->lockedto, c->goal(), c->fileCount());
	return checksum;
}

static void chunk_update_checksum(chunk* ch) {
	if (!ch) {
		return;
	}
	removeFromChecksum(gChunksChecksum, ch->checksum);
	ch->checksum = chunk_checksum(ch);
	addToChecksum(gChunksChecksum, ch->checksum);
}

static void chunk_recalculate_checksum() {
	gChunksChecksum = 78765491511151883ULL;
	for (int i = 0; i < HASHSIZE; ++i) {
		for (chunk* ch = chunkhash[i]; ch; ch = ch->next) {
			ch->checksum = chunk_checksum(ch);
			addToChecksum(gChunksChecksum, ch->checksum);
		}
	}
}

uint64_t chunk_checksum(ChecksumMode mode) {
	uint64_t checksum = 46586918175221;
	addToChecksum(checksum, nextchunkid);
	if (mode == ChecksumMode::kForceRecalculate) {
		chunk_recalculate_checksum();
	}
	addToChecksum(checksum, gChunksChecksum);
	return checksum;
}

#ifndef METARESTORE
#ifdef USE_SLIST_BUCKETS
static inline slist* slist_malloc() {
	slist_bucket *sb;
	slist *ret;
	if (slfreehead) {
		ret = slfreehead;
		slfreehead = ret->next;
		return ret;
	}
	if (sbhead==NULL || sbhead->firstfree==SLIST_BUCKET_SIZE) {
		sb = (slist_bucket*)malloc(sizeof(slist_bucket));
		passert(sb);
		sb->next = sbhead;
		sb->firstfree = 0;
		sbhead = sb;
	}
	ret = (sbhead->bucket)+(sbhead->firstfree);
	sbhead->firstfree++;
	return ret;
}

static inline void slist_free(slist *p) {
	p->next = slfreehead;
	slfreehead = p;
}
#else /* USE_SLIST_BUCKETS */

static inline slist* slist_malloc() {
	slist *sl;
	sl = (slist*)malloc(sizeof(slist));
	passert(sl);
	return sl;
}

static inline void slist_free(slist* p) {
	free(p);
}

#endif /* USE_SLIST_BUCKETS */
#endif /* !METARESTORE */

#ifdef USE_CHUNK_BUCKETS
static inline chunk* chunk_malloc() {
	chunk_bucket *cb;
	chunk *ret;
	if (chfreehead) {
		ret = chfreehead;
		chfreehead = ret->next;
		return ret;
	}
	if (cbhead==NULL || cbhead->firstfree==CHUNK_BUCKET_SIZE) {
		cb = new chunk_bucket;
		cb->next = cbhead;
		cb->firstfree = 0;
		cbhead = cb;
	}
	ret = (cbhead->bucket)+(cbhead->firstfree);
	cbhead->firstfree++;
	return ret;
}

static inline void chunk_free(chunk *p) {
	p->next = chfreehead;
	chfreehead = p;
}
#else /* USE_CHUNK_BUCKETS */

static inline chunk* chunk_malloc() {
	return new chunk;
}

static inline void chunk_free(chunk* p) {
	delete p;
}

#endif /* USE_CHUNK_BUCKETS */

chunk* chunk_new(uint64_t chunkid, uint32_t chunkversion) {
	uint32_t chunkpos = HASHPOS(chunkid);
	chunk *newchunk;
	newchunk = chunk_malloc();
	newchunk->next = chunkhash[chunkpos];
	chunkhash[chunkpos] = newchunk;
	newchunk->chunkid = chunkid;
	newchunk->version = chunkversion;
	newchunk->lockedto = 0;
#ifndef METARESTORE
	newchunk->needverincrease = 1;
	newchunk->interrupted = 0;
	newchunk->operation = NONE;
	newchunk->slisthead = NULL;
	newchunk->initStats();
#endif
	lastchunkid = chunkid;
	lastchunkptr = newchunk;
	newchunk->checksum = 0;
	chunk_update_checksum(newchunk);
	return newchunk;
}

chunk* chunk_find(uint64_t chunkid) {
	uint32_t chunkpos = HASHPOS(chunkid);
	chunk *chunkit;
	if (lastchunkid==chunkid) {
		return lastchunkptr;
	}
	for (chunkit = chunkhash[chunkpos] ; chunkit ; chunkit = chunkit->next) {
		if (chunkit->chunkid == chunkid) {
			lastchunkid = chunkid;
			lastchunkptr = chunkit;
			return chunkit;
		}
	}
	return NULL;
}

#ifndef METARESTORE
void chunk_delete(chunk* c) {
	if (lastchunkptr==c) {
		lastchunkid=0;
		lastchunkptr=NULL;
	}
	c->freeStats();
	chunk_free(c);
}

uint32_t chunk_count(void) {
	return chunk::count;
}

void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regularvalidcopies) {
	uint32_t i,j,ag,rg;
	*allchunks = chunk::count;
	*allcopies = 0;
	*regularvalidcopies = 0;
	for (i=1 ; i<=10 ; i++) {
		ag=0;
		rg=0;
		for (j=0 ; j<=10 ; j++) {
			ag += chunk::allValidCopies[j][i];
			rg += chunk::regularValidCopies[j][i];
		}
		*allcopies += ag*i;
		*regularvalidcopies += rg*i;
	}
}

uint32_t chunk_get_missing_count(void) {
	uint32_t res=0;
	uint8_t i;

	for (i=1 ; i<=10 ; i++) {
		res += chunk::allValidCopies[i][0];
	}
	return res;
}

void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid) {
	uint8_t i,j;
	if (matrixid==0) {
		for (i=0 ; i<=10 ; i++) {
			for (j=0 ; j<=10 ; j++) {
				put32bit(&buff, chunk::allValidCopies[i][j]);
			}
		}
	} else if (matrixid==1) {
		for (i=0 ; i<=10 ; i++) {
			for (j=0 ; j<=10 ; j++) {
				put32bit(&buff, chunk::regularValidCopies[i][j]);
			}
		}
	} else {
		memset(buff,0,11*11*4);
	}
}
#endif

/// updates chunk's goal after a file goal has been changed
int chunk_change_file(uint64_t chunkid,uint8_t prevgoal,uint8_t newgoal) {
	chunk *c;
	if (prevgoal==newgoal) {
		return STATUS_OK;
	}
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	try {
		c->changeFileGoal(prevgoal, newgoal);
	} catch (Exception& ex) {
		syslog(LOG_WARNING, "chunk_change_file: %s", ex.what());
		return ERROR_CHUNKLOST;
	}
	chunk_update_checksum(c);
	return STATUS_OK;
}

/// updates chunk's goal after a file with goal `goal' has been removed
static inline int chunk_delete_file_int(chunk *c,uint8_t goal) {
	try {
		c->removeFileWithGoal(goal);
	} catch (Exception& ex) {
		syslog(LOG_WARNING, "chunk_delete_file_int: %s", ex.what());
		return ERROR_CHUNKLOST;
	}
	chunk_update_checksum(c);
	return STATUS_OK;
}

/// updates chunk's goal after a file with goal `goal' has been added
static inline int chunk_add_file_int(chunk *c,uint8_t goal) {
	try {
		c->addFileWithGoal(goal);
	} catch (Exception& ex) {
		syslog(LOG_WARNING, "chunk_add_file_int: %s", ex.what());
		return ERROR_CHUNKLOST;
	}
	chunk_update_checksum(c);
	return STATUS_OK;
}

int chunk_delete_file(uint64_t chunkid,uint8_t goal) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	return chunk_delete_file_int(c,goal);
}

int chunk_add_file(uint64_t chunkid,uint8_t goal) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	return chunk_add_file_int(c,goal);
}

int chunk_unlock(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->lockedto=0;
	chunk_update_checksum(c);
	return STATUS_OK;
}

#ifndef METARESTORE

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies) {
	chunk *c;
	*vcopies = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	*vcopies = c->allValidCopiesCount();
	return STATUS_OK;
}
#endif


#ifndef METARESTORE
int chunk_multi_modify(uint64_t *nchunkid, uint64_t ochunkid, uint8_t goal, uint8_t *opflag,
		bool quota_exceeded) {
	void* ptrs[65536];
	uint16_t servcount;
	slist *os,*s;
	uint32_t i;
#else
int chunk_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t opflag) {
#endif
	chunk *oc,*c;

	if (ochunkid==0) { // new chunk
#ifndef METARESTORE
		if (quota_exceeded) {
			return ERROR_QUOTA;
		}
		servcount = matocsserv_getservers_wrandom(ptrs,goal);
		if (servcount==0) {
			uint16_t uscount,tscount;
			double minusage,maxusage;
			matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);
			if (uscount>0 && (uint32_t)(main_time())>(starttime+600)) { // if there are chunkservers and it's at least one minute after start then it means that there is no space left
				return ERROR_NOSPACE;
			} else {
				return ERROR_NOCHUNKSERVERS;
			}
		}
#endif
		c = chunk_new(nextchunkid++, 1);
#ifndef METARESTORE
		c->interrupted = 0;
		c->operation = CREATE;
#endif
		chunk_add_file_int(c,goal);
#ifndef METARESTORE
		for (i=0 ; i<servcount ; i++) {
			s = c->addCopyNoStatsUpdate(ptrs[i], BUSY, c->version);
			matocsserv_send_createchunk(s->ptr,c->chunkid,c->version);
		}
		c->updateStats();
		*opflag=1;
#endif
		*nchunkid = c->chunkid;
	} else {
		c = NULL;
		oc = chunk_find(ochunkid);
		if (oc==NULL) {
			return ERROR_NOCHUNK;
		}
#ifndef METARESTORE
		if (oc->isLocked()) {
			return ERROR_LOCKED;
		}
#endif
		if (oc->fileCount() == 1) { // refcount==1
			*nchunkid = ochunkid;
			c = oc;
#ifndef METARESTORE

			if (c->operation!=NONE) {
				return ERROR_CHUNKBUSY;
			}
			if (c->needverincrease) {
				i=0;
				for (s=c->slisthead ;s ; s=s->next) {
					if (s->is_valid()) {
						if (!s->is_busy()) {
							s->mark_busy();
						}
						s->version = c->version+1;
						matocsserv_send_setchunkversion(s->ptr,ochunkid,c->version+1,c->version);
						i++;
					}
				}
				if (i>0) {
					c->interrupted = 0;
					c->operation = SET_VERSION;
					c->version++;
					*opflag=1;
				} else {
					return ERROR_CHUNKLOST;
				}
			} else {
				*opflag=0;
			}
#else
			if (opflag) {
				c->version++;
			}
#endif
		} else {
			if (oc->fileCount() == 0) { // it's serious structure error
#ifndef METARESTORE
				syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016" PRIX64 ")",ochunkid);
#else
				printf("serious structure inconsistency: (chunkid:%016" PRIX64 ")\n",ochunkid);
#endif
				return ERROR_CHUNKLOST; // ERROR_STRUCTURE
			}
#ifndef METARESTORE
			if (quota_exceeded) {
				return ERROR_QUOTA;
			}
			i=0;
			for (os=oc->slisthead ;os ; os=os->next) {
				if (os->is_valid()) {
					if (c==NULL) {
#endif
						c = chunk_new(nextchunkid++, 1);
#ifndef METARESTORE
						c->interrupted = 0;
						c->operation = DUPLICATE;
#endif
						chunk_delete_file_int(oc,goal);
						chunk_add_file_int(c,goal);
#ifndef METARESTORE
					}
					s = c->addCopyNoStatsUpdate(os->ptr, BUSY, c->version);
					matocsserv_send_duplicatechunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version);
					i++;
				}
			}
			if (c!=NULL) {
				c->updateStats();
			}
			if (i>0) {
#endif
				*nchunkid = c->chunkid;
#ifndef METARESTORE
				*opflag=1;
			} else {
				return ERROR_CHUNKLOST;
			}
#endif
		}
	}

#ifndef METARESTORE
	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
#else
	c->lockedto=ts+LOCKTIMEOUT;
#endif
	chunk_update_checksum(c);
	return STATUS_OK;
}

#ifndef METARESTORE
int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t goal,
		bool quota_exceeded) {
	slist *os,*s;
	uint32_t i;
#else
int chunk_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal) {
#endif
	chunk *oc,*c;

	c=NULL;
	oc = chunk_find(ochunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
#ifndef METARESTORE
	if (oc->isLocked()) {
		return ERROR_LOCKED;
	}
#endif
	if (oc->fileCount() == 1) { // refcount==1
		*nchunkid = ochunkid;
		c = oc;
#ifndef METARESTORE
		if (c->operation!=NONE) {
			return ERROR_CHUNKBUSY;
		}
		i=0;
		for (s=c->slisthead ;s ; s=s->next) {
			if (s->is_valid()) {
				if (!s->is_busy()) {
					s->mark_busy();
				}
				s->version = c->version+1;
				matocsserv_send_truncatechunk(s->ptr,ochunkid,length,c->version+1,c->version);
				i++;
			}
		}
		if (i>0) {
			c->interrupted = 0;
			c->operation = TRUNCATE;
			c->version++;
		} else {
			return ERROR_CHUNKLOST;
		}
#else
		c->version++;
#endif
	} else {
		if (oc->fileCount() == 0) { // it's serious structure error
#ifndef METARESTORE
			syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016" PRIX64 ")",ochunkid);
#else
			printf("serious structure inconsistency: (chunkid:%016" PRIX64 ")\n",ochunkid);
#endif
			return ERROR_CHUNKLOST; // ERROR_STRUCTURE
		}
#ifndef METARESTORE
		if (quota_exceeded) {
			return ERROR_QUOTA;
		}
		i=0;
		for (os=oc->slisthead ;os ; os=os->next) {
			if (os->is_valid()) {
				if (c==NULL) {
#endif
					c = chunk_new(nextchunkid++, 1);
#ifndef METARESTORE
					c->interrupted = 0;
					c->operation = DUPTRUNC;
#endif
					chunk_delete_file_int(oc,goal);
					chunk_add_file_int(c,goal);
#ifndef METARESTORE
				}
				s = c->addCopyNoStatsUpdate(os->ptr, BUSY, c->version);
				matocsserv_send_duptruncchunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version,length);
				i++;
			}
		}
		if (c!=NULL) {
			c->updateStats();
		}
		if (i>0) {
#endif
			*nchunkid = c->chunkid;
#ifndef METARESTORE
		} else {
			return ERROR_CHUNKLOST;
		}
#endif
	}

#ifndef METARESTORE
	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
#else
	c->lockedto=ts+LOCKTIMEOUT;
#endif
	chunk_update_checksum(c);
	return STATUS_OK;
}

#ifndef METARESTORE
int chunk_repair(uint8_t goal,uint64_t ochunkid,uint32_t *nversion) {
	uint32_t bestversion;
	chunk *c;
	slist *s;

	*nversion=0;
	if (ochunkid==0) {
		return 0; // not changed
	}

	c = chunk_find(ochunkid);
	if (c==NULL) { // no such chunk - erase (nchunkid already is 0 - so just return with "changed" status)
		return 1;
	}
	if (c->isLocked()) { // can't repair locked chunks - but if it's locked, then likely it doesn't need to be repaired
		return 0;
	}
	bestversion = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid == VALID || s->valid == TDVALID || s->valid == BUSY || s->valid == TDBUSY) { // found chunk that is ok - so return
			return 0;
		}
		if (s->valid == INVALID) {
			if (s->version>=bestversion) {
				bestversion = s->version;
			}
		}
	}
	if (bestversion==0) { // didn't find sensible chunk - so erase it
		chunk_delete_file_int(c,goal);
		return 1;
	}
	c->version = bestversion;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid == INVALID && s->version==bestversion) {
			s->valid = VALID;
		}
	}
	*nversion = bestversion;
	c->needverincrease=1;
	c->updateStats();
	chunk_update_checksum(c);
	return 1;
}
#else
int chunk_set_version(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->version = version;
	chunk_update_checksum(c);
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
void chunk_emergency_increase_version(chunk *c) {
	slist *s;
	uint32_t i;
	i=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->is_valid()) {
			if (!s->is_busy()) {
				s->mark_busy();
			}
			s->version = c->version+1;
			matocsserv_send_setchunkversion(s->ptr,c->chunkid,c->version+1,c->version);
			i++;
		}
	}
	if (i>0) { // should always be true !!!
		c->interrupted = 0;
		c->operation = SET_VERSION;
		c->version++;
	} else {
		matoclserv_chunk_status(c->chunkid,ERROR_CHUNKLOST);
	}
	fs_incversion(c->chunkid);
	chunk_update_checksum(c);
}
#else
int chunk_increase_version(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->version++;
	chunk_update_checksum(c);
	return STATUS_OK;
}
#endif

#ifndef METARESTORE

typedef struct locsort {
	uint32_t ip;
	uint16_t port;
	uint32_t dist;
	uint32_t rnd;
} locsort;

int chunk_locsort_cmp(const void *aa,const void *bb) {
	const locsort *a = (const locsort*)aa;
	const locsort *b = (const locsort*)bb;
	if (a->dist<b->dist) {
		return -1;
	} else if (a->dist>b->dist) {
		return 1;
	} else if (a->rnd<b->rnd) {
		return -1;
	} else if (a->rnd>b->rnd) {
		return 1;
	}
	return 0;
}

int chunk_getversionandlocations(uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t loc[100*6]) {
	chunk *c;
	slist *s;
	uint8_t i;
	uint8_t cnt;
	uint8_t *wptr;
	locsort lstab[100];

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	*version = c->version;
	cnt=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->is_valid()) {
			if (cnt<100 && matocsserv_getlocation(s->ptr,&(lstab[cnt].ip),&(lstab[cnt].port))==0) {
				lstab[cnt].dist = topology_distance(lstab[cnt].ip,cuip); // in the future prepare more sofisticated distance function
				lstab[cnt].rnd = rndu32();
				cnt++;
			}
		}
	}
	qsort(lstab,cnt,sizeof(locsort),chunk_locsort_cmp);
	wptr = loc;
	for (i=0 ; i<cnt ; i++) {
		put32bit(&wptr,lstab[i].ip);
		put16bit(&wptr,lstab[i].port);
	}
	*count = cnt;
	return STATUS_OK;
}

void chunk_server_has_chunk(void *ptr,uint64_t chunkid,uint32_t version) {
	chunk *c;
	slist *s;
	const uint32_t new_version = version & 0x7FFFFFFF;
	const bool todel = version & 0x80000000;
	c = chunk_find(chunkid);
	if (c==NULL) {
		// syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016" PRIX64 "_%08" PRIX32 "), so create it for future deletion",chunkid,version);
		if (chunkid>=nextchunkid) {
			nextchunkid=chunkid+1;
		}
		c = chunk_new(chunkid, new_version);
		c->lockedto = (uint32_t)main_time()+UNUSED_DELETE_TIMEOUT;
		chunk_update_checksum(c);
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr==ptr) {
			// This server already notified us about its copy.
			// We normally don't get repeated notifications about the same copy, but
			// they can arrive after chunkserver configuration reload (particularly,
			// when folders change their 'to delete' status) or due to bugs.
			// Let's try to handle them as well as we can.
			switch (s->valid) {
			case DEL:
				// We requested deletion, but the chunkserver 'has' this copy again.
				// Repeat deletion request.
				c->invalidateCopy(s);
				// fallthrough
			case INVALID:
				// leave this copy alone
				return;
			default:
				break;
			}
			if (s->version != new_version) {
				syslog(LOG_WARNING, "chunk %016" PRIX64 ": master data indicated "
						"version %08" PRIX32 ", chunkserver reports %08"
						PRIX32 "!!! Updating master data.", c->chunkid,
						s->version, new_version);
				s->version = new_version;
			}
			if (s->version != c->version) {
				c->markCopyAsHavingWrongVersion(s);
				return;
			}
			if (!s->is_todel() && todel) {
				s->mark_todel();
				c->updateStats();
			}
			if (s->is_todel() && !todel) {
				s->unmark_todel();
				c->updateStats();
			}
			return;
		}
	}
	const uint8_t state = (new_version == c->version) ?
			(todel ? TDVALID : VALID) : INVALID;
	s = c->addCopy(ptr, state, new_version);
}

void chunk_damaged(void *ptr,uint64_t chunkid) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		// syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016" PRIX64 "), so create it for future deletion",chunkid);
		if (chunkid>=nextchunkid) {
			nextchunkid=chunkid+1;
		}
		c = chunk_new(chunkid, 0);
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr==ptr) {
			c->invalidateCopy(s);
			c->needverincrease=1;
			return;
		}
	}
	s = c->addCopyNoStatsUpdate(ptr, INVALID, 0);
	c->needverincrease=1;
}

void chunk_lost(void *ptr,uint64_t chunkid) {
	chunk *c;
	slist **sptr,*s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return;
	}
	sptr=&(c->slisthead);
	while ((s=*sptr)) {
		if (s->ptr==ptr) {
			c->unlinkCopy(s, sptr);
			c->needverincrease=1;
		} else {
			sptr = &(s->next);
		}
	}
}

void chunk_server_disconnected(void *ptr) {
	chunk *c;
	slist *s,**st;
	uint32_t i;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next) {
			st = &(c->slisthead);
			while (*st) {
				s = *st;
				if (s->ptr == ptr) {
					c->unlinkCopy(s, st);
					c->needverincrease=1;
				} else {
					st = &(s->next);
				}
			}
			if (c->operation!=NONE) {
				bool any_copy_busy = false;
				uint8_t valid_copies = 0;
				for (s=c->slisthead ; s ; s=s->next) {
					any_copy_busy |= s->is_busy();
					valid_copies += s->is_valid() ? 1 : 0;
				}
				if (any_copy_busy) {
					c->interrupted = 1;
				} else {
					if (valid_copies > 0) {
						chunk_emergency_increase_version(c);
					} else {
						matoclserv_chunk_status(c->chunkid,ERROR_NOTDONE);
						c->operation=NONE;
					}
				}
			}
		}
	}
	fs_cs_disconnected();
}

void chunk_got_delete_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	slist *s,**st;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (s->ptr == ptr) {
			if (s->valid!=DEL) {
				syslog(LOG_WARNING,"got unexpected delete status");
			}
			c->unlinkCopy(s, st);
		} else {
			st = &(s->next);
		}
	}
	if (status!=0) {
		return ;
	}
}

void chunk_got_replicate_status(void *ptr,uint64_t chunkid,uint32_t version,uint8_t status) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	if (status!=0) {
		return ;
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr == ptr) {
			syslog(LOG_WARNING,"got replication status from server which had had that chunk before (chunk:%016" PRIX64 "_%08" PRIX32 ")",chunkid,version);
			if (s->valid==VALID && version!=c->version) {
				s->version = version;
				c->markCopyAsHavingWrongVersion(s);
			}
			return;
		}
	}
	const uint8_t state = (c->isLocked() || version != c->version) ?
			INVALID : VALID;
	s = c->addCopy(ptr, state, version);
}


void chunk_operation_status(chunk *c,uint8_t status,void *ptr) {
	slist *s;
	uint8_t valid_copies = 0;
	bool any_copy_busy = false;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr == ptr) {
			if (status!=0) {
				c->interrupted = 1; // increase version after finish, just in case
				c->invalidateCopy(s);
			} else {
				if (s->is_busy()) {
					s->unmark_busy();
				}
			}
		}
		any_copy_busy |= s->is_busy();
		valid_copies += s->is_valid() ? 1 : 0;
	}
	if (!any_copy_busy) {
		if (valid_copies > 0) {
			if (c->interrupted) {
				chunk_emergency_increase_version(c);
			} else {
				matoclserv_chunk_status(c->chunkid,STATUS_OK);
				c->operation=NONE;
				c->needverincrease = 0;
			}
		} else {
			matoclserv_chunk_status(c->chunkid,ERROR_NOTDONE);
			c->operation=NONE;
		}
	}
}

void chunk_got_chunkop_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_create_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_duplicate_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_setversion_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_truncate_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_duptrunc_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

/* ----------------------- */
/* JOBS (DELETE/REPLICATE) */
/* ----------------------- */

void chunk_store_info(uint8_t *buff) {
	put32bit(&buff,chunksinfo_loopstart);
	put32bit(&buff,chunksinfo_loopend);
	put32bit(&buff,chunksinfo.done.del_invalid);
	put32bit(&buff,chunksinfo.notdone.del_invalid);
	put32bit(&buff,chunksinfo.done.del_unused);
	put32bit(&buff,chunksinfo.notdone.del_unused);
	put32bit(&buff,chunksinfo.done.del_diskclean);
	put32bit(&buff,chunksinfo.notdone.del_diskclean);
	put32bit(&buff,chunksinfo.done.del_overgoal);
	put32bit(&buff,chunksinfo.notdone.del_overgoal);
	put32bit(&buff,chunksinfo.done.copy_undergoal);
	put32bit(&buff,chunksinfo.notdone.copy_undergoal);
	put32bit(&buff,chunksinfo.copy_rebalance);
}

//jobs state: jobshpos

void chunk_do_jobs(chunk *c,uint16_t scount,double minusage,double maxusage) {
	slist *s;
	static void* ptrs[65535];
	static uint16_t servcount;
	static uint32_t min,max;
	void* rptrs[65536];
	uint16_t rservcount;
	void *srcptr;
	uint16_t i;
	uint32_t vc,tdc,ivc,bc,tdb,dc;
	static loop_info inforec;
	static uint32_t delnotdone;
	static uint32_t deldone;
	static uint32_t prevtodeletecount;
	static uint32_t delloopcnt;

	if (c==NULL) {
		if (scount==JOBS_INIT) { // init tasks
			delnotdone = 0;
			deldone = 0;
			prevtodeletecount = 0;
			delloopcnt = 0;
			memset(&inforec,0,sizeof(loop_info));
		} else if (scount==JOBS_EVERYLOOP) { // every loop tasks
			delloopcnt++;
			if (delloopcnt>=16) {
				uint32_t todeletecount = deldone+delnotdone;
				delloopcnt=0;
				if ((delnotdone > deldone) && (todeletecount > prevtodeletecount)) {
					TmpMaxDelFrac *= 1.5;
					if (TmpMaxDelFrac>MaxDelHardLimit) {
						syslog(LOG_NOTICE,"DEL_LIMIT hard limit (%" PRIu32 " per server) reached",MaxDelHardLimit);
						TmpMaxDelFrac=MaxDelHardLimit;
					}
					TmpMaxDel = TmpMaxDelFrac;
					syslog(LOG_NOTICE,"DEL_LIMIT temporary increased to: %" PRIu32 " per server",TmpMaxDel);
				}
				if ((todeletecount < prevtodeletecount) && (TmpMaxDelFrac > MaxDelSoftLimit)) {
					TmpMaxDelFrac /= 1.5;
					if (TmpMaxDelFrac<MaxDelSoftLimit) {
						syslog(LOG_NOTICE,"DEL_LIMIT back to soft limit (%" PRIu32 " per server)",MaxDelSoftLimit);
						TmpMaxDelFrac = MaxDelSoftLimit;
					}
					TmpMaxDel = TmpMaxDelFrac;
					syslog(LOG_NOTICE,"DEL_LIMIT decreased back to: %" PRIu32 " per server",TmpMaxDel);
				}
				prevtodeletecount = todeletecount;
				delnotdone = 0;
				deldone = 0;
			}
			chunksinfo = inforec;
			memset(&inforec,0,sizeof(inforec));
			chunksinfo_loopstart = chunksinfo_loopend;
			chunksinfo_loopend = main_time();
		} else if (scount==JOBS_EVERYSECOND) { // every second tasks
			servcount=0;
		}
		return;
	}

	// step 0. Update chunk's statistics
	// Just in case if somewhere is a bug and updateStats was not called
	c->updateStats();

	// step 1. calculate number of valid and invalid copies
	vc=tdc=ivc=bc=tdb=dc=0;
	for (s=c->slisthead ; s ; s=s->next) {
		switch (s->valid) {
		case INVALID:
			ivc++;
			break;
		case TDVALID:
			tdc++;
			break;
		case VALID:
			vc++;
			break;
		case TDBUSY:
			tdb++;
			break;
		case BUSY:
			bc++;
			break;
		case DEL:
			dc++;
			break;
		}
	}

	// step 2. check number of copies
	if (tdc+vc+tdb+bc==0 && ivc>0 && c->fileCount() > 0) {
		syslog(LOG_WARNING,"chunk %016" PRIX64 " has only invalid copies (%" PRIu32 ") - please repair it manually",c->chunkid,ivc);
		for (s=c->slisthead ; s ; s=s->next) {
			syslog(LOG_NOTICE,"chunk %016" PRIX64 "_%08" PRIX32 " - invalid copy on (%s - ver:%08" PRIX32 ")",c->chunkid,c->version,matocsserv_getstrip(s->ptr),s->version);
		}
		return ;
	}

	// step 3. delete invalid copies
	for (s=c->slisthead ; s ; s=s->next) {
		if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
			if (!s->is_valid()) {
				if (s->valid==DEL) {
					syslog(LOG_WARNING,"chunk hasn't been deleted since previous loop - retry");
				}
				s->valid = DEL;
				stats_deletions++;
				matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
				inforec.done.del_invalid++;
				deldone++;
				dc++;
				ivc--;
			}
		} else {
			if (s->valid==INVALID) {
				inforec.notdone.del_invalid++;
				delnotdone++;
			}
		}
	}

	// step 4. return if chunk is during some operation
	if (c->operation!=NONE || (c->isLocked())) {
		return ;
	}

	// step 5. check busy count
	if ((bc+tdb)>0) {
		syslog(LOG_WARNING,"chunk %016" PRIX64 " has unexpected BUSY copies",c->chunkid);
		return ;
	}

	// step 6. delete unused chunk
	if (c->fileCount()==0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
				if (s->is_valid() && !s->is_busy()) {
					c->deleteCopy(s);
					c->needverincrease=1;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,c->version);
					inforec.done.del_unused++;
					deldone++;
				}
			} else {
				if (s->valid==VALID || s->valid==TDVALID) {
					inforec.notdone.del_unused++;
					delnotdone++;
				}
			}
		}
		return ;
	}

	// step 7a. if chunk has too many copies and some of them have status TODEL then delete them
	/* GONE */

	// step 7b. if chunk has too many copies then delete some of them
	if (vc > c->goal()) {
		uint8_t prevdone;
		// syslog(LOG_WARNING,"vc (%" PRIu32 ") > goal (%" PRIu32 ") - delete",vc,c->goal());
		if (servcount==0) {
			servcount = matocsserv_getservers_ordered(ptrs,AcceptableDifference/2.0,&min,&max);
		}
		inforec.notdone.del_overgoal+=(vc-(c->goal()));
		delnotdone+=(vc-(c->goal()));
		prevdone = 1;
		for (i=0 ; i<servcount && vc>c->goal() && prevdone; i++) {
			for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next) {}
			if (s && s->valid==VALID) {
				if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
					c->deleteCopy(s);
					c->needverincrease=1;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					inforec.done.del_overgoal++;
					inforec.notdone.del_overgoal--;
					deldone++;
					delnotdone--;
					vc--;
					dc++;
				} else {
					prevdone=0;
				}
			}
		}
		return;
	}

	// step 7c. if chunk has one copy on each server and some of them have status TODEL then delete one of it
	if (vc+tdc>=scount && vc<c->goal() && tdc>0 && vc+tdc>1) {
		uint8_t prevdone;
		// syslog(LOG_WARNING,"vc+tdc (%" PRIu32 ") >= scount (%" PRIu32 ") and vc (%" PRIu32 ") < goal (%" PRIu32 ") and tdc (%" PRIu32 ") > 0 and vc+tdc > 1 - delete",vc+tdc,scount,vc,c->goal(),tdc);
		prevdone = 0;
		for (s=c->slisthead ; s && prevdone==0 ; s=s->next) {
			if (s->valid==TDVALID) {
				if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
					c->deleteCopy(s);
					c->needverincrease=1;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					inforec.done.del_diskclean++;
					tdc--;
					dc++;
					prevdone = 1;
				} else {
					inforec.notdone.del_diskclean++;
				}
			}
		}
		return;
	}

	//step 8. if chunk has number of copies less than goal then make another copy of this chunk
	if (c->goal() > vc && vc+tdc > 0) {
		if (jobsnorepbefore<(uint32_t)main_time()) {
			uint32_t rgvc,rgtdc;
			rservcount = matocsserv_getservers_lessrepl(rptrs,MaxWriteRepl);
			rgvc=0;
			rgtdc=0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (matocsserv_replication_read_counter(s->ptr)<MaxReadRepl) {
					if (s->valid==VALID) {
						rgvc++;
					} else if (s->valid==TDVALID) {
						rgtdc++;
					}
				}
			}
			if (rgvc+rgtdc>0 && rservcount>0) { // have at least one server to read from and at least one to write to
				for (i=0 ; i<rservcount ; i++) {
					for (s=c->slisthead ; s && s->ptr!=rptrs[i] ; s=s->next) {}
					if (!s) {
						uint32_t r;
						if (rgvc>0) { // if there are VALID copies then make copy of one VALID chunk
							r = 1+rndu32_ranged(rgvc);
							srcptr = NULL;
							for (s=c->slisthead ; s && r>0 ; s=s->next) {
								if (matocsserv_replication_read_counter(s->ptr)<MaxReadRepl && s->valid==VALID) {
									r--;
									srcptr = s->ptr;
								}
							}
						} else { // if not then use TDVALID chunks.
							r = 1+rndu32_ranged(rgtdc);
							srcptr = NULL;
							for (s=c->slisthead ; s && r>0 ; s=s->next) {
								if (matocsserv_replication_read_counter(s->ptr)<MaxReadRepl && s->valid==TDVALID) {
									r--;
									srcptr = s->ptr;
								}
							}
						}
						if (srcptr) {
							stats_replications++;
							matocsserv_send_replicatechunk(rptrs[i],c->chunkid,c->version,srcptr);
							c->needverincrease=1;
							inforec.done.copy_undergoal++;
							return;
						}
					}
				}
			}
		}
		inforec.notdone.copy_undergoal++;
	}

	// step 8. if chunk has number of copies less than goal then make another copy of this chunk
	/* GONE */

	if (chunksinfo.notdone.copy_undergoal>0 && chunksinfo.done.copy_undergoal>0) {
		return;
	}

	// step 9. if there is too big difference between chunkservers then make copy of chunk from server with biggest disk usage on server with lowest disk usage
	if (c->goal() >= vc && vc+tdc>0 && (maxusage-minusage)>AcceptableDifference) {
		if (servcount==0) {
			servcount = matocsserv_getservers_ordered(ptrs,AcceptableDifference/2.0,&min,&max);
		}
		if (min>0 || max>0) {
			void *srcserv=NULL;
			void *dstserv=NULL;
			if (max>0) {
				for (i=0 ; i<max && srcserv==NULL ; i++) {
					if (matocsserv_replication_read_counter(ptrs[servcount-1-i])<MaxReadRepl) {
						for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srcserv=s->ptr;
						}
					}
				}
			} else {
				for (i=0 ; i<(servcount-min) && srcserv==NULL ; i++) {
					if (matocsserv_replication_read_counter(ptrs[servcount-1-i])<MaxReadRepl) {
						for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srcserv=s->ptr;
						}
					}
				}
			}
			if (srcserv!=NULL) {
				if (min>0) {
					for (i=0 ; i<min && dstserv==NULL ; i++) {
						if (matocsserv_replication_write_counter(ptrs[i])<MaxWriteRepl) {
							for (s=c->slisthead ; s && s->ptr!=ptrs[i] ; s=s->next) {}
							if (s==NULL) {
								dstserv=ptrs[i];
							}
						}
					}
				} else {
					for (i=0 ; i<servcount-max && dstserv==NULL ; i++) {
						if (matocsserv_replication_write_counter(ptrs[i])<MaxWriteRepl) {
							for (s=c->slisthead ; s && s->ptr!=ptrs[i] ; s=s->next) {}
							if (s==NULL) {
								dstserv=ptrs[i];
							}
						}
					}
				}
				if (dstserv!=NULL) {
					stats_replications++;
					matocsserv_send_replicatechunk(dstserv,c->chunkid,c->version,srcserv);
					c->needverincrease=1;
					inforec.copy_rebalance++;
				}
			}
		}
	}
}

void chunk_jobs_main(void) {
	uint32_t i,l,lc,r;
	uint16_t uscount,tscount;
	static uint16_t lasttscount=0;
	static uint16_t maxtscount=0;
	double minusage,maxusage;
	chunk *c,**cp;

	if (starttime+ReplicationsDelayInit>main_time()) {
		return;
	}

	matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);

	if (tscount<lasttscount) {              // servers disconnected
		jobsnorepbefore = main_time()+ReplicationsDelayDisconnect;
	} else if (tscount>lasttscount) {       // servers connected
		if (tscount>=maxtscount) {
			maxtscount = tscount;
			jobsnorepbefore = main_time();
		}
	} else if (tscount<maxtscount && (uint32_t)main_time()>jobsnorepbefore) {
		maxtscount = tscount;
	}
	lasttscount = tscount;

	if (minusage>maxusage) {
		return;
	}

	chunk_do_jobs(NULL,JOBS_EVERYSECOND,0.0,0.0); // every second tasks
	lc = 0;
	for (i=0 ; i<HashSteps && lc<HashCPS ; i++) {
		if (jobshpos==0) {
			chunk_do_jobs(NULL,JOBS_EVERYLOOP,0.0,0.0); // every loop tasks
		}
		// delete unused chunks from structures
		l=0;
		cp = &(chunkhash[jobshpos]);
		while ((c=*cp)!=NULL) {
			if (c->fileCount()==0 && c->slisthead==NULL) {
				*cp = (c->next);
				chunk_delete(c);
			} else {
				cp = &(c->next);
				l++;
				lc++;
			}
		}
		if (l>0) {
			r = rndu32_ranged(l);
			l=0;
		// do jobs on rest of them
			for (c=chunkhash[jobshpos] ; c ; c=c->next) {
				if (l>=r) {
					chunk_do_jobs(c,uscount,minusage,maxusage);
				}
				l++;
			}
			l=0;
			for (c=chunkhash[jobshpos] ; l<r && c ; c=c->next) {
				chunk_do_jobs(c,uscount,minusage,maxusage);
				l++;
			}
		}
		jobshpos+=123; // if HASHSIZE is any power of 2 then any odd number is good here
		jobshpos%=HASHSIZE;
	}
}

#endif

#define CHUNKFSIZE 16
#define CHUNKCNT 1000

#ifdef METARESTORE

void chunk_dump(void) {
	chunk *c;
	uint32_t i;

	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next) {
			printf("*|i:%016" PRIX64 "|v:%08" PRIX32 "|g:%" PRIu8 "|t:%10" PRIu32 "\n",c->chunkid,c->version,c->goal(),c->lockedto);
		}
	}
}

#endif

int chunk_load(FILE *fd) {
	uint8_t hdr[8];
	uint8_t loadbuff[CHUNKFSIZE];
	const uint8_t *ptr;
	int32_t r;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version,lockedto;

#ifndef METARESTORE
	chunk::count = 0;
#endif
	if (fread(hdr,1,8,fd)!=8) {
		return -1;
	}
	ptr = hdr;
	nextchunkid = get64bit(&ptr);
	for (;;) {
		r = fread(loadbuff,1,CHUNKFSIZE,fd);
		if (r!=CHUNKFSIZE) {
			return -1;
		}
		ptr = loadbuff;
		chunkid = get64bit(&ptr);
		if (chunkid>0) {
			version = get32bit(&ptr);
			lockedto = get32bit(&ptr);
			c = chunk_new(chunkid, version);
			c->lockedto = lockedto;
		} else {
			version = get32bit(&ptr);
			lockedto = get32bit(&ptr);
			if (version==0 && lockedto==0) {
				return 0;
			} else {
				return -1;
			}
		}
	}
	return 0; // unreachable
}

void chunk_store(FILE *fd) {
	uint8_t hdr[8];
	uint8_t storebuff[CHUNKFSIZE*CHUNKCNT];
	uint8_t *ptr;
	uint32_t i,j;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version;
	uint32_t lockedto;
	ptr = hdr;
	put64bit(&ptr,nextchunkid);
	if (fwrite(hdr,1,8,fd)!=(size_t)8) {
		return;
	}
	j=0;
	ptr = storebuff;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next) {
			chunkid = c->chunkid;
			put64bit(&ptr,chunkid);
			version = c->version;
			put32bit(&ptr,version);
			lockedto = c->lockedto;
			put32bit(&ptr,lockedto);
			j++;
			if (j==CHUNKCNT) {
				if (fwrite(storebuff,1,CHUNKFSIZE*CHUNKCNT,fd)!=(size_t)(CHUNKFSIZE*CHUNKCNT)) {
					return;
				}
				j=0;
				ptr = storebuff;
			}
		}
	}
	memset(ptr,0,CHUNKFSIZE);
	j++;
	if (fwrite(storebuff,1,CHUNKFSIZE*j,fd)!=(size_t)(CHUNKFSIZE*j)) {
		return;
	}
}

void chunk_term(void) {
#ifndef METARESTORE
# ifdef USE_SLIST_BUCKETS
	slist_bucket *sb,*sbn;
# else
	slist *sl,*sln;
# endif
# if 0
# ifdef USE_FLIST_BUCKETS
	flist_bucket *fb,*fbn;
# else
	flist *fl,*fln;
# endif
# endif
# ifdef USE_CHUNK_BUCKETS
	chunk_bucket *cb,*cbn;
# endif
# if !defined(USE_SLIST_BUCKETS) || !defined(USE_FLIST_BUCKETS) || !defined(USE_CHUNK_BUCKETS)
	uint32_t i;
	chunk *ch,*chn;
# endif
#else
# ifdef USE_CHUNK_BUCKETS
	chunk_bucket *cb,*cbn;
# else
	uint32_t i;
	chunk *ch,*chn;
# endif
#endif

#ifndef METARESTORE
# ifdef USE_SLIST_BUCKETS
	for (sb = sbhead ; sb ; sb = sbn) {
		sbn = sb->next;
		free(sb);
	}
# else
	for (i=0 ; i<HASHSIZE ; i++) {
		for (ch = chunkhash[i] ; ch ; ch = ch->next) {
			for (sl = ch->slisthead ; sl ; sl = sln) {
				sln = sl->next;
				free(sl);
			}
		}
	}
# endif
#endif
#ifdef USE_CHUNK_BUCKETS
	for (cb = cbhead ; cb ; cb = cbn) {
		cbn = cb->next;
		delete cb;
	}
#else
	for (i=0 ; i<HASHSIZE ; i++) {
		for (ch = chunkhash[i] ; ch ; ch = chn) {
			chn = ch->next;
			delete ch;
		}
	}
#endif
}

void chunk_newfs(void) {
#ifndef METARESTORE
	chunk::count = 0;
#endif
	nextchunkid = 1;
}

#ifndef METARESTORE
void chunk_reload(void) {
	uint32_t oldMaxDelSoftLimit,oldMaxDelHardLimit;
	uint32_t repl;
	uint32_t looptime;

	ReplicationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT",300);
	ReplicationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600);


	oldMaxDelSoftLimit = MaxDelSoftLimit;
	oldMaxDelHardLimit = MaxDelHardLimit;

	MaxDelSoftLimit = cfg_getuint32("CHUNKS_SOFT_DEL_LIMIT",10);
	if (cfg_isdefined("CHUNKS_HARD_DEL_LIMIT")) {
		MaxDelHardLimit = cfg_getuint32("CHUNKS_HARD_DEL_LIMIT",25);
		if (MaxDelHardLimit<MaxDelSoftLimit) {
			MaxDelSoftLimit = MaxDelHardLimit;
			syslog(LOG_WARNING,"CHUNKS_SOFT_DEL_LIMIT is greater than CHUNKS_HARD_DEL_LIMIT - using CHUNKS_HARD_DEL_LIMIT for both");
		}
	} else {
		MaxDelHardLimit = 3 * MaxDelSoftLimit;
	}
	if (MaxDelSoftLimit==0) {
		MaxDelSoftLimit = oldMaxDelSoftLimit;
		MaxDelHardLimit = oldMaxDelHardLimit;
	}
	if (TmpMaxDelFrac<MaxDelSoftLimit) {
		TmpMaxDelFrac = MaxDelSoftLimit;
	}
	if (TmpMaxDelFrac>MaxDelHardLimit) {
		TmpMaxDelFrac = MaxDelHardLimit;
	}
	if (TmpMaxDel<MaxDelSoftLimit) {
		TmpMaxDel = MaxDelSoftLimit;
	}
	if (TmpMaxDel>MaxDelHardLimit) {
		TmpMaxDel = MaxDelHardLimit;
	}


	repl = cfg_getuint32("CHUNKS_WRITE_REP_LIMIT",2);
	if (repl>0) {
		MaxWriteRepl = repl;
	}


	repl = cfg_getuint32("CHUNKS_READ_REP_LIMIT",10);
	if (repl>0) {
		MaxReadRepl = repl;
	}

	if (cfg_isdefined("CHUNKS_LOOP_TIME")) {
		looptime = cfg_getuint32("CHUNKS_LOOP_TIME",300);
		if (looptime < MINLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_TIME value too low (%" PRIu32 ") increased to %u",looptime,MINLOOPTIME);
			looptime = MINLOOPTIME;
		}
		if (looptime > MAXLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_TIME value too high (%" PRIu32 ") decreased to %u",looptime,MAXLOOPTIME);
			looptime = MAXLOOPTIME;
		}
		HashSteps = 1+((HASHSIZE)/looptime);
		HashCPS = 0xFFFFFFFF;
	} else {
		looptime = cfg_getuint32("CHUNKS_LOOP_MIN_TIME",300);
		if (looptime < MINLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MIN_TIME value too low (%" PRIu32 ") increased to %u",looptime,MINLOOPTIME);
			looptime = MINLOOPTIME;
		}
		if (looptime > MAXLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MIN_TIME value too high (%" PRIu32 ") decreased to %u",looptime,MAXLOOPTIME);
			looptime = MAXLOOPTIME;
		}
		HashSteps = 1+((HASHSIZE)/looptime);
		HashCPS = cfg_getuint32("CHUNKS_LOOP_MAX_CPS",100000);
		if (HashCPS < MINCPS) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MAX_CPS value too low (%" PRIu32 ") increased to %u",HashCPS,MINCPS);
			HashCPS = MINCPS;
		}
		if (HashCPS > MAXCPS) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MAX_CPS value too high (%" PRIu32 ") decreased to %u",HashCPS,MAXCPS);
			HashCPS = MAXCPS;
		}
	}

	AcceptableDifference = cfg_getdouble("ACCEPTABLE_DIFFERENCE",0.1);
	if (AcceptableDifference<0.001) {
		AcceptableDifference = 0.001;
	}
	if (AcceptableDifference>10.0) {
		AcceptableDifference = 10.0;
	}
}
#endif

int chunk_strinit(void) {
#ifndef METARESTORE
	uint32_t looptime;

	ReplicationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT",300);
	ReplicationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600);
	MaxDelSoftLimit = cfg_getuint32("CHUNKS_SOFT_DEL_LIMIT",10);
	if (cfg_isdefined("CHUNKS_HARD_DEL_LIMIT")) {
		MaxDelHardLimit = cfg_getuint32("CHUNKS_HARD_DEL_LIMIT",25);
		if (MaxDelHardLimit<MaxDelSoftLimit) {
			MaxDelSoftLimit = MaxDelHardLimit;
			fprintf(stderr,"CHUNKS_SOFT_DEL_LIMIT is greater than CHUNKS_HARD_DEL_LIMIT - using CHUNKS_HARD_DEL_LIMIT for both\n");
		}
	} else {
		MaxDelHardLimit = 3 * MaxDelSoftLimit;
	}
	if (MaxDelSoftLimit==0) {
		fprintf(stderr,"delete limit is zero !!!\n");
		return -1;
	}
	TmpMaxDelFrac = MaxDelSoftLimit;
	TmpMaxDel = MaxDelSoftLimit;
	MaxWriteRepl = cfg_getuint32("CHUNKS_WRITE_REP_LIMIT",2);
	MaxReadRepl = cfg_getuint32("CHUNKS_READ_REP_LIMIT",10);
	if (MaxReadRepl==0) {
		fprintf(stderr,"read replication limit is zero !!!\n");
		return -1;
	}
	if (MaxWriteRepl==0) {
		fprintf(stderr,"write replication limit is zero !!!\n");
		return -1;
	}
	if (cfg_isdefined("CHUNKS_LOOP_TIME")) {
		fprintf(stderr,"Defining loop time by CHUNKS_LOOP_TIME option is deprecated - use CHUNKS_LOOP_MAX_CPS and CHUNKS_LOOP_MIN_TIME\n");
		looptime = cfg_getuint32("CHUNKS_LOOP_TIME",300);
		if (looptime < MINLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_TIME value too low (%" PRIu32 ") increased to %u\n",looptime,MINLOOPTIME);
			looptime = MINLOOPTIME;
		}
		if (looptime > MAXLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_TIME value too high (%" PRIu32 ") decreased to %u\n",looptime,MAXLOOPTIME);
			looptime = MAXLOOPTIME;
		}
		HashSteps = 1+((HASHSIZE)/looptime);
		HashCPS = 0xFFFFFFFF;
	} else {
		looptime = cfg_getuint32("CHUNKS_LOOP_MIN_TIME",300);
		if (looptime < MINLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_MIN_TIME value too low (%" PRIu32 ") increased to %u\n",looptime,MINLOOPTIME);
			looptime = MINLOOPTIME;
		}
		if (looptime > MAXLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_MIN_TIME value too high (%" PRIu32 ") decreased to %u\n",looptime,MAXLOOPTIME);
			looptime = MAXLOOPTIME;
		}
		HashSteps = 1+((HASHSIZE)/looptime);
		HashCPS = cfg_getuint32("CHUNKS_LOOP_MAX_CPS",100000);
		if (HashCPS < MINCPS) {
			fprintf(stderr,"CHUNKS_LOOP_MAX_CPS value too low (%" PRIu32 ") increased to %u\n",HashCPS,MINCPS);
			HashCPS = MINCPS;
		}
		if (HashCPS > MAXCPS) {
			fprintf(stderr,"CHUNKS_LOOP_MAX_CPS value too high (%" PRIu32 ") decreased to %u\n",HashCPS,MAXCPS);
			HashCPS = MAXCPS;
		}
	}
	AcceptableDifference = cfg_getdouble("ACCEPTABLE_DIFFERENCE",0.1);
	if (AcceptableDifference<0.001) {
		AcceptableDifference = 0.001;
	}
	if (AcceptableDifference>10.0) {
		AcceptableDifference = 10.0;
	}
#endif
	for (int i=0 ; i<HASHSIZE ; i++) {
		chunkhash[i]=NULL;
	}
#ifndef METARESTORE
	jobshpos = 0;
	jobsrebalancecount = 0;
	starttime = main_time();
	jobsnorepbefore = starttime+ReplicationsDelayInit;
	chunk_do_jobs(NULL,JOBS_INIT,0.0,0.0); // clear chunk loop internal data
	main_reloadregister(chunk_reload);
	main_timeregister(TIMEMODE_RUN_LATE,1,0,chunk_jobs_main);
#endif
	return 1;
}
