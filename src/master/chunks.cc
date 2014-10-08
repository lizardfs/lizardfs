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
#include <algorithm>

#include "common/chunks_availability_state.h"
#include "common/datapack.h"
#include "common/goal.h"
#include "common/hashfn.h"
#include "common/lizardfs_version.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/MFSCommunication.h"
#include "master/checksum.h"
#include "master/chunk_copies_calculator.h"
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
#  endif

#define MINLOOPTIME 1
#define MAXLOOPTIME 7200
#define MAXCPS 10000000
#define MINCPS 10000

#define HASHSIZE 0x100000
#define HASHPOS(chunkid) (((uint32_t)chunkid)&0xFFFFF)

#define CHECKSUMSEED 78765491511151883ULL

#ifndef METARESTORE


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

/* List of servers containing the chunk */
struct slist {
	void *ptr; // server data as matocsserventry
	uint32_t version;
	ChunkType chunkType;
	uint8_t valid;
//      uint8_t sectionid; - idea - Split machines into sctions. Try to place each copy of particular chunk in different section.
//      uint16_t machineid; - idea - If there are many different processes on the same physical computer then place there only one copy of chunk.
	slist *next;
	slist()
			: ptr(nullptr),
			  version(0),
			  chunkType(ChunkType::getStandardChunkType()),
			  valid(INVALID),
			  next(nullptr) {
	}

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

#ifndef METARESTORE
static std::vector<void*> zombieServersHandledInThisLoop;
static std::vector<void*> zombieServersToBeHandledInNextLoop;
#endif // METARESTORE

#define SLIST_BUCKET_SIZE 5000
struct slist_bucket {
	slist bucket[SLIST_BUCKET_SIZE];
	uint32_t firstfree;
	slist_bucket *next;
};

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
	uint32_t lockid;
	uint32_t lockedto;
private: // public/private sections are mixed here to make the struct as small as possible
	ChunkGoalCounters goalCounters_;
#ifndef METARESTORE
	uint8_t goalInStats_;
#endif
#ifndef METARESTORE
public:
	uint8_t needverincrease:1;
	uint8_t interrupted:1;
	uint8_t operation:4;
#endif
#ifndef METARESTORE
private:
	uint8_t allMissingParts_, regularMissingParts_;
	uint8_t allRedundantParts_, regularRedundantParts_;
	uint8_t allStandardCopies_, regularStandardCopies_;
	uint8_t allAvailabilityState_, regularAvailabilityState_;
#endif

public:
#ifndef METARESTORE
	static ChunksAvailabilityState allChunksAvailability, regularChunksAvailability;
	static ChunksReplicationState allChunksReplicationState, regularChunksReplicationState;
	static uint64_t count;
	static uint64_t allStandardChunkCopies[11][11], regularStandardChunkCopies[11][11];
#endif

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
		allMissingParts_ = regularMissingParts_ = 0;
		allRedundantParts_ = regularRedundantParts_ = 0;
		allStandardCopies_ = regularStandardCopies_ = 0;
		allAvailabilityState_ = regularAvailabilityState_ = ChunksAvailabilityState::kSafe;
		goalInStats_ = 0;
		addToStats();
		updateStats();
	}

	// This method should be called when a chunk is removed
	void freeStats() {
		count--;
		removeFromStats();
	}

	// Updates statistics of all chunks
	void updateStats() {
		removeFromStats();
		allStandardCopies_ = regularStandardCopies_ = 0;
		ChunkCopiesCalculator all(goal()), regular(goal());
		for (slist* s = slisthead; s != nullptr; s = s->next) {
			if (!s->is_valid()) {
				continue;
			}
			all.addPart(s->chunkType);
			if (s->chunkType.isStandardChunkType()) {
				allStandardCopies_++;
			}
			if (!s->is_todel()) {
				regular.addPart(s->chunkType);
				if (s->chunkType.isStandardChunkType()) {
					regularStandardCopies_++;
				}
			}
		}
		allAvailabilityState_ = all.getState();
		allMissingParts_ = std::min(200U, all.countPartsToRecover());
		allRedundantParts_ = std::min(200U, all.countPartsToRemove());
		regularAvailabilityState_ = regular.getState();
		regularMissingParts_ = std::min(200U, regular.countPartsToRecover());
		regularRedundantParts_ = std::min(200U, regular.countPartsToRemove());
		addToStats();
	}

	bool isSafe() const {
		return allAvailabilityState_ == ChunksAvailabilityState::kSafe;
	}

	bool isEndangered() const {
		return allAvailabilityState_ == ChunksAvailabilityState::kEndangered;
	}

	bool isLost() const {
		return allAvailabilityState_ == ChunksAvailabilityState::kLost;
	}

	bool needsReplication() const {
		return regularMissingParts_ > 0;
	}

	bool needsDeletion() const {
		return regularRedundantParts_ > 0;
	}

	uint8_t getStandardCopiesCount() const {
		return allStandardCopies_;
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

	slist* addCopyNoStatsUpdate(void *ptr, uint8_t valid, uint32_t version, ChunkType type) {
		slist *s = slist_malloc();
		s->ptr = ptr;
		s->valid = valid;
		s->version = version;
		s->chunkType = type;
		s->next = slisthead;
		slisthead = s;
		return s;
	}

	slist *addCopy(void *ptr, uint8_t valid, uint32_t version, ChunkType type) {
		slist *s = addCopyNoStatsUpdate(ptr, valid, version, type);
		updateStats();
		return s;
	}

	ChunkCopiesCalculator makeRegularCopiesCalculator() const {
		ChunkCopiesCalculator calculator(goal());
		for (const slist *s = slisthead; s != nullptr; s = s->next) {
			if (s->is_valid() && !s->is_todel()) {
				calculator.addPart(s->chunkType);
			}
		}
		return calculator;
	}

private:
	void removeFromStats() {
		ChunksAvailabilityState::State chunkState;

		chunkState = static_cast<ChunksAvailabilityState::State>(allAvailabilityState_);
		allChunksAvailability.removeChunk(goalInStats_, chunkState);
		allChunksReplicationState.removeChunk(goalInStats_, allMissingParts_, allRedundantParts_);

		chunkState = static_cast<ChunksAvailabilityState::State>(regularAvailabilityState_);
		regularChunksAvailability.removeChunk(goalInStats_, chunkState);
		regularChunksReplicationState.removeChunk(goalInStats_,
				regularMissingParts_, regularRedundantParts_);

		if (goalInStats_ == 0 || isOrdinaryGoal(goalInStats_)) {
			uint8_t limitedGoal = std::min<uint8_t>(10, goalInStats_);
			uint8_t limitedAll = std::min<uint8_t>(10, allStandardCopies_);
			uint8_t limitedRegular = std::min<uint8_t>(10, regularStandardCopies_);
			allStandardChunkCopies[limitedGoal][limitedAll]--;
			regularStandardChunkCopies[limitedGoal][limitedRegular]--;
		}
	}

	void addToStats() {
		goalInStats_ = goal();
		ChunksAvailabilityState::State chunkState;

		chunkState = static_cast<ChunksAvailabilityState::State>(allAvailabilityState_);
		allChunksAvailability.addChunk(goalInStats_, chunkState);
		allChunksReplicationState.addChunk(goalInStats_, allMissingParts_, allRedundantParts_);

		chunkState = static_cast<ChunksAvailabilityState::State>(regularAvailabilityState_);
		regularChunksAvailability.addChunk(goalInStats_, chunkState);
		regularChunksReplicationState.addChunk(goalInStats_,
				regularMissingParts_, regularRedundantParts_);

		if (goalInStats_ == 0 || isOrdinaryGoal(goalInStats_)) {
			uint8_t limitedGoal = std::min<uint8_t>(10, goalInStats_);
			uint8_t limitedAll = std::min<uint8_t>(10, allStandardCopies_);
			uint8_t limitedRegular = std::min<uint8_t>(10, regularStandardCopies_);
			allStandardChunkCopies[limitedGoal][limitedAll]++;
			regularStandardChunkCopies[limitedGoal][limitedRegular]++;
		}
	}
#endif
};

#ifndef METARESTORE
ChunksAvailabilityState chunk::allChunksAvailability, chunk::regularChunksAvailability;
ChunksReplicationState chunk::allChunksReplicationState, chunk::regularChunksReplicationState;
uint64_t chunk::count;
uint64_t chunk::allStandardChunkCopies[11][11];
uint64_t chunk::regularStandardChunkCopies[11][11];
#endif

#define CHUNK_BUCKET_SIZE 20000
struct chunk_bucket {
	chunk bucket[CHUNK_BUCKET_SIZE];
	uint32_t firstfree;
	chunk_bucket *next;
};

namespace {
struct ChunksMetadata {
#ifndef METARESTORE
	// server lists
	slist_bucket *sbhead;
	slist *slfreehead;
#endif

	// chunks
	chunk_bucket *cbhead;
	chunk *chfreehead;
	chunk *chunkhash[HASHSIZE];
	uint64_t lastchunkid;
	chunk* lastchunkptr;

	// other chunks metadata information
	uint64_t nextchunkid;
	uint64_t chunksChecksum;
	uint64_t chunksChecksumRecalculated;
	uint32_t checksumRecalculationPosition;

	ChunksMetadata() :
#ifndef METARESTORE
			sbhead{},
			slfreehead{},
#endif
			cbhead{},
			chfreehead{},
			chunkhash{},
			lastchunkid{},
			lastchunkptr{},
			nextchunkid{1},
			chunksChecksum{},
			chunksChecksumRecalculated{},
			checksumRecalculationPosition{0} {
	}

	~ChunksMetadata() {
#ifndef METARESTORE
		slist_bucket *sbn;
		for (slist_bucket *sb = sbhead; sb; sb = sbn) {
			sbn = sb->next;
			delete sb;
		}
#endif

		chunk_bucket *cbn;
		for (chunk_bucket *cb = cbhead; cb; cb = cbn) {
			cbn = cb->next;
			delete cb;
		}
	}
};
} // anonymous namespace

static ChunksMetadata* gChunksMetadata;

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

static uint32_t stats_deletions=0;
static uint32_t stats_replications=0;

void chunk_stats(uint32_t *del,uint32_t *repl) {
	*del = stats_deletions;
	*repl = stats_replications;
	stats_deletions = 0;
	stats_replications = 0;
}

#endif // ! METARESTORE

static uint64_t chunk_checksum(const chunk* c) {
	if (c == nullptr || c->fileCount() == 0) {
		// We treat chunks with fileCount=0 as non-existent, so that we don't have to notify shadow
		// masters when we remove them from our structures.
		return 0;
	}
	uint64_t checksum = 64517419147637ULL;
	hashCombine(checksum, c->chunkid, c->version, c->lockedto, c->lockid, c->goal(), c->fileCount());
	return checksum;
}

static void chunk_checksum_add_to_background(chunk* ch) {
	if (!ch) {
		return;
	}
	removeFromChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
	ch->checksum = chunk_checksum(ch);
	addToChecksum(gChunksMetadata->chunksChecksumRecalculated, ch->checksum);
	addToChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
}

static void chunk_update_checksum(chunk* ch) {
	if (!ch) {
		return;
	}
	if (HASHPOS(ch->chunkid) < gChunksMetadata->checksumRecalculationPosition) {
		removeFromChecksum(gChunksMetadata->chunksChecksumRecalculated, ch->checksum);
	}
	removeFromChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
	ch->checksum = chunk_checksum(ch);
	if (HASHPOS(ch->chunkid) < gChunksMetadata->checksumRecalculationPosition) {
		DEBUG_LOG("master.fs.checksum.changing_recalculated_chunk");
		addToChecksum(gChunksMetadata->chunksChecksumRecalculated, ch->checksum);
	} else {
		DEBUG_LOG("master.fs.checksum.changing_not_recalculated_chunk");
	}
	addToChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
}

/*!
 * \brief update chunks checksum in the background
 * \param limit for processed chunks per function call
 * \return info whether all chunks were updated or not.
 */

ChecksumRecalculationStatus chunks_update_checksum_a_bit(uint32_t speedLimit) {
	if (gChunksMetadata->checksumRecalculationPosition == 0) {
		gChunksMetadata->chunksChecksumRecalculated = CHECKSUMSEED;
	}
	uint32_t recalculated = 0;
	while (gChunksMetadata->checksumRecalculationPosition < HASHSIZE) {
		chunk* c;
		for (c = gChunksMetadata->chunkhash[gChunksMetadata->checksumRecalculationPosition]; c; c=c->next) {
			chunk_checksum_add_to_background(c);
			++recalculated;
		}
		++gChunksMetadata->checksumRecalculationPosition;
		if (recalculated >= speedLimit) {
			return ChecksumRecalculationStatus::kInProgress;
		}
	}
	// Recalculating chunks checksum finished
	gChunksMetadata->checksumRecalculationPosition = 0;
	if (gChunksMetadata->chunksChecksum != gChunksMetadata->chunksChecksumRecalculated) {
		syslog(LOG_WARNING,"Chunks metadata checksum mismatch found, replacing with a new value.");
		DEBUG_LOG("master.fs.checksum.mismatch");
		gChunksMetadata->chunksChecksum = gChunksMetadata->chunksChecksumRecalculated;
	}
	return ChecksumRecalculationStatus::kDone;
}

static void chunk_recalculate_checksum() {
	gChunksMetadata->chunksChecksum = CHECKSUMSEED;
	for (int i = 0; i < HASHSIZE; ++i) {
		for (chunk* ch = gChunksMetadata->chunkhash[i]; ch; ch = ch->next) {
			ch->checksum = chunk_checksum(ch);
			addToChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
		}
	}
}

uint64_t chunk_checksum(ChecksumMode mode) {
	uint64_t checksum = 46586918175221;
	addToChecksum(checksum, gChunksMetadata->nextchunkid);
	if (mode == ChecksumMode::kForceRecalculate) {
		chunk_recalculate_checksum();
	}
	addToChecksum(checksum, gChunksMetadata->chunksChecksum);
	return checksum;
}

#ifndef METARESTORE
static inline slist* slist_malloc() {
	slist_bucket *sb;
	slist *ret;
	if (gChunksMetadata->slfreehead) {
		ret = gChunksMetadata->slfreehead;
		gChunksMetadata->slfreehead = ret->next;
		return ret;
	}
	if (gChunksMetadata->sbhead==NULL || gChunksMetadata->sbhead->firstfree==SLIST_BUCKET_SIZE) {
		sb = new slist_bucket;
		passert(sb);
		sb->next = gChunksMetadata->sbhead;
		sb->firstfree = 0;
		gChunksMetadata->sbhead = sb;
	}
	ret = (gChunksMetadata->sbhead->bucket)+(gChunksMetadata->sbhead->firstfree);
	gChunksMetadata->sbhead->firstfree++;
	return ret;
}

static inline void slist_free(slist *p) {
	p->next = gChunksMetadata->slfreehead;
	gChunksMetadata->slfreehead = p;
}
#endif /* !METARESTORE */

static inline chunk* chunk_malloc() {
	chunk_bucket *cb;
	chunk *ret;
	if (gChunksMetadata->chfreehead) {
		ret = gChunksMetadata->chfreehead;
		gChunksMetadata->chfreehead = ret->next;
		return ret;
	}
	if (gChunksMetadata->cbhead==NULL || gChunksMetadata->cbhead->firstfree==CHUNK_BUCKET_SIZE) {
		cb = new chunk_bucket;
		cb->next = gChunksMetadata->cbhead;
		cb->firstfree = 0;
		gChunksMetadata->cbhead = cb;
	}
	ret = (gChunksMetadata->cbhead->bucket)+(gChunksMetadata->cbhead->firstfree);
	gChunksMetadata->cbhead->firstfree++;
	return ret;
}

#ifndef METARESTORE
static inline void chunk_free(chunk *p) {
	p->next = gChunksMetadata->chfreehead;
	gChunksMetadata->chfreehead = p;
}
#endif /* METARESTORE */

chunk* chunk_new(uint64_t chunkid, uint32_t chunkversion) {
	uint32_t chunkpos = HASHPOS(chunkid);
	chunk *newchunk;
	newchunk = chunk_malloc();
	newchunk->next = gChunksMetadata->chunkhash[chunkpos];
	gChunksMetadata->chunkhash[chunkpos] = newchunk;
	newchunk->chunkid = chunkid;
	newchunk->version = chunkversion;
	newchunk->lockid = 0;
	newchunk->lockedto = 0;
#ifndef METARESTORE
	newchunk->needverincrease = 1;
	newchunk->interrupted = 0;
	newchunk->operation = NONE;
	newchunk->slisthead = NULL;
	newchunk->initStats();
#endif
	gChunksMetadata->lastchunkid = chunkid;
	gChunksMetadata->lastchunkptr = newchunk;
	newchunk->checksum = 0;
	chunk_update_checksum(newchunk);
	return newchunk;
}

#ifndef METARESTORE
void chunk_emergency_increase_version(chunk *c) {
	if (c->isLost()) { // should always be false !!!
		syslog(LOG_ERR, "chunk_emergency_increase_version called on a lost chunk");
		matoclserv_chunk_status(c->chunkid, ERROR_CHUNKLOST);
		c->operation = NONE;
		return;
	}
	uint32_t i = 0;
	for (slist *s=c->slisthead ;s ; s=s->next) {
		if (s->is_valid()) {
			if (!s->is_busy()) {
				s->mark_busy();
			}
			s->version = c->version+1;
			matocsserv_send_setchunkversion(s->ptr,c->chunkid,c->version+1,c->version,
					s->chunkType);
			i++;
		}
	}
	if (i>0) { // should always be true !!!
		c->interrupted = 0;
		c->operation = SET_VERSION;
		c->version++;
		chunk_update_checksum(c);
		fs_incversion(c->chunkid);
	} else {
		syslog(LOG_ERR, "chunk %016" PRIX64 " lost in emergency increase version", c->chunkid);
		matoclserv_chunk_status(c->chunkid, ERROR_CHUNKLOST);
		c->operation = NONE;
	}
}

bool chunk_server_is_disconnected(void* ptr) {
	for (auto zombies : {&zombieServersHandledInThisLoop, &zombieServersToBeHandledInNextLoop}) {
		if (std::find(zombies->begin(), zombies->end(), ptr) != zombies->end()) {
			return true;
		}
	}
	return false;
}

void chunk_handle_disconnected_copies(chunk* c) {
	slist *s, **st;
	st = &(c->slisthead);
	bool lostCopyFound = false;
	while (*st) {
		s = *st;
		if (chunk_server_is_disconnected(s->ptr)) {
			c->unlinkCopy(s, st);
			c->needverincrease=1;
			lostCopyFound = true;
		} else {
			st = &(s->next);
		}
	}
	if (lostCopyFound && c->operation!=NONE) {
		bool any_copy_busy = false;
		for (s=c->slisthead ; s ; s=s->next) {
			any_copy_busy |= s->is_busy();
		}
		if (any_copy_busy) {
			c->interrupted = 1;
		} else {
			if (!c->isLost()) {
				chunk_emergency_increase_version(c);
			} else {
				matoclserv_chunk_status(c->chunkid,ERROR_NOTDONE);
				c->operation=NONE;
			}
		}
	}
}
#endif

chunk* chunk_find(uint64_t chunkid) {
	uint32_t chunkpos = HASHPOS(chunkid);
	chunk *chunkit;
	if (gChunksMetadata->lastchunkid==chunkid) {
		return gChunksMetadata->lastchunkptr;
	}
	for (chunkit = gChunksMetadata->chunkhash[chunkpos] ; chunkit ; chunkit = chunkit->next) {
		if (chunkit->chunkid == chunkid) {
			gChunksMetadata->lastchunkid = chunkid;
			gChunksMetadata->lastchunkptr = chunkit;
#ifndef METARESTORE
			chunk_handle_disconnected_copies(chunkit);
#endif // METARESTORE
			return chunkit;
		}
	}
	return NULL;
}

#ifndef METARESTORE
void chunk_delete(chunk* c) {
	if (gChunksMetadata->lastchunkptr==c) {
		gChunksMetadata->lastchunkid=0;
		gChunksMetadata->lastchunkptr=NULL;
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
			ag += chunk::allStandardChunkCopies[j][i];
			rg += chunk::regularStandardChunkCopies[j][i];
		}
		*allcopies += ag*i;
		*regularvalidcopies += rg*i;
	}
}

uint32_t chunk_get_missing_count(void) {
	uint32_t res = 0;
	for (int goal = kMinOrdinaryGoal; goal <= kMaxOrdinaryGoal; ++goal) {
		res += chunk::allChunksAvailability.lostChunks(goal);
	}
	for (int level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
		res += chunk::allChunksAvailability.lostChunks(xorLevelToGoal(level));
	}
	return res;
}

void chunk_store_chunkcounters(uint8_t *buff, uint8_t matrixid) {
	uint8_t i,j;
	if (matrixid==0) {
		for (i=0 ; i<=10 ; i++) {
			for (j=0 ; j<=10 ; j++) {
				put32bit(&buff, chunk::allStandardChunkCopies[i][j]);
			}
		}
	} else if (matrixid==1) {
		for (i=0 ; i<=10 ; i++) {
			for (j=0 ; j<=10 ; j++) {
				put32bit(&buff, chunk::regularStandardChunkCopies[i][j]);
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

int chunk_can_unlock(uint64_t chunkid, uint32_t lockid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (lockid == 0) {
		// lockid == 0 -> force unlock
		return STATUS_OK;
	}
	// We will let client unlock the chunk even if c->lockedto < main_time()
	// if he provides lockId that was used to lock the chunk -- this means that nobody
	// else used this chunk since it was locked (operations like truncate or replicate
	// would remove such a stale lock before modifying the chunk)
	if (c->lockid == lockid) {
		return STATUS_OK;
	} else if (c->lockedto == 0) {
		return ERROR_NOTLOCKED;
	} else {
		return ERROR_WRONGLOCKID;
	}
}

int chunk_unlock(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	// Don't remove lockid to safely accept retransmission of FUSE_CHUNK_UNLOCK message
	c->lockedto = 0;
	chunk_update_checksum(c);
	return STATUS_OK;
}

#ifndef METARESTORE

bool chunk_has_only_invalid_copies(uint64_t chunkid) {
	if (chunkid == 0) {
		return false;
	}
	chunk *c = chunk_find(chunkid);
	if (c == NULL || !c->isLost()) {
		return false;
	}
	// Chunk is lost, so it can only have INVALID or DEL copies.
	// Return true it there is at least one INVALID.
	for (slist *s = c->slisthead; s != nullptr; s = s->next) {
		if (s->valid == INVALID) {
			return true;
		}
	}
	return false;
}

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies) {
	chunk *c;
	*vcopies = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->isLost()) {
		*vcopies = 0;
	} else if (c->isEndangered()) {
		*vcopies = 1;
	} else {
		// Safe chunk
		*vcopies = std::max<uint8_t>(2, c->getStandardCopiesCount());
	}
	return STATUS_OK;
}

uint8_t chunk_multi_modify(uint64_t ochunkid, uint32_t *lockid, uint8_t goal,
		bool usedummylockid, bool quota_exceeded, uint8_t *opflag, uint64_t *nchunkid) {
	chunk *c = NULL;
	if (ochunkid == 0) { // new chunk
		if (quota_exceeded) {
			return ERROR_QUOTA;
		}
		auto serversWithChunkTypes = matocsserv_getservers_for_new_chunk(goal);
		if (serversWithChunkTypes.empty()) {
			uint16_t uscount,tscount;
			double minusage,maxusage;
			matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);
			if ((uscount > 0) && (main_time() > (starttime+600))) { // if there are chunkservers and it's at least one minute after start then it means that there is no space left
				return ERROR_NOSPACE;
			} else {
				return ERROR_NOCHUNKSERVERS;
			}
		}
		c = chunk_new(gChunksMetadata->nextchunkid++, 1);
		c->interrupted = 0;
		c->operation = CREATE;
		chunk_add_file_int(c,goal);
		for (uint32_t i = 0; i < serversWithChunkTypes.size(); ++i) {
			slist *s = c->addCopyNoStatsUpdate(serversWithChunkTypes[i].first, BUSY,
					c->version, serversWithChunkTypes[i].second);
			matocsserv_send_createchunk(s->ptr, c->chunkid, s->chunkType, c->version);
		}
		c->updateStats();
		*opflag=1;
		*nchunkid = c->chunkid;
	} else {
		chunk *oc = chunk_find(ochunkid);
		if (oc==NULL) {
			return ERROR_NOCHUNK;
		}
		if (*lockid != 0 && *lockid != oc->lockid) {
			if (oc->lockid == 0 || oc->lockedto == 0) {
				// Lock was removed by some chunk operation or by a different client
				return ERROR_NOTLOCKED;
			} else {
				return ERROR_WRONGLOCKID;
			}
		}
		if (*lockid == 0 && oc->isLocked()) {
			return ERROR_LOCKED;
		}
		if (oc->isLost()) {
			return ERROR_CHUNKLOST;
		}

		if (oc->fileCount() == 1) { // refcount==1
			*nchunkid = ochunkid;
			c = oc;
			if (c->operation!=NONE) {
				return ERROR_CHUNKBUSY;
			}
			if (c->needverincrease) {
				uint32_t i = 0;
				for (slist *s=c->slisthead ;s ; s=s->next) {
					if (s->is_valid()) {
						if (!s->is_busy()) {
							s->mark_busy();
						}
						s->version = c->version+1;
						matocsserv_send_setchunkversion(s->ptr, ochunkid, c->version+1, c->version,
								s->chunkType);
						i++;
					}
				}
				if (i>0) {
					c->interrupted = 0;
					c->operation = SET_VERSION;
					c->version++;
					*opflag=1;
				} else {
					// This should never happen - we verified this using ChunkCopiesCalculator
					return ERROR_CHUNKLOST;
				}
			} else {
				*opflag=0;
			}
		} else {
			if (oc->fileCount() == 0) { // it's serious structure error
				syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016" PRIX64 ")",ochunkid);
				return ERROR_CHUNKLOST; // ERROR_STRUCTURE
			}
			if (quota_exceeded) {
				return ERROR_QUOTA;
			}
			uint32_t i = 0;
			for (slist *os=oc->slisthead ;os ; os=os->next) {
				if (os->is_valid()) {
					if (c==NULL) {
						c = chunk_new(gChunksMetadata->nextchunkid++, 1);
						c->interrupted = 0;
						c->operation = DUPLICATE;
						chunk_delete_file_int(oc,goal);
						chunk_add_file_int(c,goal);
					}
					slist *s = c->addCopyNoStatsUpdate(os->ptr, BUSY, c->version, os->chunkType);
					matocsserv_send_duplicatechunk(s->ptr, c->chunkid, c->version, os->chunkType,
							oc->chunkid, oc->version);
					i++;
				}
			}
			if (c!=NULL) {
				c->updateStats();
			}
			if (i>0) {
				*nchunkid = c->chunkid;
				*opflag=1;
			} else {
				return ERROR_CHUNKLOST;
			}
		}
	}

	c->lockedto = main_time() + LOCKTIMEOUT;
	if (*lockid == 0) {
		if (usedummylockid) {
			*lockid = 1;
		} else {
			*lockid = 2 + rndu32_ranged(0xFFFFFFF0); // some random number greater than 1
		}
	}
	c->lockid = *lockid;
	chunk_update_checksum(c);
	return STATUS_OK;
}

uint8_t chunk_multi_truncate(uint64_t ochunkid, uint32_t lockid, uint32_t length,
		uint8_t goal, bool denyTruncatingParityParts, bool quota_exceeded, uint64_t *nchunkid) {
	uint32_t i;
	chunk *oc,*c;

	c=NULL;
	oc = chunk_find(ochunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->isLocked() && (lockid == 0 || lockid != oc->lockid)) {
		return ERROR_LOCKED;
	}
	if (denyTruncatingParityParts) {
		for (slist *s = oc->slisthead; s; s = s->next) {
			if (s->chunkType.isXorChunkType() && s->chunkType.isXorParity()) {
				return ERROR_NOTPOSSIBLE;
			}
		}
	}
	if (oc->fileCount() == 1) { // refcount==1
		*nchunkid = ochunkid;
		c = oc;
		if (c->operation!=NONE) {
			return ERROR_CHUNKBUSY;
		}
		i=0;
		for (slist *s=c->slisthead ; s ; s=s->next) {
			if (s->is_valid()) {
				if (!s->is_busy()) {
					s->mark_busy();
				}
				s->version = c->version+1;
				uint32_t chunkTypeLength =
						ChunkType::chunkLengthToChunkTypeLength(s->chunkType, length);
				matocsserv_send_truncatechunk(s->ptr, ochunkid, s->chunkType, chunkTypeLength,
						c->version + 1, c->version);
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
	} else {
		if (oc->fileCount() == 0) { // it's serious structure error
			syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016" PRIX64 ")",ochunkid);
			return ERROR_CHUNKLOST; // ERROR_STRUCTURE
		}
		if (quota_exceeded) {
			return ERROR_QUOTA;
		}
		i=0;
		for (slist *os=oc->slisthead ; os ; os=os->next) {
			if (os->is_valid()) {
				if (c==NULL) {
					c = chunk_new(gChunksMetadata->nextchunkid++, 1);
					c->interrupted = 0;
					c->operation = DUPTRUNC;
					chunk_delete_file_int(oc,goal);
					chunk_add_file_int(c,goal);
				}
				slist *s = c->addCopyNoStatsUpdate(os->ptr, BUSY, c->version, os->chunkType);
				matocsserv_send_duptruncchunk(s->ptr, c->chunkid, c->version,
						s->chunkType, oc->chunkid, oc->version,
						ChunkType::chunkLengthToChunkTypeLength(s->chunkType, length));
				i++;
			}
		}
		if (c!=NULL) {
			c->updateStats();
		}
		if (i>0) {
			*nchunkid = c->chunkid;
		} else {
			return ERROR_CHUNKLOST;
		}
	}

	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
	c->lockid = lockid;
	chunk_update_checksum(c);
	return STATUS_OK;
}
#endif // ! METARESTORE

uint8_t chunk_apply_modification(uint32_t ts, uint64_t oldChunkId, uint32_t lockid, uint8_t goal,
		bool doIncreaseVersion, uint64_t *newChunkId) {
	chunk *c;
	if (oldChunkId == 0) { // new chunk
		c = chunk_new(gChunksMetadata->nextchunkid++, 1);
		chunk_add_file_int(c, goal);
	} else {
		chunk *oc = chunk_find(oldChunkId);
		if (oc == NULL) {
			return ERROR_NOCHUNK;
		}
		if (oc->fileCount() == 0) { // refcount == 0
			syslog(LOG_WARNING,
					"serious structure inconsistency: (chunkid:%016" PRIX64 ")", oldChunkId);
			return ERROR_CHUNKLOST; // ERROR_STRUCTURE
		} else if (oc->fileCount() == 1) { // refcount == 1
			c = oc;
			if (doIncreaseVersion) {
				c->version++;
			}
		} else {
			c = chunk_new(gChunksMetadata->nextchunkid++, 1);
			chunk_delete_file_int(oc, goal);
			chunk_add_file_int(c, goal);
		}
	}
	c->lockedto = ts + LOCKTIMEOUT;
	c->lockid = lockid;
	chunk_update_checksum(c);
	*newChunkId = c->chunkid;
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
#endif

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

uint8_t chunk_set_next_chunkid(uint64_t nextChunkIdToBeSet) {
	if (nextChunkIdToBeSet >= gChunksMetadata->nextchunkid) {
		gChunksMetadata->nextchunkid = nextChunkIdToBeSet;
		return STATUS_OK;
	} else {
		syslog(LOG_WARNING,"was asked to increase the next chunk id to %" PRIu64 ", but it was"
				"already set to a bigger value %" PRIu64 ". Ignoring.",
				nextChunkIdToBeSet, gChunksMetadata->nextchunkid);
		return ERROR_MISMATCH;
	}
}

#ifndef METARESTORE

const ChunksReplicationState& chunk_get_replication_state(bool regularChunksOnly) {
	return regularChunksOnly ?
			chunk::regularChunksReplicationState :
			chunk::allChunksReplicationState;
}

const ChunksAvailabilityState& chunk_get_availability_state(bool regularChunksOnly) {
	return regularChunksOnly ?
			chunk::regularChunksAvailability :
			chunk::allChunksAvailability;
}

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

struct ChunkLocation {
	ChunkLocation() : chunkType(ChunkType::getStandardChunkType()),
			distance(0), random(0) {
	}
	NetworkAddress address;
	ChunkType chunkType;
	uint32_t distance;
	uint32_t random;
	bool operator<(const ChunkLocation& other) const {
		if (distance < other.distance) {
			return true;
		} else if (distance > other.distance) {
			return false;
		} else {
			return random < other.random;
		}
	}
};

int chunk_getversionandlocations(uint64_t chunkid, uint32_t currentIp, uint32_t& version,
		uint32_t maxNumberOfChunkCopies, std::vector<ChunkTypeWithAddress>& serversList) {
	chunk *c;
	slist *s;
	uint8_t cnt;

	sassert(serversList.empty());
	c = chunk_find(chunkid);

	if (c == NULL) {
		return ERROR_NOCHUNK;
	}
	version = c->version;
	cnt = 0;
	std::vector<ChunkLocation> chunkLocation;
	ChunkLocation chunkserverLocation;
	for (s = c->slisthead; s; s = s->next) {
		if (s->is_valid()) {
			if (cnt < maxNumberOfChunkCopies && matocsserv_getlocation(s->ptr,
					&(chunkserverLocation.address.ip),
					&(chunkserverLocation.address.port)) == 0) {
				chunkserverLocation.distance =
						topology_distance(chunkserverLocation.address.ip, currentIp);
						// in the future prepare more sophisticated distance function
				chunkserverLocation.random = rndu32();
				chunkserverLocation.chunkType = s->chunkType;
				chunkLocation.push_back(chunkserverLocation);
				cnt++;
			}
		}
	}
	std::sort(chunkLocation.begin(), chunkLocation.end());
	for (uint i = 0; i < chunkLocation.size(); ++i) {
		const ChunkLocation& loc = chunkLocation[i];
		serversList.push_back(ChunkTypeWithAddress(loc.address, loc.chunkType));
	}
	return STATUS_OK;
}

void chunk_server_has_chunk(void *ptr, uint64_t chunkid, uint32_t version, ChunkType chunkType) {
	chunk *c;
	slist *s;
	const uint32_t new_version = version & 0x7FFFFFFF;
	const bool todel = version & 0x80000000;
	c = chunk_find(chunkid);
	if (c==NULL) {
		// chunkserver has nonexistent chunk, so create it for future deletion
		if (chunkid>=gChunksMetadata->nextchunkid) {
			fs_set_nextchunkid(FsContext::getForMaster(main_time()), chunkid + 1);
		}
		c = chunk_new(chunkid, new_version);
		c->lockedto = (uint32_t)main_time()+UNUSED_DELETE_TIMEOUT;
		c->lockid = 0;
		chunk_update_checksum(c);
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr == ptr && s->chunkType == chunkType) {
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
	const uint8_t state = (new_version == c->version) ? (todel ? TDVALID : VALID) : INVALID;
	c->addCopy(ptr, state, new_version, chunkType);
}

void chunk_damaged(void *ptr,uint64_t chunkid) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		// syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016" PRIX64 "), so create it for future deletion",chunkid);
		if (chunkid>=gChunksMetadata->nextchunkid) {
			gChunksMetadata->nextchunkid=chunkid+1;
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
	c->addCopy(ptr, INVALID, 0, ChunkType::getStandardChunkType());
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
	zombieServersToBeHandledInNextLoop.push_back(ptr);
	if (zombieServersHandledInThisLoop.empty()) {
		std::swap(zombieServersToBeHandledInNextLoop, zombieServersHandledInThisLoop);
	}
	main_make_next_poll_nonblocking();
	fs_cs_disconnected();
	gChunksMetadata->lastchunkid = 0;
	gChunksMetadata->lastchunkptr = NULL;
}

/*
 * A function that is called in every main loop iteration, that cleans chunk structs
 */
void chunk_clean_zombie_servers_a_bit() {
	static uint32_t currentPosition = 0;
	if (zombieServersHandledInThisLoop.empty()) {
		return;
	}
	for (auto i = 0; i < 100 ; ++i) {
		if (currentPosition < HASHSIZE) {
			chunk* c;
			for (c=gChunksMetadata->chunkhash[currentPosition] ; c ; c=c->next) {
				chunk_handle_disconnected_copies(c);
			}
			++currentPosition;
		} else {
			for (auto& server : zombieServersHandledInThisLoop) {
				matocsserv_remove_server(server);
			}
			zombieServersHandledInThisLoop.clear();
			std::swap(zombieServersHandledInThisLoop, zombieServersToBeHandledInNextLoop);
			currentPosition = 0;
			break;
		}
	}
	main_make_next_poll_nonblocking();
}

int chunk_canexit(void) {
	if (zombieServersHandledInThisLoop.size() + zombieServersToBeHandledInNextLoop.size() > 0) {
		return 0;
	}
	return 1;
}

void chunk_got_delete_status(void *ptr, uint64_t chunkId, ChunkType chunkType, uint8_t status) {
	chunk *c;
	slist *s,**st;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (s->ptr == ptr && s->chunkType == chunkType) {
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

void chunk_got_replicate_status(void *ptr, uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType, uint8_t status) {
	slist *s;
	chunk *c = chunk_find(chunkId);
	if (c == NULL || status != 0) {
		return;
	}

	for (s = c->slisthead; s; s = s->next) {
		if (s->chunkType == chunkType && s->ptr == ptr) {
			syslog(LOG_WARNING,
					"got replication status from server which had had that chunk before (chunk:%016"
					PRIX64 "_%08" PRIX32 ")", chunkId, chunkVersion);
			if (s->valid == VALID && chunkVersion != c->version) {
				s->version = chunkVersion;
				c->markCopyAsHavingWrongVersion(s);
			}
			return;
		}
	}
	const uint8_t state = (c->isLocked() || chunkVersion != c->version) ? INVALID : VALID;
	c->addCopy(ptr, state, chunkVersion, chunkType);
}

void chunk_operation_status(chunk *c, ChunkType chunkType, uint8_t status,void *ptr) {
	slist *s;
	bool any_copy_busy = false;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr == ptr && s->chunkType == chunkType) {
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
	}
	if (!any_copy_busy) {
		if (!c->isLost()) {
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
	chunk_operation_status(c, ChunkType::getStandardChunkType(), status, ptr);
}

void chunk_got_create_status(void *ptr,uint64_t chunkId, ChunkType chunkType, uint8_t status) {
	chunk *c;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_duplicate_status(void *ptr, uint64_t chunkId, ChunkType chunkType, uint8_t status) {
	chunk *c;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_setversion_status(void *ptr, uint64_t chunkId, ChunkType chunkType, uint8_t status) {
	chunk *c;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_truncate_status(void *ptr, uint64_t chunkid, ChunkType chunkType, uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_duptrunc_status(void *ptr, uint64_t chunkId, ChunkType chunkType, uint8_t status) {
	chunk *c;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
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

class ChunkWorker {
public:
	ChunkWorker();
	void doEveryLoopTasks();
	void doEverySecondTasks();
	void doChunkJobs(chunk *c, uint16_t serverCount, double minUsage, double maxUsage);

private:
	bool tryReplication(chunk *c, ChunkType type, void *destinationServer);

	uint16_t serverCount_;
	loop_info inforec_;
	uint32_t deleteNotDone_;
	uint32_t deleteDone_;
	uint32_t prevToDeleteCount_;
	uint32_t deleteLoopCount_;
};

ChunkWorker::ChunkWorker()
		: serverCount_(0),
		  deleteNotDone_(0),
		  deleteDone_(0),
		  prevToDeleteCount_(0),
		  deleteLoopCount_(0) {
	memset(&inforec_,0,sizeof(loop_info));
}

void ChunkWorker::doEveryLoopTasks() {
	deleteLoopCount_++;
	if (deleteLoopCount_ >= 16) {
		uint32_t toDeleteCount = deleteDone_ + deleteNotDone_;
		deleteLoopCount_ = 0;
		if ((deleteNotDone_ > deleteDone_) && (toDeleteCount > prevToDeleteCount_)) {
			TmpMaxDelFrac *= 1.5;
			if (TmpMaxDelFrac>MaxDelHardLimit) {
				syslog(LOG_NOTICE,"DEL_LIMIT hard limit (%" PRIu32 " per server) reached",MaxDelHardLimit);
				TmpMaxDelFrac=MaxDelHardLimit;
			}
			TmpMaxDel = TmpMaxDelFrac;
			syslog(LOG_NOTICE,"DEL_LIMIT temporary increased to: %" PRIu32 " per server",TmpMaxDel);
		}
		if ((toDeleteCount < prevToDeleteCount_) && (TmpMaxDelFrac > MaxDelSoftLimit)) {
			TmpMaxDelFrac /= 1.5;
			if (TmpMaxDelFrac<MaxDelSoftLimit) {
				syslog(LOG_NOTICE,"DEL_LIMIT back to soft limit (%" PRIu32 " per server)",MaxDelSoftLimit);
				TmpMaxDelFrac = MaxDelSoftLimit;
			}
			TmpMaxDel = TmpMaxDelFrac;
			syslog(LOG_NOTICE,"DEL_LIMIT decreased back to: %" PRIu32 " per server",TmpMaxDel);
		}
		prevToDeleteCount_ = toDeleteCount;
		deleteNotDone_ = 0;
		deleteDone_ = 0;
	}
	chunksinfo = inforec_;
	memset(&inforec_,0,sizeof(inforec_));
	chunksinfo_loopstart = chunksinfo_loopend;
	chunksinfo_loopend = main_time();
}

void ChunkWorker::doEverySecondTasks() {
	serverCount_ = 0;
}

static bool chunkPresentOnServer(chunk *c, void *server) {
	for (slist *s = c->slisthead ; s ; s = s->next) {
		if (s->ptr == server) {
			return true;
		}
	}
	return false;
}

static void* getServerForReplication(chunk *c, ChunkType chunkTypeToRecover) {
	// get list of chunkservers which can be written to
	std::vector<void*> possibleDestinations;
	matocsserv_getservers_lessrepl(possibleDestinations, MaxWriteRepl);
	uint32_t minServerVersion = 0;
	if (!chunkTypeToRecover.isStandardChunkType()) {
		minServerVersion = lizardfsVersion(1, 6, 28);
	}
	void *destination = nullptr;
	for (void* server : possibleDestinations) {
		if (matocsserv_get_version(server) < minServerVersion) {
			continue;
		}
		destination = server;
		for (slist *s = c->slisthead; s; s = s->next) {
			if (s->ptr == server &&
					s->chunkType.getStripeSize() == chunkTypeToRecover.getStripeSize()) {
				// server can't have any chunk of the same goal
				destination = nullptr;
				break;
			}
		}
		if (destination) {
			break;
		}
	}
	return destination;
}

bool ChunkWorker::tryReplication(chunk *c, ChunkType chunkTypeToRecover, void *destinationServer) {
	// NOTE: we don't allow replicating xor chunks from pre-1.6.28 chunkservers
	const uint32_t newServerVersion = lizardfsVersion(1, 6, 28);
	std::vector<void*> standardSources;
	std::vector<void*> newServerSources;
	ChunkCopiesCalculator newSourcesCalculator(c->goal());

	for (slist *s = c->slisthead ; s ; s = s->next) {
		if (s->is_valid() && !s->is_busy()) {
			if (matocsserv_get_version(s->ptr) >= newServerVersion) {
				newServerSources.push_back(s->ptr);
				newSourcesCalculator.addPart(s->chunkType);
			}
			if (s->chunkType.isStandardChunkType()) {
				standardSources.push_back(s->ptr);
			}
		}
	}

	if (newSourcesCalculator.isRecoveryPossible() &&
			matocsserv_get_version(destinationServer) >= newServerVersion) {
		// new replication possible - use it
		matocsserv_send_liz_replicatechunk(destinationServer, c->chunkid, c->version,
				chunkTypeToRecover, newServerSources,
				newSourcesCalculator.availableParts());
	} else if (chunkTypeToRecover.isStandardChunkType() && !standardSources.empty()) {
		// fall back to legacy replication
		matocsserv_send_replicatechunk(destinationServer, c->chunkid, c->version,
				standardSources[rndu32_ranged(standardSources.size())]);
	} else {
		// no replication possible
		return false;
	}
	stats_replications++;
	c->needverincrease = 1;
	return true;
}

void ChunkWorker::doChunkJobs(chunk *c, uint16_t serverCount, double minUsage, double maxUsage) {
	slist *s;
	static void* ptrs[65535];
	static uint32_t min,max;

	// step 0. Update chunk's statistics
	// Just in case if somewhere is a bug and updateStats was not called
	c->updateStats();

	// step 1. calculate number of valid and invalid copies
	uint32_t vc, tdc, ivc, bc, tdb, dc;
	vc = tdc = ivc = bc = tdb = dc = 0;
	for (s = c->slisthead ; s ; s = s->next) {
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
				matocsserv_send_deletechunk(s->ptr, c->chunkid, 0, s->chunkType);
				inforec_.done.del_invalid++;
				deleteDone_++;
				dc++;
				ivc--;
			}
		} else {
			if (s->valid==INVALID) {
				inforec_.notdone.del_invalid++;
				deleteNotDone_++;
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
					matocsserv_send_deletechunk(s->ptr, c->chunkid, c->version, s->chunkType);
					inforec_.done.del_unused++;
					deleteDone_++;
				}
			} else {
				if (s->valid==VALID || s->valid==TDVALID) {
					inforec_.notdone.del_unused++;
					deleteNotDone_++;
				}
			}
		}
		return ;
	}

	// step 7a. if chunk needs replication, do it before removing any copies
	if (c->needsReplication()) {
		std::vector<ChunkType> toRecover = c->makeRegularCopiesCalculator().getPartsToRecover();
		if (jobsnorepbefore >= main_time() || c->isLost() || toRecover.empty()) {
			inforec_.notdone.copy_undergoal++;
			return;
		}
		const ChunkType chunkTypeToRecover = toRecover.front();
		void* destination = getServerForReplication(c, chunkTypeToRecover);
		if (destination == nullptr) {
			inforec_.notdone.copy_undergoal++;
			return;
		}
		if (tryReplication(c, chunkTypeToRecover, destination)) {
			inforec_.done.copy_undergoal++;
		} else {
			inforec_.notdone.copy_undergoal++;
		}
		return;
	}

	// step 7b. if chunk has too many copies then delete some of them
	if (c->needsDeletion()) {
		std::vector<ChunkType> toRemove = c->makeRegularCopiesCalculator().getPartsToRemove();
		if (serverCount_ == 0) {
			serverCount_ = matocsserv_getservers_ordered(ptrs,AcceptableDifference/2.0,&min,&max);
		}
		const uint32_t overgoalCopies = toRemove.size();
		uint32_t copiesRemoved = 0;
		for (uint32_t i = 0; i < serverCount_ && !toRemove.empty(); ++i) {
			for (s = c->slisthead; s; s = s->next) {
				if (s->ptr != ptrs[serverCount_ - 1 - i] || s->valid != VALID) {
					continue;
				}
				if (matocsserv_deletion_counter(s->ptr) >= TmpMaxDel) {
					break;
				}

				auto it = std::find(toRemove.begin(), toRemove.end(), s->chunkType);
				if (it == toRemove.end()) {
					continue;
				}
				c->deleteCopy(s);
				c->needverincrease=1;
				stats_deletions++;
				matocsserv_send_deletechunk(s->ptr, c->chunkid, 0, s->chunkType);
				toRemove.erase(it);
				copiesRemoved++;
				vc--;
				dc++;
			}
		}
		inforec_.done.del_overgoal += copiesRemoved;
		deleteDone_ += copiesRemoved;
		inforec_.notdone.del_overgoal += (overgoalCopies - copiesRemoved);
		deleteNotDone_ += (overgoalCopies - copiesRemoved);
		return;
	}

	// step 7c. if chunk has one copy on each server and some of them have status TODEL then delete one of it
	bool hasXorCopies = false;
	for (s=c->slisthead ; s ; s=s->next) {
		if (!s->chunkType.isStandardChunkType()) {
			hasXorCopies = true;
		}
	}
	if (isOrdinaryGoal(c->goal())
			&& !hasXorCopies
			&& vc + tdc >= serverCount
			&& vc < c->goal()
			&& tdc > 0
			&& vc + tdc > 1) {
		uint8_t prevdone;
		prevdone = 0;
		for (s=c->slisthead ; s && prevdone==0 ; s=s->next) {
			if (s->valid==TDVALID) {
				if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
					c->deleteCopy(s);
					c->needverincrease=1;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr, c->chunkid, 0, s->chunkType);
					inforec_.done.del_diskclean++;
					tdc--;
					dc++;
					prevdone = 1;
				} else {
					inforec_.notdone.del_diskclean++;
				}
			}
		}
		return;
	}

	if (chunksinfo.notdone.copy_undergoal > 0 && chunksinfo.done.copy_undergoal > 0) {
		return;
	}

	// step 9. if there is too big difference between chunkservers then make copy of chunk from server with biggest disk usage on server with lowest disk usage
	if (c->goal() >= vc && vc + tdc>0 && (maxUsage - minUsage) > AcceptableDifference) {
		if (serverCount_==0) {
			serverCount_ = matocsserv_getservers_ordered(ptrs,AcceptableDifference/2.0,&min,&max);
		}
		if (min>0 || max>0) {
			ChunkType chunkType = ChunkType::getStandardChunkType();
			void *srcserv=NULL;
			void *dstserv=NULL;
			if (max>0) {
				for (uint32_t i=0 ; i<max && srcserv==NULL ; i++) {
					if (matocsserv_replication_read_counter(ptrs[serverCount_-1-i])<MaxReadRepl) {
						for (s=c->slisthead ; s && s->ptr!=ptrs[serverCount_-1-i] ; s=s->next) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srcserv = s->ptr;
							chunkType = s->chunkType;
						}
					}
				}
			} else {
				for (uint32_t i=0 ; i<(serverCount_-min) && srcserv==NULL ; i++) {
					if (matocsserv_replication_read_counter(ptrs[serverCount_-1-i])<MaxReadRepl) {
						for (s=c->slisthead ; s && s->ptr!=ptrs[serverCount_-1-i] ; s=s->next) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srcserv = s->ptr;
							chunkType = s->chunkType;
						}
					}
				}
			}
			if (srcserv!=NULL) {
				if (min>0) {
					for (uint32_t i=0 ; i<min && dstserv==NULL ; i++) {
						if (matocsserv_replication_write_counter(ptrs[i])<MaxWriteRepl) {
							if (!chunkPresentOnServer(c, ptrs[i])) {
								dstserv=ptrs[i];
							}
						}
					}
				} else {
					for (uint32_t i=0 ; i<serverCount_-max && dstserv==NULL ; i++) {
						if (matocsserv_replication_write_counter(ptrs[i])<MaxWriteRepl) {
							if (!chunkPresentOnServer(c, ptrs[i])) {
								dstserv=ptrs[i];
							}
						}
					}
				}
				if (dstserv!=NULL) {
					if (tryReplication(c, chunkType, dstserv)) {
						inforec_.copy_rebalance++;
					}
				}
			}
		}
	}
}

static std::unique_ptr<ChunkWorker> gChunkWorker;

void chunk_jobs_main(void) {
	uint32_t i,l,lc,r;
	uint16_t usableServerCount, totalServerCount;
	static uint16_t lastTotalServerCount = 0;
	static uint16_t maxTotalServerCount = 0;
	double minUsage, maxUsage;
	chunk *c,**cp;

	if (starttime + ReplicationsDelayInit > main_time()) {
		return;
	}

	matocsserv_usagedifference(&minUsage, &maxUsage, &usableServerCount, &totalServerCount);

	if (totalServerCount < lastTotalServerCount) {          // servers disconnected
		jobsnorepbefore = main_time() + ReplicationsDelayDisconnect;
	} else if (totalServerCount > lastTotalServerCount) {   // servers connected
		if (totalServerCount >= maxTotalServerCount) {
			maxTotalServerCount = totalServerCount;
			jobsnorepbefore = main_time();
		}
	} else if (totalServerCount < maxTotalServerCount && (uint32_t)main_time() > jobsnorepbefore) {
		maxTotalServerCount = totalServerCount;
	}
	lastTotalServerCount = totalServerCount;

	if (minUsage > maxUsage) {
		return;
	}

	gChunkWorker->doEverySecondTasks();
	lc = 0;
	for (i=0 ; i<HashSteps && lc<HashCPS ; i++) {
		if (jobshpos==0) {
			gChunkWorker->doEveryLoopTasks();
		}
		// delete unused chunks from structures
		l=0;
		cp = &(gChunksMetadata->chunkhash[jobshpos]);
		while ((c=*cp)!=NULL) {
			chunk_handle_disconnected_copies(c);
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
			for (c=gChunksMetadata->chunkhash[jobshpos] ; c ; c=c->next) {
				if (l>=r) {
					gChunkWorker->doChunkJobs(c, usableServerCount, minUsage, maxUsage);
				}
				l++;
			}
			l=0;
			for (c=gChunksMetadata->chunkhash[jobshpos] ; l<r && c ; c=c->next) {
				gChunkWorker->doChunkJobs(c, usableServerCount, minUsage, maxUsage);
				l++;
			}
		}
		jobshpos+=123; // if HASHSIZE is any power of 2 then any odd number is good here
		jobshpos%=HASHSIZE;
	}
}

#endif

constexpr uint32_t kSerializedChunkSizeNoLockId = 16;
constexpr uint32_t kSerializedChunkSizeWithLockId = 20;
#define CHUNKCNT 1000

#ifdef METARESTORE

void chunk_dump(void) {
	chunk *c;
	uint32_t i;

	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=gChunksMetadata->chunkhash[i] ; c ; c=c->next) {
			printf("*|i:%016" PRIX64 "|v:%08" PRIX32 "|g:%" PRIu8 "|t:%10" PRIu32 "\n",c->chunkid,c->version,c->goal(),c->lockedto);
		}
	}
}

#endif

int chunk_load(FILE *fd, bool loadLockIds) {
	uint8_t hdr[8];
	const uint8_t *ptr;
	int32_t r;
	chunk *c;
// chunkdata
	uint64_t chunkid;

	if (fread(hdr,1,8,fd)!=8) {
		return -1;
	}
	ptr = hdr;
	gChunksMetadata->nextchunkid = get64bit(&ptr);
	int32_t serializedChunkSize = (loadLockIds
			? kSerializedChunkSizeWithLockId : kSerializedChunkSizeNoLockId);
	std::vector<uint8_t> loadbuff(serializedChunkSize);
	for (;;) {
		r = fread(loadbuff.data(), 1, serializedChunkSize, fd);
		if (r != serializedChunkSize) {
			return -1;
		}
		ptr = loadbuff.data();
		chunkid = get64bit(&ptr);
		if (chunkid>0) {
			uint32_t version = get32bit(&ptr);
			c = chunk_new(chunkid, version);
			c->lockedto = get32bit(&ptr);
			if (loadLockIds) {
				c->lockid = get32bit(&ptr);
			}
		} else {
			uint32_t version = get32bit(&ptr);
			uint32_t lockedto = get32bit(&ptr);
			if (version==0 && lockedto==0) {
				return 0;
			} else {
				return -1;
			}
		}
	}
	return 0;       // unreachable
}

void chunk_store(FILE *fd) {
	passert(gChunksMetadata);
	uint8_t hdr[8];
	uint8_t storebuff[kSerializedChunkSizeWithLockId * CHUNKCNT];
	uint8_t *ptr;
	uint32_t i,j;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version;
	uint32_t lockedto, lockid;
	ptr = hdr;
	put64bit(&ptr,gChunksMetadata->nextchunkid);
	if (fwrite(hdr,1,8,fd)!=(size_t)8) {
		return;
	}
	j=0;
	ptr = storebuff;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=gChunksMetadata->chunkhash[i] ; c ; c=c->next) {
#ifndef METARESTORE
			chunk_handle_disconnected_copies(c);
#endif
			chunkid = c->chunkid;
			put64bit(&ptr,chunkid);
			version = c->version;
			put32bit(&ptr,version);
			lockedto = c->lockedto;
			lockid = c->lockid;
			put32bit(&ptr,lockedto);
			put32bit(&ptr,lockid);
			j++;
			if (j==CHUNKCNT) {
				size_t writtenBlockSize = kSerializedChunkSizeWithLockId * CHUNKCNT;
				if (fwrite(storebuff, 1, writtenBlockSize, fd) != writtenBlockSize) {
					return;
				}
				j=0;
				ptr = storebuff;
			}
		}
	}
	memset(ptr, 0, kSerializedChunkSizeWithLockId);
	j++;
	size_t writtenBlockSize = kSerializedChunkSizeWithLockId * j;
	if (fwrite(storebuff, 1, writtenBlockSize, fd) != writtenBlockSize) {
		return;
	}
}

void chunk_unload(void) {
	delete gChunksMetadata;
	gChunksMetadata = nullptr;
}

void chunk_newfs(void) {
#ifndef METARESTORE
	chunk::count = 0;
#endif
	gChunksMetadata->nextchunkid = 1;
}

#ifndef METARESTORE
void chunk_become_master() {
	starttime = main_time();
	jobsnorepbefore = starttime+ReplicationsDelayInit;
	gChunkWorker = std::unique_ptr<ChunkWorker>(new ChunkWorker());
	main_timeregister(TIMEMODE_RUN_LATE,1,0,chunk_jobs_main);
	return;
}

void chunk_reload(void) {
	uint32_t repl;
	uint32_t looptime;

	ReplicationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT",300);
	ReplicationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600);

	uint32_t disableChunksDel = cfg_getuint32("DISABLE_CHUNKS_DEL", 0);
	if (disableChunksDel) {
		MaxDelSoftLimit = MaxDelHardLimit = 0;
	} else {
		uint32_t oldMaxDelSoftLimit = MaxDelSoftLimit;
		uint32_t oldMaxDelHardLimit = MaxDelHardLimit;

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
	if (metadataserver::isDuringPersonalityChange()) {
		chunk_become_master();
	}
}
#endif

int chunk_strinit(void) {
	gChunksMetadata = new ChunksMetadata;

#ifndef METARESTORE
	chunk::count = 0;
	for (int i = 0; i < 11; ++i) {
		for (int j = 0; j < 11; ++j) {
			chunk::allStandardChunkCopies[i][j] = 0;
			chunk::regularStandardChunkCopies[i][j] = 0;
		}
	}
	chunk::allChunksAvailability = ChunksAvailabilityState();
	chunk::regularChunksAvailability = ChunksAvailabilityState();
	chunk::allChunksReplicationState = ChunksReplicationState();
	chunk::regularChunksReplicationState = ChunksReplicationState();

	uint32_t disableChunksDel = cfg_getuint32("DISABLE_CHUNKS_DEL", 0);
	ReplicationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT",300);
	ReplicationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600);
	if (disableChunksDel) {
		MaxDelHardLimit = MaxDelSoftLimit = 0;
	} else {
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
		if (MaxDelSoftLimit == 0) {
			fprintf(stderr,"delete limit is zero !!!\n");
			return -1;
		}
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

	uint32_t looptime;
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
	jobshpos = 0;
	jobsrebalancecount = 0;
	main_reloadregister(chunk_reload);
	main_canexitregister(chunk_canexit);
	main_eachloopregister(chunk_clean_zombie_servers_a_bit);
	if (metadataserver::isMaster()) {
		chunk_become_master();
	}
#endif
	return 1;
}
