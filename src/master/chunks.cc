/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of LizardFS and is part of LizardFS.

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
#include "common/main.h"
#include "common/massert.h"
#include "common/LFSCommunication.h"
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

#define MINLOOPTIME 1
#define MAXLOOPTIME 7200
#define MAXCPS 10000000
#define MINCPS 10000

#define HASHSIZE 0x100000
#define HASHPOS(chunkid) (((uint32_t)chunkid)&0xFFFFF)

#define CHECKSUMSEED 78765491511151883ULL

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
	matocsserventry *ptr;
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

#ifndef METARESTORE
static std::vector<matocsserventry*> zombieServersHandledInThisLoop;
static std::vector<matocsserventry*> zombieServersToBeHandledInNextLoop;
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
	uint32_t lockedto;
private: // public/private sections are mixed here to make the struct as small as possible
	ChunkGoalCounters goalCounters_;
#ifndef METARESTORE
	uint8_t allValidCopies_, regularValidCopies_;
	uint8_t allMissingCopies_, regularMissingCopies_;
	uint8_t allRedundantCopies_, regularRedundantCopies_;
	uint8_t goalInStats_;
	uint8_t copiesInStats_;
#endif
public:
#ifndef METARESTORE
	uint8_t needverincrease:1;
	uint8_t interrupted:1;
	uint8_t operation:4;
	static ChunksAvailabilityState allChunksAvailability, regularChunksAvailability;
	static ChunksReplicationState allChunksReplicationState, regularChunksReplicationState;
	static uint64_t count;
	static uint64_t allValidCopies[CHUNK_MATRIX_SIZE][CHUNK_MATRIX_SIZE];
	static uint64_t regularValidCopies[CHUNK_MATRIX_SIZE][CHUNK_MATRIX_SIZE];
#endif

	/// ID of the chunk's goal.
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
	/// The expected number of chunk's copies.
	uint8_t expectedCopies() const {
		return fs_get_goal_definition(goal()).getExpectedCopies();
	}

	// This method should be called on a new chunk
	void initStats() {
		count++;
		allValidCopies_ = regularValidCopies_ = 0;
		allRedundantCopies_ = regularRedundantCopies_ = 0;
		allMissingCopies_ = regularMissingCopies_ = 0;
		copiesInStats_ = 0;
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

		uint32_t allMissingCopiesOfLabels = 0;
		uint32_t regularMissingCopiesOfLabels = 0;
		const Goal::Labels& labels = fs_get_goal_definition(goal()).labels();
		for (const auto& labelAndCount : labels) {
			const auto& label = labelAndCount.first;
			if (label == kMediaLabelWildcard) {
				continue;
			}
			uint32_t allCopiesOfLabel = 0;
			uint32_t regularCopiesOfLabel = 0;
			for (slist* s = slisthead; s != nullptr; s = s->next) {
				if (!s->is_valid() || matocsserv_get_label(s->ptr) != label) {
					continue;
				}
				allCopiesOfLabel++;
				if (!s->is_todel()) {
					regularCopiesOfLabel++;
				}
			}
			uint32_t expectedCopiesOfLabel = labelAndCount.second;
			if (allCopiesOfLabel < expectedCopiesOfLabel) {
				allMissingCopiesOfLabels += expectedCopiesOfLabel - allCopiesOfLabel;
			}
			if (regularCopiesOfLabel < expectedCopiesOfLabel) {
				regularMissingCopiesOfLabels += expectedCopiesOfLabel - regularCopiesOfLabel;
			}
		}

		allMissingCopies_ = missingCopies(allValidCopies_, allMissingCopiesOfLabels);
		regularMissingCopies_ = missingCopies(regularValidCopies_, regularMissingCopiesOfLabels);
		allRedundantCopies_ = redundantCopies(allValidCopies_, allMissingCopies_);
		regularRedundantCopies_ = redundantCopies(regularValidCopies_, regularMissingCopies_);
		addToStats();
	}

	/**
	 * Given number of valid copies and number of missing copies for non-wildcard labels return
	 * overall number of missing copies
	 */
	uint32_t missingCopies(uint32_t validCopies, uint32_t missingCopiesOfLabels) {
		uint32_t ret = std::max<uint32_t>(
				expectedCopies() > validCopies ? expectedCopies() - validCopies : 0,
				missingCopiesOfLabels);
		return std::min<uint32_t>(200U, ret);
	}

	/**
	 * Given number of valid copies and number of missing copies return number of redundant copies
	 */
	uint32_t redundantCopies(uint32_t validCopies, uint32_t missingCopies) {
		uint32_t validAndMissing = validCopies + missingCopies;
		uint32_t ret = expectedCopies() < validAndMissing ? validAndMissing - expectedCopies() : 0;
		return std::min<uint32_t>(200U, ret);
	}

	bool isSafe() const {
		return allValidCopies_ >= 2;
	}

	bool isEndangered() const {
		return allValidCopies_ == 1;
	}

	bool isLost() const {
		return allValidCopies_ == 0;
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

	slist* addCopyNoStatsUpdate(matocsserventry *ptr, uint8_t valid, uint32_t version) {
		slist *s = slist_malloc();
		s->ptr = ptr;
		s->valid = valid;
		s->version = version;
		s->next = slisthead;
		slisthead = s;
		return s;
	}

	slist* addCopy(matocsserventry *ptr, uint8_t valid, uint32_t version) {
		slist *s = addCopyNoStatsUpdate(ptr, valid, version);
		updateStats();
		return s;
	}

private:
	ChunksAvailabilityState::State allCopiesState() const {
		if (allValidCopies_ >= 2) {
			return ChunksAvailabilityState::kSafe;
		} else if (allValidCopies_ == 1) {
			return ChunksAvailabilityState::kEndangered;
		} else {
			return ChunksAvailabilityState::kLost;
		}
	}

	ChunksAvailabilityState::State regularCopiesState() const {
		if (regularValidCopies_ >= 2) {
			return ChunksAvailabilityState::kSafe;
		} else if (regularValidCopies_ == 1) {
			return ChunksAvailabilityState::kEndangered;
		} else {
			return ChunksAvailabilityState::kLost;
		}
	}

	void removeFromStats() {
		allChunksAvailability.removeChunk(goalInStats_, allCopiesState());
		allChunksReplicationState.removeChunk(goalInStats_, allMissingCopies_,
				allRedundantCopies_);

		regularChunksAvailability.removeChunk(goalInStats_, regularCopiesState());
		regularChunksReplicationState.removeChunk(goalInStats_,
				regularMissingCopies_, regularRedundantCopies_);

		uint8_t limitedCopies = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, copiesInStats_);
		uint8_t limitedAll = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, allValidCopies_);
		uint8_t limitedRegular = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, regularValidCopies_);
		allValidCopies[limitedCopies][limitedAll]--;
		regularValidCopies[limitedCopies][limitedRegular]--;
	}

	void addToStats() {
		copiesInStats_ = expectedCopies();
		goalInStats_ = goal();
		allChunksAvailability.addChunk(goalInStats_, allCopiesState());
		allChunksReplicationState.addChunk(goalInStats_, allMissingCopies_, allRedundantCopies_);

		regularChunksAvailability.addChunk(goalInStats_, regularCopiesState());
		regularChunksReplicationState.addChunk(goalInStats_,
				regularMissingCopies_, regularRedundantCopies_);

		uint8_t limitedCopies = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, copiesInStats_);
		uint8_t limitedAll = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, allValidCopies_);
		uint8_t limitedRegular = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, regularValidCopies_);
		allValidCopies[limitedCopies][limitedAll]++;
		regularValidCopies[limitedCopies][limitedRegular]++;
	}
#endif
};

#ifndef METARESTORE
ChunksAvailabilityState chunk::allChunksAvailability, chunk::regularChunksAvailability;
ChunksReplicationState chunk::allChunksReplicationState, chunk::regularChunksReplicationState;
uint64_t chunk::count;
uint64_t chunk::allValidCopies[CHUNK_MATRIX_SIZE][CHUNK_MATRIX_SIZE];
uint64_t chunk::regularValidCopies[CHUNK_MATRIX_SIZE][CHUNK_MATRIX_SIZE];
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
static bool RebalancingBetweenLabels = false;

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
	hashCombine(checksum, c->chunkid, c->version, c->lockedto, c->goal(), c->fileCount());
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
		chunk_update_checksum(c);
	} else {
		matoclserv_chunk_status(c->chunkid,ERROR_CHUNKLOST);
	}
	fs_incversion(c->chunkid);
}

bool chunk_server_is_disconnected(matocsserventry* ptr) {
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
	*allchunks = chunk::count;
	*allcopies = 0;
	*regularvalidcopies = 0;
	for (int actualCopies = 1; actualCopies < CHUNK_MATRIX_SIZE; actualCopies++) {
		uint32_t ag = 0;
		uint32_t rg = 0;
		for (int expectedCopies = 0; expectedCopies < CHUNK_MATRIX_SIZE; expectedCopies++) {
			ag += chunk::allValidCopies[expectedCopies][actualCopies];
			rg += chunk::regularValidCopies[expectedCopies][actualCopies];
		}
		*allcopies += ag * actualCopies;
		*regularvalidcopies += rg * actualCopies;
	}
}

uint32_t chunk_get_missing_count(void) {
	uint32_t res = 0;
	for (int expectedCopies = 1; expectedCopies < CHUNK_MATRIX_SIZE; expectedCopies++) {
		res += chunk::allValidCopies[expectedCopies][0];
	}
	return res;
}

void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid) {
	if (matrixid == MATRIX_ALL_COPIES) {
		for (int i = 0; i < CHUNK_MATRIX_SIZE; i++) {
			for (int j = 0; j < CHUNK_MATRIX_SIZE; j++) {
				put32bit(&buff, chunk::allValidCopies[i][j]);
			}
		}
	} else if (matrixid == MATRIX_REGULAR_COPIES) {
		for (int i = 0; i < CHUNK_MATRIX_SIZE; i++) {
			for (int j = 0; j < CHUNK_MATRIX_SIZE; j++) {
				put32bit(&buff, chunk::regularValidCopies[i][j]);
			}
		}
	} else {
		memset(buff, 0, CHUNK_MATRIX_SIZE * CHUNK_MATRIX_SIZE * sizeof(uint32_t));
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
	*vcopies = c->allValidCopiesCount();
	return STATUS_OK;
}

uint8_t chunk_multi_modify(uint64_t ochunkid, uint8_t goal, bool quota_exceeded,
		uint8_t *opflag, uint64_t *nchunkid) {
	chunk *c = NULL;
	if (ochunkid == 0) { // new chunk
		if (quota_exceeded) {
			return ERROR_QUOTA;
		}
		auto servers = matocsserv_getservers_for_new_chunk(goal);
		if (servers.empty()) {
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
		for (uint32_t i = 0; i < servers.size(); i++) {
			slist *s = c->addCopyNoStatsUpdate(servers[i], BUSY, c->version);
			matocsserv_send_createchunk(s->ptr,c->chunkid,c->version);
		}
		c->updateStats();
		*opflag=1;
		*nchunkid = c->chunkid;
	} else {
		chunk *oc = chunk_find(ochunkid);
		if (oc==NULL) {
			return ERROR_NOCHUNK;
		}
		if (oc->isLocked()) {
			return ERROR_LOCKED;
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
					slist *s = c->addCopyNoStatsUpdate(os->ptr, BUSY, c->version);
					matocsserv_send_duplicatechunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version);
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
	chunk_update_checksum(c);
	return STATUS_OK;
}

uint8_t chunk_multi_truncate(uint64_t ochunkid, uint32_t length, uint8_t goal, bool quota_exceeded,
		uint64_t *nchunkid) {
	chunk *c = NULL;
	chunk *oc = chunk_find(ochunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->isLocked()) {
		return ERROR_LOCKED;
	}
	if (oc->fileCount() == 1) { // refcount==1
		*nchunkid = ochunkid;
		c = oc;
		if (c->operation!=NONE) {
			return ERROR_CHUNKBUSY;
		}
		uint32_t i = 0;
		for (slist *s = c->slisthead; s; s = s->next) {
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
					c->operation = DUPTRUNC;
					chunk_delete_file_int(oc,goal);
					chunk_add_file_int(c,goal);
				}
				slist *s = c->addCopyNoStatsUpdate(os->ptr, BUSY, c->version);
				matocsserv_send_duptruncchunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version,length);
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

	c->lockedto=main_time()+LOCKTIMEOUT;
	chunk_update_checksum(c);
	return STATUS_OK;
}
#endif // ! METARESTORE

uint8_t chunk_apply_modification(uint32_t ts, uint64_t oldChunkId, uint8_t goal,
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
	ChunkLocation() : distance(0), random(0) {
	}
	NetworkAddress address;
	uint32_t distance;
	uint32_t random;
	MediaLabel* label;
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
		uint32_t maxNumberOfChunkCopies, std::vector<ChunkWithAddressAndLabel>& serversList) {
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
					&(chunkserverLocation.address.port),
					&(chunkserverLocation.label)) == 0) {
				chunkserverLocation.distance =
						topology_distance(chunkserverLocation.address.ip, currentIp);
						// in the future prepare more sophisticated distance function
				chunkserverLocation.random = rndu32();
				chunkLocation.push_back(chunkserverLocation);
				cnt++;
			}
		}
	}
	std::sort(chunkLocation.begin(), chunkLocation.end());
	for (uint i = 0; i < chunkLocation.size(); ++i) {
		const ChunkLocation& loc = chunkLocation[i];
		uint8_t reserved = 0;
		serversList.emplace_back(loc.address, *loc.label, reserved);
	}
	return STATUS_OK;
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
			MediaLabel* dummy = nullptr;
			if (cnt < 100 && matocsserv_getlocation(
					s->ptr, &(lstab[cnt].ip), &(lstab[cnt].port), &dummy) == 0) {
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

void chunk_server_has_chunk(matocsserventry *ptr,uint64_t chunkid,uint32_t version) {
	chunk *c;
	slist *s;
	const uint32_t new_version = version & 0x7FFFFFFF;
	const bool todel = version & 0x80000000;
	c = chunk_find(chunkid);
	if (c==NULL) {
		// syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016" PRIX64 "_%08" PRIX32 "), so create it for future deletion",chunkid,version);
		if (chunkid>=gChunksMetadata->nextchunkid) {
			fs_set_nextchunkid(FsContext::getForMaster(main_time()), chunkid + 1);
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

void chunk_damaged(matocsserventry *ptr,uint64_t chunkid) {
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
	s = c->addCopyNoStatsUpdate(ptr, INVALID, 0);
	c->needverincrease=1;
}

void chunk_lost(matocsserventry *ptr,uint64_t chunkid) {
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

void chunk_server_disconnected(matocsserventry *ptr) {
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

void chunk_got_delete_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status) {
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

void chunk_got_replicate_status(matocsserventry *ptr,uint64_t chunkid,uint32_t version,uint8_t status) {
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
	const uint8_t state = (c->isLocked() || version != c->version) ? INVALID : VALID;
	s = c->addCopy(ptr, state, version);
}


void chunk_operation_status(chunk *c,uint8_t status,matocsserventry *ptr) {
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

void chunk_got_chunkop_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_create_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_duplicate_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_setversion_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_truncate_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_duptrunc_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status) {
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

class ChunkWorker {
public:
	ChunkWorker();
	void doEveryLoopTasks();
	void doEverySecondTasks();
	void doChunkJobs(chunk *c, uint16_t serverCount);

private:
	typedef std::vector<ServerWithUsage> ServersWithUsage;

	bool tryReplication(chunk *c, matocsserventry *destinationServer);

	loop_info inforec_;
	uint32_t deleteNotDone_;
	uint32_t deleteDone_;
	uint32_t prevToDeleteCount_;
	uint32_t deleteLoopCount_;

	/// All chunkservers sorted by disk usage.
	ServersWithUsage sortedServers_;

	/// For each label, all servers with this label sorted by disk usage.
	std::map<MediaLabel, ServersWithUsage> labeledSortedServers_;
};

ChunkWorker::ChunkWorker()
		: deleteNotDone_(0),
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
	sortedServers_ = matocsserv_getservers_sorted();
	labeledSortedServers_.clear();
	for (const ServerWithUsage& sw : sortedServers_) {
		labeledSortedServers_[*(sw.label)].push_back(sw);
	}
}

static bool chunkPresentOnServer(chunk *c, matocsserventry *server) {
	for (slist *s = c->slisthead ; s ; s = s->next) {
		if (s->ptr == server) {
			return true;
		}
	}
	return false;
}

void ChunkWorker::doChunkJobs(chunk *c, uint16_t serverCount) {
	// step 0. Update chunk's statistics
	// Useful e.g. if definitions of goals did change.
	c->updateStats();

	if (serverCount == 0) {
		return;
	}

	// step 1. calculate number of valid and invalid copies
	uint32_t vc, tdc, ivc, bc, tdb, dc;
	vc = tdc = ivc = bc = tdb = dc = 0;
	const Goal::Labels& expectedCopies = fs_get_goal_definition(c->goal()).labels();
	Goal::Labels validCopies;

	for (slist *s = c->slisthead; s; s = s->next) {
		switch (s->valid) {
		case INVALID:
			ivc++;
			break;
		case TDVALID:
			tdc++;
			break;
		case VALID:
			++validCopies[matocsserv_get_label(s->ptr)];
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
		for (slist *s = c->slisthead; s; s = s->next) {
			syslog(LOG_NOTICE,"chunk %016" PRIX64 "_%08" PRIX32 " - invalid copy on (%s - ver:%08" PRIX32 ")",c->chunkid,c->version,matocsserv_getstrip(s->ptr),s->version);
		}
		return ;
	}

	// step 3. delete invalid copies
	for (slist *s = c->slisthead; s; s = s->next) {
		if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
			if (!s->is_valid()) {
				if (s->valid==DEL) {
					syslog(LOG_WARNING,"chunk hasn't been deleted since previous loop - retry");
				}
				s->valid = DEL;
				stats_deletions++;
				matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
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
	if (c->fileCount() == 0) {
		for (slist *s = c->slisthead; s; s=s->next) {
			if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
				if (s->is_valid() && !s->is_busy()) {
					c->deleteCopy(s);
					c->needverincrease=1;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,c->version);
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

	if (vc + tdc == 0) {
		return;
	}

	// step 7. check if chunk needs any replication

	// First, order labels assigned to the goal of the current chunk in a way that wildcard is
	// the last one. This would prevent us from making a copy on a random servers (which is
	// determined by wildcards) before making sure that we have enough copies on servers required
	// by other labels in the goal.
	bool triedToReplicate = false;
	std::vector<std::reference_wrapper<const Goal::Labels::value_type>> labelsAndExpectedCopies(
			expectedCopies.begin(), expectedCopies.end());
	std::partition(labelsAndExpectedCopies.begin(), labelsAndExpectedCopies.end(),
			[](const Goal::Labels::value_type& labelGoal) {
				return labelGoal.first != kMediaLabelWildcard;
			}
	);

	// Sometimes we will be temporary unable (due to replication limits) to make a copy on a server
	// pointed by some label. We will count how many non-wildcard copies we have skipped because of
	// this fact to determine how many wildcard copies we can create -- we don't want to create
	// a copy on a random server because some required server was temporary overloaded.
	uint32_t skippedReplications = 0;

	// Now, analyze the goal label by label.
	for (const auto& labelAndExpectedCopies : labelsAndExpectedCopies) {
		const MediaLabel& label = labelAndExpectedCopies.get().first;
		int expectedCopiesForLabel = labelAndExpectedCopies.get().second;

		// First, determine if we need more copies for the current label.
		// For each non-wildcard label we need the number of valid copies determined by the goal.
		// For the wildcard label we will create copies on any servers until we have exactly
		// 'c->expectedCopies()' valid copies.
		int missingCopiesForLabel;
		if (label == kMediaLabelWildcard) {
			missingCopiesForLabel = c->expectedCopies() - (vc + skippedReplications);
		} else {
			missingCopiesForLabel = expectedCopiesForLabel - validCopies[label];
		}
		if (missingCopiesForLabel <= 0) {
			// No replication is needed for the current label, go to the next one
			continue;
		}
		triedToReplicate = true;

		// After setting triedToReplicate we can verify this condition
		if (jobsnorepbefore >= main_time()) {
			break;
		}

		// Get a list of possible destination servers
		static matocsserventry* servers[65536];
		uint16_t totalMatching = 0;
		uint16_t returnedMatching = 0;
		uint32_t destinationCount = matocsserv_getservers_lessrepl(label, MaxWriteRepl,
				servers, &totalMatching, &returnedMatching);
		if (label != kMediaLabelWildcard && totalMatching > returnedMatching) {
			// There is a server which matches the current label, but it has exceeded the
			// replication limit. In this case we won't try to use servers with non-matching
			// labels as our destination -- we will wait for that server to be ready.
			destinationCount = returnedMatching;
		}

		// Find a destination server for replication -- the first one without a copy of 'c'
		matocsserventry *destination = nullptr;
		for (uint32_t i = 0; i < destinationCount; i++) {
			if (!chunkPresentOnServer(c, servers[i])) {
				destination = servers[i];
				break;
			}
		}
		if (destination == nullptr) {
			// there is no server suitable for replication to be written to
			skippedReplications += missingCopiesForLabel;
			continue;
		}

		// Find a source server. Prefer VALID over TDVALID copies.
		uint8_t typeOfSources = TDVALID;
		uint16_t availableSources = 0;
		for (slist *s = c->slisthead; s; s = s->next) {
			if (matocsserv_replication_read_counter(s->ptr) >= MaxReadRepl) {
				continue;
			}
			if (s->valid == VALID && typeOfSources == TDVALID) {
				// We have found a VALID source, so let's remove all TDVALID (if any) from servers
				typeOfSources = VALID;
				availableSources = 0;
			}
			if (s->valid == typeOfSources) {
				servers[availableSources++] = s->ptr;
			}
		}
		if (availableSources == 0) {
			// There is no server suitable for replication to be read from
			skippedReplications += missingCopiesForLabel;
			break; // there's no need to analyze other labels if there's no free source server
		}
		matocsserventry *source = servers[rndu32_ranged(availableSources)];

		// Initialize the replication
		stats_replications++;
		matocsserv_send_replicatechunk(destination, c->chunkid, c->version, source);
		c->needverincrease = 1;
		inforec_.done.copy_undergoal++;
		return;
	}
	if (triedToReplicate) {
		inforec_.notdone.copy_undergoal++;
		return; // Don't go to the next step (= don't delete any copies) for undergoal chunks
	}

	// step 8. if chunk has too many copies then delete some of them
	if (vc > c->expectedCopies()) {
		const uint32_t overgoalCopies = vc - c->expectedCopies();
		typedef std::vector<slist*> Candidates;
		Candidates candidates;
		for (uint32_t i = 0; i < overgoalCopies; ++ i) {
			slist* candidate = nullptr;
			double maxUsage = 0.;
			for (slist *s = c->slisthead; s != nullptr; s = s->next) {
				if (s->valid != VALID) {
					continue;
				}
				if (std::find(candidates.begin(), candidates.end(), s) != candidates.end()) {
					continue;
				}
				const MediaLabel& csLabel = matocsserv_get_label(s->ptr);
				/*
				 * If copies' chunkserver has non-wildcard label
				 * and this label is in this chunk goal
				 * and this chunk does not have overgoal on this label
				 * then skip processing this chunkserver.
				 */
				if (csLabel != kMediaLabelWildcard
						&& expectedCopies.count(csLabel)
						&& validCopies[csLabel] <= expectedCopies.at(csLabel)) {
					continue;
				}
				double usage = matocsserv_get_usage(s->ptr);
				if (usage > maxUsage) {
					candidate = s;
					usage = maxUsage;
				}
			}
			if (candidate != nullptr) {
				--validCopies[matocsserv_get_label(candidate->ptr)];
				candidates.push_back(candidate);
			} else {
				break;
			}
		}
		uint32_t copiesRemoved = 0;
		for (slist* s : candidates) {
			if (matocsserv_deletion_counter(s->ptr) >= TmpMaxDel) {
				continue;
			}
			c->deleteCopy(s);
			c->needverincrease=1;
			stats_deletions++;
			matocsserv_send_deletechunk(s->ptr, c->chunkid, 0);
			copiesRemoved++;
		}
		inforec_.done.del_overgoal += copiesRemoved;
		deleteDone_ += copiesRemoved;
		inforec_.notdone.del_overgoal += (overgoalCopies - copiesRemoved);
		deleteNotDone_ += (overgoalCopies - copiesRemoved);
		return;
	}

	// step 9. if chunk has one copy on each server and some of them have status TODEL then delete one of it
	if (vc+tdc>=serverCount && vc<c->expectedCopies() && tdc>0 && vc+tdc>1) {
		uint8_t prevdone;
		prevdone = 0;
		for (slist *s = c->slisthead; s && prevdone==0; s = s->next) {
			if (s->valid==TDVALID) {
				if (matocsserv_deletion_counter(s->ptr)<TmpMaxDel) {
					c->deleteCopy(s);
					c->needverincrease=1;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
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

	if (chunksinfo.notdone.copy_undergoal>0 && chunksinfo.done.copy_undergoal>0) {
		return;
	}

	// step 10. if there is too big difference between chunkservers then make copy of chunk from
	// a server with a high disk usage on a server with low disk usage
	double minUsage = sortedServers_.front().diskUsage;
	double maxUsage = sortedServers_.back().diskUsage;
	if ((maxUsage - minUsage) > AcceptableDifference) {
		// Consider each copy to be moved to a server with disk usage much less than actual.
		// There are at least two servers with a disk usage difference grater than
		// AcceptableDifference, so it's worth checking.
		for (slist *s = c->slisthead; s != nullptr; s = s->next) {
			if (!s->is_valid() || matocsserv_replication_read_counter(s->ptr) >= MaxReadRepl) {
				continue;
			}
			const MediaLabel& currentCopyLabel = matocsserv_get_label(s->ptr);
			double currentCopyDiskUsage = matocsserv_get_usage(s->ptr);
			// Look for a server that has disk usage much less than currentCopyDiskUsage.
			// If such a server exists consider creating a new copy of this chunk there.
			// First, choose all possible candidates for the destination server: we consider only
			// servers with the same label is rebalancing between labels if turned off or the goal
			// requires our copy to exist on a server labeled 'currentCopyLabel'.
			bool labelOnlyRebalance = !RebalancingBetweenLabels
					|| (currentCopyLabel != kMediaLabelWildcard
						&& expectedCopies.count(currentCopyLabel)
						&& validCopies[currentCopyLabel] <= expectedCopies.at(currentCopyLabel));
			const ServersWithUsage& sortedServers = labelOnlyRebalance
					? labeledSortedServers_[currentCopyLabel] : sortedServers_;
			for (uint32_t i = 0; i < sortedServers.size(); ++i) {
				const ServerWithUsage& emptyServer = sortedServers[i];
				if (emptyServer.diskUsage > currentCopyDiskUsage - AcceptableDifference) {
					break; // No more suitable destination servers (next servers have higher usage)
				}
				if (chunkPresentOnServer(c, emptyServer.server)) {
					continue; // A copy is already here
				}
				if (matocsserv_replication_write_counter(emptyServer.server) >= MaxWriteRepl) {
					continue; // We can't create a new copy here
				}
				stats_replications++;
				matocsserv_send_replicatechunk(emptyServer.server, c->chunkid, c->version, s->ptr);
				c->needverincrease=1;
				inforec_.copy_rebalance++;
				return;
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
					gChunkWorker->doChunkJobs(c, usableServerCount);
				}
				l++;
			}
			l=0;
			for (c=gChunksMetadata->chunkhash[jobshpos] ; l<r && c ; c=c->next) {
				gChunkWorker->doChunkJobs(c, usableServerCount);
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
		for (c=gChunksMetadata->chunkhash[i] ; c ; c=c->next) {
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
	gChunksMetadata->nextchunkid = get64bit(&ptr);
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
	passert(gChunksMetadata);
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
	RebalancingBetweenLabels = cfg_getuint32("CHUNKS_REBALANCING_BETWEEN_LABELS", 0) == 1;
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
	for (int i = 0; i < CHUNK_MATRIX_SIZE; ++i) {
		for (int j = 0; j < CHUNK_MATRIX_SIZE; ++j) {
			chunk::allValidCopies[i][j] = 0;
			chunk::regularValidCopies[i][j] = 0;
		}
	}

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
	RebalancingBetweenLabels = cfg_getuint32("CHUNKS_REBALANCING_BETWEEN_LABELS", 0) == 1;
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
