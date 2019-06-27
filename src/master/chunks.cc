/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o..

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
#include <unordered_map>
#include <algorithm>
#include <deque>

#include "common/chunks_availability_state.h"
#include "common/chunk_copies_calculator.h"
#include "common/compact_vector.h"
#include "common/counting_sort.h"
#include "common/coroutine.h"
#include "common/datapack.h"
#include "common/exceptions.h"
#include "common/event_loop.h"
#include "common/flat_set.h"
#include "common/goal.h"
#include "common/hashfn.h"
#include "common/lizardfs_version.h"
#include "common/loop_watchdog.h"
#include "common/massert.h"
#include "common/slice_traits.h"
#include "common/small_vector.h"
#include "master/chunkserver_db.h"
#include "master/checksum.h"
#include "master/chunk_goal_counters.h"
#include "master/filesystem.h"
#include "master/get_servers_for_new_chunk.h"
#include "master/goal_cache.h"
#include "protocol/MFSCommunication.h"

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
#define MINCPS 500
#define MINCHUNKSLOOPPERIOD 40
#define MAXCHUNKSLOOPPERIOD 10000
#define MINCHUNKSLOOPCPU    10
#define MAXCHUNKSLOOPCPU    90

#define HASHSIZE 0x100000
#define HASHPOS(chunkid) (((uint32_t)chunkid)&0xFFFFF)

#define CHECKSUMSEED 78765491511151883ULL

#ifndef METARESTORE

static uint32_t gRedundancyLevel;
static uint64_t gEndangeredChunksServingLimit;
static uint64_t gEndangeredChunksMaxCapacity;
static uint64_t gDisconnectedCounter = 0;
bool gAvoidSameIpChunkservers = false;

struct ChunkPart {
	enum {
		INVALID =
		    0,  /*!< Wrong version / or got info from chunkserver (IO error etc.)  ->  to delete. */
		DEL,    /*!< Deletion in progress. */
		BUSY,   /*!< Operation in progress. */
		VALID,  /*!< Ok. */
		TDBUSY, /*!< To delete + BUSY. */
		TDVALID /*!< Want to be deleted. */
	};

	uint32_t version;   /*!< Part version. */
	ChunkPartType type; /*!< Part type. */
	uint16_t csid : 13; /*!< Chunkserver id. */
	uint16_t state : 3; /*!< Chunk part state. */

	ChunkPart() : version(0), type(), csid(0), state(INVALID) {
	}

	ChunkPart(const ChunkPart &other)
	    : version(other.version), type(other.type), csid(other.csid), state(other.state) {
	}

	ChunkPart(uint16_t part_csid, int part_state, uint32_t part_version,
	          const ChunkPartType &part_type)
	    : version(part_version), type(part_type), csid(part_csid), state(part_state) {
	}

	bool is_busy() const {
		return state == BUSY || state == TDBUSY;
	}

	bool is_valid() const {
		return state != INVALID && state != DEL;
	}

	bool is_todel() const {
		return state == TDVALID || state == TDBUSY;
	}

	void mark_busy() {
		switch (state) {
		case VALID:
			state = BUSY;
			break;
		case TDVALID:
			state = TDBUSY;
			break;
		default:
			sassert(!"ChunkPartInfo::mark_busy(): wrong state");
		}
	}
	void unmark_busy() {
		switch (state) {
		case BUSY:
			state = VALID;
			break;
		case TDBUSY:
			state = TDVALID;
			break;
		default:
			sassert(!"ChunkPartInfo::unmark_busy(): wrong state");
		}
	}
	void mark_todel() {
		switch (state) {
		case VALID:
			state = TDVALID;
			break;
		case BUSY:
			state = TDBUSY;
			break;
		default:
			sassert(!"ChunkPartInfo::mark_todel(): wrong state");
		}
	}
	void unmark_todel() {
		switch (state) {
		case TDVALID:
			state = VALID;
			break;
		case TDBUSY:
			state = BUSY;
			break;
		default:
			sassert(!"ChunkPartInfo::unmark_todel(): wrong state");
		}
	}

	matocsserventry *server() const {
		assert(csdb_find(csid));
		assert(csdb_find(csid)->eptr);
		return csdb_find(csid)->eptr;
	}
};

static void*                         gChunkLoopEventHandle = NULL;

static uint32_t gOperationsDelayDisconnect = 3600;
static uint32_t gOperationsDelayInit = 300;

static uint32_t MaxWriteRepl;
static uint32_t MaxReadRepl;
static uint32_t MaxDelSoftLimit;
static uint32_t MaxDelHardLimit;
static double   TmpMaxDelFrac;
static uint32_t TmpMaxDel;
static uint32_t HashSteps;
static uint32_t HashCPS;
static uint32_t ChunksLoopPeriod;
static uint32_t ChunksLoopTimeout;
static double   gAcceptableDifference;
static bool     RebalancingBetweenLabels = false;

static uint32_t jobsnorepbefore;

static uint32_t starttime;
#endif // METARESTORE

class Chunk {
	static constexpr int kMaxStatCount = 15;
	static_assert(CHUNK_MATRIX_SIZE <= kMaxStatCount, "stats matrix size too big for internal stats storage");
	static_assert(ChunksAvailabilityState::kStateCount <= 3, "not enough space for chunk state");

public:
	/* chunk.operation */
	enum {
		NONE,
		CREATE,
		SET_VERSION,
		DUPLICATE,
		TRUNCATE,
		DUPTRUNC
	};

	uint64_t chunkid;
	uint64_t checksum;
	Chunk *next;
#ifndef METARESTORE
	compact_vector<ChunkPart> parts;
#endif
private: // public/private sections are mixed here to make the struct as small as possible
	ChunkGoalCounters goalCounters_;
public:
	uint32_t version;
	uint32_t lockid;
	uint32_t lockedto;
#ifndef METARESTORE
	uint8_t inEndangeredQueue:1;
	uint8_t needverincrease:1;
	uint8_t interrupted:1;
	uint8_t operation:3;
private:
	uint8_t allAvailabilityState_:2;
	uint8_t copiesInStats_:4;
	uint8_t allMissingParts_:4;
	uint8_t allRedundantParts_:4;
	uint8_t allFullCopies_:4;
#endif

public:
#ifndef METARESTORE
	static ChunksAvailabilityState allChunksAvailability;
	static ChunksReplicationState allChunksReplicationState;
	static uint64_t count;
	static uint64_t allFullChunkCopies[CHUNK_MATRIX_SIZE][CHUNK_MATRIX_SIZE];
	static std::deque<Chunk *> endangeredChunks;
	static GoalCache goalCache;
#endif

	void clear() {
		goalCounters_.clear();
		next = nullptr;
		chunkid = 0;
		version = 0;
		lockid = 0;
		lockedto = 0;
		checksum = 0;
#ifndef METARESTORE
		inEndangeredQueue = 0;
		needverincrease = 1;
		interrupted = 0;
		operation = Chunk::NONE;
		parts.clear();
		allMissingParts_ = 0;
		allRedundantParts_= 0;
		allFullCopies_ = 0;
		allAvailabilityState_ = ChunksAvailabilityState::kSafe;
		copiesInStats_ = 0;
		count++;
		updateStats(false);
#endif
	}

	// Highest id of the chunk's goal
	// This function is preserved only for backward compatibility of metadata checksums
	// and shouldn't be used anywhere else.
	uint8_t highestIdGoal() const {
		return goalCounters_.highestIdGoal();
	}

	// Number of files this chunk belongs to
	uint32_t fileCount() const {
		return goalCounters_.fileCount();
	}

	// Called when this chunk becomes a part of a file with the given goal
	void addFileWithGoal(uint8_t goal) {
#ifndef METARESTORE
		removeFromStats();
#endif
		goalCounters_.addFile(goal);
#ifndef METARESTORE
		updateStats(false);
#endif
	}

	// Called when a file that this chunk belongs to is removed
	void removeFileWithGoal(uint8_t goal) {
#ifndef METARESTORE
		removeFromStats();
#endif
		goalCounters_.removeFile(goal);
#ifndef METARESTORE
		updateStats(false);
#endif
	}

	// Called when a file that this chunk belongs to changes goal
	void changeFileGoal(uint8_t prevGoal, uint8_t newGoal) {
#ifndef METARESTORE
		removeFromStats();
#endif
		goalCounters_.changeFileGoal(prevGoal, newGoal);
#ifndef METARESTORE
		updateStats(false);
#endif
	}

#ifndef METARESTORE
	Goal getGoal() {
		// Do not search for empty goalCounters in cache
		if (goalCounters_.size() == 0) {
			return Goal();
		}

		auto it = goalCache.find(goalCounters_);
		if (it != goalCache.end()) {
			return it->second;
		}

		Goal result;
		int prev_goal = -1;
		for (auto counter : goalCounters_) {
			const Goal &goal = fs_get_goal_definition(counter.goal);
			if (prev_goal != (int)counter.goal) {
				result.mergeIn(goal);
				prev_goal = counter.goal;
			}
		}

		goalCache.insert(goalCounters_, result);
		return result;
	}

	// This method should be called when a chunk is removed
	void freeStats() {
		count--;
		removeFromStats();
	}

	// Updates statistics of all chunks
	void updateStats(bool remove_from_stats = true) {
		int oldAllMissingParts = allMissingParts_;

		if (remove_from_stats) {
			removeFromStats();
		}

		Goal g = getGoal();

		ChunkCopiesCalculator all(g);

		for (const auto &part : parts) {
			if (!part.is_valid()) {
				continue;
			}
			all.addPart(part.type, csdb_find(part.csid)->label);
		}

		all.optimize();

		allFullCopies_ = std::min(kMaxStatCount, all.getFullCopiesCount());
		allAvailabilityState_ = all.getState();
		allMissingParts_ = std::min(kMaxStatCount, all.countPartsToRecover());
		allRedundantParts_ = std::min(kMaxStatCount, all.countPartsToRemove());
		copiesInStats_ = std::min(kMaxStatCount, ChunkCopiesCalculator::getFullCopiesCount(g));

		/* Enqueue a chunk as endangered only if:
		 * 1. Endangered chunks prioritization is on (limit > 0)
		 * 2. Limit of endangered chunks in queue is not reached
		 * 3. Chunk has more missing parts than it used to
		 * 4. Chunk is endangered
		 * 5. It is not already in queue
		 * By checking conditions below we assert no repetitions in endangered queue. */
		if (gEndangeredChunksServingLimit > 0
				&& endangeredChunks.size() < gEndangeredChunksMaxCapacity
				&& allMissingParts_ > oldAllMissingParts
				&& allAvailabilityState_ == ChunksAvailabilityState::kEndangered
				&& !inEndangeredQueue) {
			inEndangeredQueue = 1;
			endangeredChunks.push_back(this);
		}

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

	bool isWritable() {
		return !isLost();
	}

	int countMissingParts() const {
		return allMissingParts_;
	}

	bool countRedundantParts() const {
		return allRedundantParts_;
	}

	uint8_t getFullCopiesCount() const {
		return allFullCopies_;
	}

	bool isLocked() const {
		return lockedto >= eventloop_time();
	}

	void markCopyAsHavingWrongVersion(ChunkPart &part) {
		part.state = ChunkPart::INVALID;
		updateStats();
	}

	void invalidateCopy(ChunkPart &part) {
		part.state = ChunkPart::INVALID;
		part.version = 0;
		updateStats();
	}

	void deleteCopy(ChunkPart &part) {
		part.state = ChunkPart::DEL;
		updateStats();
	}

private:
	ChunksAvailabilityState::State allCopiesState() const {
		return static_cast<ChunksAvailabilityState::State>(allAvailabilityState_);
	}

	void removeFromStats() {
		int prev_goal = -1;
		for (const auto& counter : goalCounters_) {
			if (prev_goal == (int)counter.goal) {
				continue;
			}
			prev_goal = counter.goal;
			allChunksAvailability.removeChunk(counter.goal, allCopiesState());
			allChunksReplicationState.removeChunk(counter.goal, allMissingParts_, allRedundantParts_);
		}

		uint8_t limitedGoal = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, copiesInStats_);
		uint8_t limitedAll = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, allFullCopies_);
		allFullChunkCopies[limitedGoal][limitedAll]--;
	}

	void addToStats() {
		int prev_goal = -1;
		for (const auto& counter : goalCounters_) {
			if (prev_goal == (int)counter.goal) {
				continue;
			}
			prev_goal = counter.goal;
			allChunksAvailability.addChunk(counter.goal, allCopiesState());
			allChunksReplicationState.addChunk(counter.goal, allMissingParts_, allRedundantParts_);
		}

		uint8_t limitedGoal = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, copiesInStats_);
		uint8_t limitedAll = std::min<uint8_t>(CHUNK_MATRIX_SIZE - 1, allFullCopies_);
		allFullChunkCopies[limitedGoal][limitedAll]++;
	}
#endif
};

constexpr int Chunk::kMaxStatCount;

#ifndef METARESTORE

std::deque<Chunk *> Chunk::endangeredChunks;
GoalCache Chunk::goalCache(10000);
ChunksAvailabilityState Chunk::allChunksAvailability;
ChunksReplicationState Chunk::allChunksReplicationState;
uint64_t Chunk::count;
uint64_t Chunk::allFullChunkCopies[CHUNK_MATRIX_SIZE][CHUNK_MATRIX_SIZE];
#endif

#define CHUNK_BUCKET_SIZE 20000
struct chunk_bucket {
	Chunk bucket[CHUNK_BUCKET_SIZE];
	uint32_t firstfree;
	chunk_bucket *next;
};

namespace {
struct ChunksMetadata {
	// chunks
	chunk_bucket *cbhead;
	Chunk *chfreehead;
	Chunk *chunkhash[HASHSIZE];
	uint64_t lastchunkid;
	Chunk *lastchunkptr;

	// other chunks metadata information
	uint64_t nextchunkid;
	uint64_t chunksChecksum;
	uint64_t chunksChecksumRecalculated;
	uint32_t checksumRecalculationPosition;

	ChunksMetadata() :
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
		chunk_bucket *cbn;
		for (chunk_bucket *cb = cbhead; cb; cb = cbn) {
			cbn = cb->next;
			delete cb;
		}
	}
};
} // anonymous namespace

static ChunksMetadata *gChunksMetadata;

#define LOCKTIMEOUT 120
#define UNUSED_DELETE_TIMEOUT (86400*7)

#ifndef METARESTORE

static Chunk *gCurrentChunkInZombieLoop = nullptr;

class ReplicationDelayInfo {
public:
	ReplicationDelayInfo()
		: disconnectedServers_(0),
		  timestamp_() {}

	void serverDisconnected() {
		refresh();
		++disconnectedServers_;
		timestamp_ = eventloop_time() + gOperationsDelayDisconnect;
	}

	void serverConnected() {
		refresh();
		if (disconnectedServers_ > 0) {
			--disconnectedServers_;
		}
	}

	bool replicationAllowed(int missingCopies) {
		refresh();
		return missingCopies > disconnectedServers_;
	}

private:
	uint16_t disconnectedServers_;
	uint32_t timestamp_;

	void refresh() {
		if (eventloop_time() > timestamp_) {
			disconnectedServers_ = 0;
		}
	}

};

/*
 * Information about recently disconnected and connected servers
 * necessary for replication to unlabeled servers.
 */
static ReplicationDelayInfo replicationDelayInfoForAll;

/*
 * Information about recently disconnected and connected servers
 * necessary for replication to servers with specified label.
 */
static std::unordered_map<MediaLabel, ReplicationDelayInfo, MediaLabel::hash> replicationDelayInfoForLabel;

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

static uint64_t chunk_checksum(const Chunk *c) {
	if (c == nullptr || c->fileCount() == 0) {
		// We treat chunks with fileCount=0 as non-existent, so that we don't have to notify shadow
		// masters when we remove them from our structures.
		return 0;
	}
	uint64_t checksum = 64517419147637ULL;
	// Only highest id goal is taken into checksum for compatibility reasons
	hashCombine(checksum, c->chunkid, c->version, c->lockedto, c->highestIdGoal(), c->fileCount());

	return checksum;
}

static void chunk_checksum_add_to_background(Chunk *ch) {
	if (!ch) {
		return;
	}
	removeFromChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
	ch->checksum = chunk_checksum(ch);
	addToChecksum(gChunksMetadata->chunksChecksumRecalculated, ch->checksum);
	addToChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
}

static void chunk_update_checksum(Chunk *ch) {
	if (!ch) {
		return;
	}
	if (HASHPOS(ch->chunkid) < gChunksMetadata->checksumRecalculationPosition) {
		removeFromChecksum(gChunksMetadata->chunksChecksumRecalculated, ch->checksum);
	}
	removeFromChecksum(gChunksMetadata->chunksChecksum, ch->checksum);
	ch->checksum = chunk_checksum(ch);
	if (HASHPOS(ch->chunkid) < gChunksMetadata->checksumRecalculationPosition) {
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.changing_recalculated_chunk");
		addToChecksum(gChunksMetadata->chunksChecksumRecalculated, ch->checksum);
	} else {
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.changing_not_recalculated_chunk");
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
		Chunk *c;
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
		lzfs_pretty_syslog(LOG_WARNING,"Chunks metadata checksum mismatch found, replacing with a new value.");
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.mismatch");
		gChunksMetadata->chunksChecksum = gChunksMetadata->chunksChecksumRecalculated;
	}
	return ChecksumRecalculationStatus::kDone;
}

static void chunk_recalculate_checksum() {
	gChunksMetadata->chunksChecksum = CHECKSUMSEED;
	for (int i = 0; i < HASHSIZE; ++i) {
		for (Chunk *ch = gChunksMetadata->chunkhash[i]; ch; ch = ch->next) {
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

static inline Chunk *chunk_malloc() {
	chunk_bucket *cb;
	Chunk *ret;
	if (gChunksMetadata->chfreehead) {
		ret = gChunksMetadata->chfreehead;
		gChunksMetadata->chfreehead = ret->next;
		ret->clear();
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
	ret->clear();
	return ret;
}

#ifndef METARESTORE
static inline void chunk_free(Chunk *p) {
	p->next = gChunksMetadata->chfreehead;
	gChunksMetadata->chfreehead = p;
	p->inEndangeredQueue = 0;
}
#endif /* METARESTORE */

Chunk *chunk_new(uint64_t chunkid, uint32_t chunkversion) {
	uint32_t chunkpos = HASHPOS(chunkid);
	Chunk *newchunk;
	newchunk = chunk_malloc();
	newchunk->next = gChunksMetadata->chunkhash[chunkpos];
	gChunksMetadata->chunkhash[chunkpos] = newchunk;
	newchunk->chunkid = chunkid;
	newchunk->version = chunkversion;
	gChunksMetadata->lastchunkid = chunkid;
	gChunksMetadata->lastchunkptr = newchunk;
	chunk_update_checksum(newchunk);
	return newchunk;
}

#ifndef METARESTORE
void chunk_emergency_increase_version(Chunk *c) {
	assert(c->isWritable());
	uint32_t i = 0;
	for (auto &part : c->parts) {
		if (part.is_valid()) {
			if (!part.is_busy()) {
				part.mark_busy();
			}
			part.version = c->version+1;
			matocsserv_send_setchunkversion(part.server(),c->chunkid,c->version+1,c->version,
					part.type);
			i++;
		}
	}
	c->interrupted = 0;
	c->operation = Chunk::SET_VERSION;
	c->version++;
	chunk_update_checksum(c);
	fs_incversion(c->chunkid);
}

void chunk_handle_disconnected_copies(Chunk *c) {
	auto it = std::remove_if(c->parts.begin(), c->parts.end(), [](const ChunkPart &part) {
		return csdb_find(part.csid)->eptr == nullptr;
	});
	bool lost_copy_found = it != c->parts.end();

	if (lost_copy_found) {
		c->parts.erase(it, c->parts.end());
		c->needverincrease = 1;
		c->updateStats();
	}

	if (lost_copy_found && c->operation != Chunk::NONE) {
		bool any_copy_busy = std::any_of(c->parts.begin(), c->parts.end(), [](const ChunkPart &part) {
			return part.is_busy();
		});
		if (any_copy_busy) {
			c->interrupted = 1;
		} else {
			if (c->isWritable()) {
				chunk_emergency_increase_version(c);
			} else {
				matoclserv_chunk_status(c->chunkid,LIZARDFS_ERROR_NOTDONE);
				c->operation = Chunk::NONE;
			}
		}
	}
}
#endif

Chunk *chunk_find(uint64_t chunkid) {
	uint32_t chunkpos = HASHPOS(chunkid);
	Chunk *chunkit;
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
void chunk_delete(Chunk *c) {
	if (gChunksMetadata->lastchunkptr==c) {
		gChunksMetadata->lastchunkid=0;
		gChunksMetadata->lastchunkptr=NULL;
	}
	c->freeStats();
	chunk_free(c);
}

uint32_t chunk_count(void) {
	return Chunk::count;
}

void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regularvalidcopies) {
	*allchunks = Chunk::count;
	*allcopies = 0;
	*regularvalidcopies = 0;
	for (int actualCopies = 1; actualCopies < CHUNK_MATRIX_SIZE; actualCopies++) {
		uint32_t ag = 0;
		for (int expectedCopies = 0; expectedCopies < CHUNK_MATRIX_SIZE; expectedCopies++) {
			ag += Chunk::allFullChunkCopies[expectedCopies][actualCopies];
		}
		*allcopies += ag * actualCopies;
	}
}

uint32_t chunk_get_missing_count(void) {
	uint32_t res = 0;
	for (uint8_t goal = GoalId::kMin; goal <= GoalId::kMax; ++goal) {
		res += Chunk::allChunksAvailability.lostChunks(goal);
	}
	return res;
}

void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid) {
	if (matrixid == MATRIX_ALL_COPIES) {
		for (int i = 0; i < CHUNK_MATRIX_SIZE; i++) {
			for (int j = 0; j < CHUNK_MATRIX_SIZE; j++) {
				put32bit(&buff, Chunk::allFullChunkCopies[i][j]);
			}
		}
	} else {
		memset(buff, 0, CHUNK_MATRIX_SIZE * CHUNK_MATRIX_SIZE * sizeof(uint32_t));
	}
}
#endif

/// updates chunk's goal after a file goal has been changed
int chunk_change_file(uint64_t chunkid,uint8_t prevgoal,uint8_t newgoal) {
	Chunk *c;
	if (prevgoal==newgoal) {
		return LIZARDFS_STATUS_OK;
	}
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	try {
		c->changeFileGoal(prevgoal, newgoal);
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "chunk_change_file: %s", ex.what());
		return LIZARDFS_ERROR_CHUNKLOST;
	}
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}

/// updates chunk's goal after a file with goal `goal' has been removed
static inline int chunk_delete_file_int(Chunk *c, uint8_t goal) {
	try {
		c->removeFileWithGoal(goal);
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "chunk_delete_file_int: %s", ex.what());
		return LIZARDFS_ERROR_CHUNKLOST;
	}
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}

/// updates chunk's goal after a file with goal `goal' has been added
static inline int chunk_add_file_int(Chunk *c, uint8_t goal) {
	try {
		c->addFileWithGoal(goal);
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "chunk_add_file_int: %s", ex.what());
		return LIZARDFS_ERROR_CHUNKLOST;
	}
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}

int chunk_delete_file(uint64_t chunkid,uint8_t goal) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	return chunk_delete_file_int(c,goal);
}

int chunk_add_file(uint64_t chunkid,uint8_t goal) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	return chunk_add_file_int(c,goal);
}

int chunk_can_unlock(uint64_t chunkid, uint32_t lockid) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (lockid == 0) {
		// lockid == 0 -> force unlock
		return LIZARDFS_STATUS_OK;
	}
	// We will let client unlock the chunk even if c->lockedto < eventloop_time()
	// if he provides lockId that was used to lock the chunk -- this means that nobody
	// else used this chunk since it was locked (operations like truncate or replicate
	// would remove such a stale lock before modifying the chunk)
	if (c->lockid == lockid) {
		return LIZARDFS_STATUS_OK;
	} else if (c->lockedto == 0) {
		return LIZARDFS_ERROR_NOTLOCKED;
	} else {
		return LIZARDFS_ERROR_WRONGLOCKID;
	}
}

int chunk_unlock(uint64_t chunkid) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	// Don't remove lockid to safely accept retransmission of FUSE_CHUNK_UNLOCK message
	c->lockedto = 0;
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE

int chunk_invalidate_goal_cache(){
	Chunk::goalCache.clear();
	return LIZARDFS_STATUS_OK;
}

bool chunk_has_only_invalid_copies(uint64_t chunkid) {
	if (chunkid == 0) {
		return false;
	}
	Chunk *c = chunk_find(chunkid);
	if (c == NULL || !c->isLost()) {
		return false;
	}
	// Chunk is lost, so it can only have INVALID or DEL copies.
	// Return true it there is at least one INVALID.
	return std::any_of(c->parts.begin(), c->parts.end(), [](const ChunkPart& part) {
		return part.state == ChunkPart::INVALID;
	});
}

int chunk_get_fullcopies(uint64_t chunkid,uint8_t *vcopies) {
	Chunk *c;
	*vcopies = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}

	*vcopies = c->getFullCopiesCount();

	return LIZARDFS_STATUS_OK;
}

int chunk_get_partstomodify(uint64_t chunkid, int &recover, int &remove) {
	Chunk *c;
	recover = 0;
	remove = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	recover = c->countMissingParts();
	remove = c->countRedundantParts();
	return LIZARDFS_STATUS_OK;
}

uint8_t chunk_multi_modify(uint64_t ochunkid, uint32_t *lockid, uint8_t goal,
		bool usedummylockid, bool quota_exceeded, uint8_t *opflag, uint64_t *nchunkid,
		uint32_t min_server_version = 0) {
	Chunk *c = NULL;
	if (ochunkid == 0) { // new chunk
		if (quota_exceeded) {
			return LIZARDFS_ERROR_QUOTA;
		}
		auto serversWithChunkTypes = matocsserv_getservers_for_new_chunk(goal, min_server_version);
		if (serversWithChunkTypes.empty()) {
			uint16_t uscount,tscount;
			double minusage,maxusage;
			matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);
			if ((uscount > 0) && (eventloop_time() > (starttime+600))) { // if there are chunkservers and it's at least one minute after start then it means that there is no space left
				return LIZARDFS_ERROR_NOSPACE;
			} else {
				return LIZARDFS_ERROR_NOCHUNKSERVERS;
			}
		}
		ChunkCopiesCalculator calculator(fs_get_goal_definition(goal));
		for (const auto &server_with_type : serversWithChunkTypes) {
			calculator.addPart(server_with_type.second, MediaLabel::kWildcard);
		}
		calculator.evalRedundancyLevel();
		if (!calculator.isSafeEnoughToWrite(gRedundancyLevel)) {
			return LIZARDFS_ERROR_NOCHUNKSERVERS;
		}
		c = chunk_new(gChunksMetadata->nextchunkid++, 1);
		c->interrupted = 0;
		c->operation = Chunk::CREATE;
		chunk_add_file_int(c,goal);
		for (const auto &server_with_type : serversWithChunkTypes) {
			c->parts.push_back(ChunkPart(matocsserv_get_csdb(server_with_type.first)->csid,
			                             ChunkPart::BUSY, c->version, server_with_type.second));
			matocsserv_send_createchunk(server_with_type.first, c->chunkid, server_with_type.second,
			                            c->version);
		}
		c->updateStats();
		*opflag=1;
		*nchunkid = c->chunkid;
	} else {
		Chunk *oc = chunk_find(ochunkid);
		if (oc==NULL) {
			return LIZARDFS_ERROR_NOCHUNK;
		}
		if (*lockid != 0 && *lockid != oc->lockid) {
			if (oc->lockid == 0 || oc->lockedto == 0) {
				// Lock was removed by some chunk operation or by a different client
				return LIZARDFS_ERROR_NOTLOCKED;
			} else {
				return LIZARDFS_ERROR_WRONGLOCKID;
			}
		}
		if (*lockid == 0 && oc->isLocked()) {
			return LIZARDFS_ERROR_LOCKED;
		}
		if (!oc->isWritable()) {
			return LIZARDFS_ERROR_CHUNKLOST;
		}
		ChunkCopiesCalculator calculator(oc->getGoal());
		for (auto &part : oc->parts) {
			calculator.addPart(part.type, MediaLabel::kWildcard);
		}
		calculator.evalRedundancyLevel();
		if (!calculator.isSafeEnoughToWrite(gRedundancyLevel)) {
			return LIZARDFS_ERROR_NOCHUNKSERVERS;
		}

		if (oc->fileCount() == 1) { // refcount==1
			*nchunkid = ochunkid;
			c = oc;
			if (c->operation != Chunk::NONE) {
				return LIZARDFS_ERROR_CHUNKBUSY;
			}
			if (c->needverincrease) {
				assert(c->isWritable());
				for (auto &part : c->parts) {
					if (part.is_valid()) {
						if (!part.is_busy()) {
							part.mark_busy();
						}
						part.version = c->version+1;
						matocsserv_send_setchunkversion(part.server(), ochunkid, c->version+1, c->version,
								part.type);
					}
				}
				c->interrupted = 0;
				c->operation = Chunk::SET_VERSION;
				c->version++;
				*opflag=1;
			} else {
				*opflag=0;
			}
		} else {
			if (oc->fileCount() == 0) { // it's serious structure error
				lzfs_pretty_syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016" PRIX64 ")",ochunkid);
				return LIZARDFS_ERROR_CHUNKLOST; // ERROR_STRUCTURE
			}
			if (quota_exceeded) {
				return LIZARDFS_ERROR_QUOTA;
			}
			assert(oc->isWritable());
			c = chunk_new(gChunksMetadata->nextchunkid++, 1);
			c->interrupted = 0;
			c->operation = Chunk::DUPLICATE;
			chunk_delete_file_int(oc,goal);
			chunk_add_file_int(c,goal);
			for (const auto &old_part : oc->parts) {
				if (old_part.is_valid()) {
					c->parts.push_back(ChunkPart(old_part.csid, ChunkPart::BUSY, c->version, old_part.type));
					matocsserv_send_duplicatechunk(old_part.server(), c->chunkid, c->version, old_part.type,
							oc->chunkid, oc->version);
				}
			}
			c->updateStats();
			*nchunkid = c->chunkid;
			*opflag=1;
		}
	}

	c->lockedto = eventloop_time() + LOCKTIMEOUT;
	if (*lockid == 0) {
		if (usedummylockid) {
			*lockid = 1;
		} else {
			*lockid = 2 + rnd_ranged<uint32_t>(0xFFFFFFF0); // some random number greater than 1
		}
	}
	c->lockid = *lockid;
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}

uint8_t chunk_multi_truncate(uint64_t ochunkid, uint32_t lockid, uint32_t length,
		uint8_t goal, bool denyTruncatingParityParts, bool quota_exceeded, uint64_t *nchunkid) {
	Chunk *oc, *c;

	c=NULL;
	oc = chunk_find(ochunkid);
	if (oc==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (!oc->isWritable()) {
		return LIZARDFS_ERROR_CHUNKLOST;
	}
	if (oc->isLocked() && (lockid == 0 || lockid != oc->lockid)) {
		return LIZARDFS_ERROR_LOCKED;
	}
	if (denyTruncatingParityParts) {
		for (const auto &part : oc->parts) {
			if (slice_traits::isParityPart(part.type)) {
				return LIZARDFS_ERROR_NOTPOSSIBLE;
			}
		}
	}
	if (oc->fileCount() == 1) { // refcount==1
		*nchunkid = ochunkid;
		c = oc;
		if (c->operation != Chunk::NONE) {
			return LIZARDFS_ERROR_CHUNKBUSY;
		}
		assert(c->isWritable());
		for (auto &part : c->parts) {
			if (part.is_valid()) {
				if (!part.is_busy()) {
					part.mark_busy();
				}
				part.version = c->version+1;
				uint32_t chunkTypeLength =
						slice_traits::chunkLengthToChunkPartLength(part.type, length);
				matocsserv_send_truncatechunk(part.server(), ochunkid, part.type, chunkTypeLength,
						c->version + 1, c->version);
			}
		}
		c->interrupted = 0;
		c->operation = Chunk::TRUNCATE;
		c->version++;
	} else {
		if (oc->fileCount() == 0) { // it's serious structure error
			lzfs_pretty_syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016" PRIX64 ")",ochunkid);
			return LIZARDFS_ERROR_CHUNKLOST; // ERROR_STRUCTURE
		}
		if (quota_exceeded) {
			return LIZARDFS_ERROR_QUOTA;
		}

		assert(oc->isWritable());
		c = chunk_new(gChunksMetadata->nextchunkid++, 1);
		c->interrupted = 0;
		c->operation = Chunk::DUPTRUNC;
		chunk_delete_file_int(oc,goal);
		chunk_add_file_int(c,goal);
		for (const auto &old_part : oc->parts) {
			if (old_part.is_valid()) {
				c->parts.push_back(ChunkPart(old_part.csid, ChunkPart::BUSY, c->version, old_part.type));
				matocsserv_send_duptruncchunk(old_part.server(), c->chunkid, c->version,
						old_part.type, oc->chunkid, oc->version,
						slice_traits::chunkLengthToChunkPartLength(old_part.type, length));
			}
		}
		c->updateStats();
		*nchunkid = c->chunkid;
	}

	c->lockedto=(uint32_t)eventloop_time()+LOCKTIMEOUT;
	c->lockid = lockid;
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}
#endif // ! METARESTORE

uint8_t chunk_apply_modification(uint32_t ts, uint64_t oldChunkId, uint32_t lockid, uint8_t goal,
		bool doIncreaseVersion, uint64_t *newChunkId) {
	Chunk *c;
	if (oldChunkId == 0) { // new chunk
		c = chunk_new(gChunksMetadata->nextchunkid++, 1);
		chunk_add_file_int(c, goal);
	} else {
		Chunk *oc = chunk_find(oldChunkId);
		if (oc == NULL) {
			return LIZARDFS_ERROR_NOCHUNK;
		}
		if (oc->fileCount() == 0) { // refcount == 0
			lzfs_pretty_syslog(LOG_WARNING,
					"serious structure inconsistency: (chunkid:%016" PRIX64 ")", oldChunkId);
			return LIZARDFS_ERROR_CHUNKLOST; // ERROR_STRUCTURE
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
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
int chunk_repair(uint8_t goal, uint64_t ochunkid, uint32_t *nversion, uint8_t correct_only) {
	uint32_t best_version;
	Chunk *c;

	*nversion=0;
	if (ochunkid==0) {
		return 0; // not changed
	}

	c = chunk_find(ochunkid);
	if (c==NULL) { // no such chunk - erase (nchunkid already is 0 - so just return with "changed" status)
		if (correct_only == 1) { // don't erase if correct only flag is set
			return 0;
		} else {
			return 1;
		}
	}
	if (c->isLocked()) { // can't repair locked chunks - but if it's locked, then likely it doesn't need to be repaired
		return 0;
	}

	// calculators will be sorted by decreasing keys, so highest version will be first.
	std::map<uint32_t, ChunkCopiesCalculator, std::greater<uint32_t>> calculators;
	best_version = 0;
	for (const auto &part : c->parts) {
		// ignore chunks which are being deleted
		if (part.state != ChunkPart::DEL) {
			ChunkCopiesCalculator &calculator = calculators[part.version];
			calculator.addPart(part.type, matocsserv_get_label(part.server()));
		}
	}
	// find best version which can be recovered
	// calculators are sorted by decreasing keys, so highest version will be first.
	for (auto &version_and_calculator : calculators) {
		uint32_t version = version_and_calculator.first;
		ChunkCopiesCalculator &calculator = version_and_calculator.second;
		calculator.optimize();
		// calculator.isRecoveryPossible() won't work below, because target goal is empty.
		if (calculator.getFullCopiesCount() > 0) {
			best_version = version;
			break;
		}
	}
	// current version is readable
	if (best_version == c->version) {
		return 0;
	}
	// didn't find sensible chunk
	if (best_version == 0) {
		if (correct_only == 1) { // don't erase if correct only flag is set
			return 0;
		} else {                  // otherwise erase it
			chunk_delete_file_int(c, goal);
			return 1;
		}
	}
	// found previous version which is readable
	c->version = best_version;
	for (auto &part : c->parts) {
		if (part.state == ChunkPart::INVALID && part.version==best_version) {
			part.state = ChunkPart::VALID;
		}
	}
	*nversion = best_version;
	c->needverincrease=1;
	c->updateStats();
	chunk_update_checksum(c);
	return 1;
}
#endif

int chunk_set_version(uint64_t chunkid,uint32_t version) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	c->version = version;
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}

int chunk_increase_version(uint64_t chunkid) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	c->version++;
	chunk_update_checksum(c);
	return LIZARDFS_STATUS_OK;
}

uint8_t chunk_set_next_chunkid(uint64_t nextChunkIdToBeSet) {
	if (nextChunkIdToBeSet >= gChunksMetadata->nextchunkid) {
		gChunksMetadata->nextchunkid = nextChunkIdToBeSet;
		return LIZARDFS_STATUS_OK;
	} else {
		lzfs_pretty_syslog(LOG_WARNING,"was asked to increase the next chunk id to %" PRIu64 ", but it was"
				"already set to a bigger value %" PRIu64 ". Ignoring.",
				nextChunkIdToBeSet, gChunksMetadata->nextchunkid);
		return LIZARDFS_ERROR_MISMATCH;
	}
}

#ifndef METARESTORE

const ChunksReplicationState& chunk_get_replication_state() {
	return Chunk::allChunksReplicationState;
}

const ChunksAvailabilityState& chunk_get_availability_state() {
	return Chunk::allChunksAvailability;
}

struct ChunkLocation {
	ChunkLocation() : chunkType(slice_traits::standard::ChunkPartType()),
			chunkserver_version(0), distance(0), random(0) {
	}
	NetworkAddress address;
	ChunkPartType chunkType;
	uint32_t chunkserver_version;
	uint32_t distance;
	uint32_t random;
	MediaLabel label;
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

// TODO deduplicate
int chunk_getversionandlocations(uint64_t chunkid, uint32_t currentIp, uint32_t& version,
		uint32_t maxNumberOfChunkCopies, std::vector<ChunkTypeWithAddress>& serversList) {
	Chunk *c;
	uint8_t cnt;

	sassert(serversList.empty());
	c = chunk_find(chunkid);

	if (c == NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	version = c->version;
	cnt = 0;
	std::vector<ChunkLocation> chunkLocation;
	ChunkLocation chunkserverLocation;
	for (const auto &part : c->parts) {
		if (part.is_valid()) {
			if (cnt < maxNumberOfChunkCopies && matocsserv_getlocation(part.server(),
					&(chunkserverLocation.address.ip),
					&(chunkserverLocation.address.port),
					&(chunkserverLocation.label)) == 0) {
				chunkserverLocation.chunkType = part.type;
				chunkserverLocation.chunkserver_version = matocsserv_get_version(part.server());
				chunkserverLocation.distance =
						topology_distance(chunkserverLocation.address.ip, currentIp);
						// in the future prepare more sophisticated distance function
				chunkserverLocation.random = rnd<uint32_t>();
				chunkLocation.push_back(chunkserverLocation);
				cnt++;
			}
		}
	}
	std::sort(chunkLocation.begin(), chunkLocation.end());
	for (uint32_t i = 0; i < chunkLocation.size(); ++i) {
		const ChunkLocation& loc = chunkLocation[i];
		serversList.emplace_back(loc.address, loc.chunkType, loc.chunkserver_version);
	}
	return LIZARDFS_STATUS_OK;
}

int chunk_getversionandlocations(uint64_t chunkid, uint32_t currentIp, uint32_t& version,
		uint32_t maxNumberOfChunkCopies, std::vector<ChunkPartWithAddressAndLabel>& serversList) {
	Chunk *c;
	uint8_t cnt;

	sassert(serversList.empty());
	c = chunk_find(chunkid);

	if (c == NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	version = c->version;
	cnt = 0;
	std::vector<ChunkLocation> chunkLocation;
	ChunkLocation chunkserverLocation;
	for (const auto &part : c->parts) {
		if (part.is_valid()) {
			if (cnt < maxNumberOfChunkCopies && matocsserv_getlocation(part.server(),
					&(chunkserverLocation.address.ip),
					&(chunkserverLocation.address.port),
					&(chunkserverLocation.label)) == 0) {
				chunkserverLocation.chunkType = part.type;
				chunkserverLocation.distance =
						topology_distance(chunkserverLocation.address.ip, currentIp);
						// in the future prepare more sophisticated distance function
				chunkserverLocation.random = rnd<uint32_t>();
				chunkLocation.push_back(chunkserverLocation);
				cnt++;
			}
		}
	}
	std::sort(chunkLocation.begin(), chunkLocation.end());
	for (uint32_t i = 0; i < chunkLocation.size(); ++i) {
		const ChunkLocation& loc = chunkLocation[i];
		serversList.emplace_back(loc.address, static_cast<std::string>(loc.label), loc.chunkType);
	}
	return LIZARDFS_STATUS_OK;
}

void chunk_server_has_chunk(matocsserventry *ptr, uint64_t chunkid, uint32_t version, ChunkPartType chunkType) {
	Chunk *c;
	const uint32_t new_version = version & 0x7FFFFFFF;
	const bool todel = version & 0x80000000;
	c = chunk_find(chunkid);
	if (c==NULL) {
		// chunkserver has nonexistent chunk, so create it for future deletion
		if (chunkid>=gChunksMetadata->nextchunkid) {
			fs_set_nextchunkid(FsContext::getForMaster(eventloop_time()), chunkid + 1);
		}
		c = chunk_new(chunkid, new_version);
		c->lockedto = (uint32_t)eventloop_time()+UNUSED_DELETE_TIMEOUT;
		c->lockid = 0;
		chunk_update_checksum(c);
	}
	auto server_csid = matocsserv_get_csdb(ptr)->csid;
	for (auto &part : c->parts) {
		if (part.csid == server_csid && part.type == chunkType) {
			// This server already notified us about its copy.
			// We normally don't get repeated notifications about the same copy, but
			// they can arrive after chunkserver configuration reload (particularly,
			// when folders change their 'to delete' status) or due to bugs.
			// Let's try to handle them as well as we can.
			switch (part.state) {
			case ChunkPart::DEL:
				// We requested deletion, but the chunkserver 'has' this copy again.
				// Repeat deletion request.
				c->invalidateCopy(part);
				// fallthrough
			case ChunkPart::INVALID:
				// leave this copy alone
				return;
			default:
				break;
			}
			if (part.version != new_version) {
				lzfs_pretty_syslog(LOG_WARNING, "chunk %016" PRIX64 ": master data indicated "
						"version %08" PRIX32 ", chunkserver reports %08"
						PRIX32 "!!! Updating master data.", c->chunkid,
						part.version, new_version);
				part.version = new_version;
			}
			if (part.version != c->version) {
				c->markCopyAsHavingWrongVersion(part);
				return;
			}
			if (!part.is_todel() && todel) {
				part.mark_todel();
				c->updateStats();
			}
			if (part.is_todel() && !todel) {
				part.unmark_todel();
				c->updateStats();
			}
			return;
		}
	}
	const uint8_t state = (new_version == c->version) ? (todel ? ChunkPart::TDVALID : ChunkPart::VALID) : ChunkPart::INVALID;
	c->parts.push_back(ChunkPart(server_csid, state, new_version, chunkType));
	c->updateStats();
}

void chunk_damaged(matocsserventry *ptr, uint64_t chunkid, ChunkPartType chunk_type) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c == NULL) {
		// syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016" PRIX64 "), so create it for future deletion",chunkid);
		if (chunkid >= gChunksMetadata->nextchunkid) {
			gChunksMetadata->nextchunkid = chunkid + 1;
		}
		c = chunk_new(chunkid, 0);
	}
	auto server_csid = matocsserv_get_csdb(ptr)->csid;
	for (auto &part : c->parts) {
		if (part.csid == server_csid && part.type == chunk_type) {
			c->invalidateCopy(part);
			c->needverincrease=1;
			return;
		}
	}
	c->parts.push_back(ChunkPart(server_csid, ChunkPart::INVALID, 0, slice_traits::standard::ChunkPartType()));
	c->updateStats();
	c->needverincrease=1;
}

void chunk_lost(matocsserventry *ptr,uint64_t chunkid, ChunkPartType chunk_type) {
	Chunk *c = chunk_find(chunkid);
	if (c == nullptr) {
		return;
	}
	auto server_csid = matocsserv_get_csdb(ptr)->csid;
	auto it = std::remove_if(c->parts.begin(), c->parts.end(), [server_csid, chunk_type](const ChunkPart &part) {
		return part.csid == server_csid && part.type == chunk_type;
	});
	if (it != c->parts.end()) {
		c->parts.erase(it, c->parts.end());
		c->updateStats();
		c->needverincrease = 1;
	}
}

void chunk_server_disconnected(matocsserventry */*ptr*/, const MediaLabel &label) {
	replicationDelayInfoForAll.serverDisconnected();
	if (label != MediaLabel::kWildcard) {
		replicationDelayInfoForLabel[label].serverDisconnected();
	}
	// If chunkserver disconnects, we can assure it was processed by zombie server loop
	// only if the loop was executed at least twice.
	gDisconnectedCounter = 2;
	eventloop_make_next_poll_nonblocking();
	fs_cs_disconnected();
	gChunksMetadata->lastchunkid = 0;
	gChunksMetadata->lastchunkptr = NULL;
}

void chunk_server_unlabelled_connected() {
	replicationDelayInfoForAll.serverConnected();
}

void chunk_server_label_changed(const MediaLabel &previousLabel, const MediaLabel &newLabel) {
	/*
	 * Only server with no label can be considered as newly connected
	 * and it was added to replicationDelayInfoForAll earlier
	 * in chunk_server_unlabelled_connected call.
	 */
	if (previousLabel == MediaLabel::kWildcard) {
		replicationDelayInfoForLabel[newLabel].serverConnected();
	}
}

/*
 * A function that is called in every main loop iteration, that cleans chunk structs
 */
void chunk_clean_zombie_servers_a_bit() {
	SignalLoopWatchdog watchdog;
	static uint32_t current_position = HASHSIZE;

	if (gDisconnectedCounter == 0) {
		return;
	}

	watchdog.start();
	while (current_position < HASHSIZE) {
		for (; gCurrentChunkInZombieLoop; gCurrentChunkInZombieLoop = gCurrentChunkInZombieLoop->next) {
			chunk_handle_disconnected_copies(gCurrentChunkInZombieLoop);
			if (watchdog.expired()) {
				eventloop_make_next_poll_nonblocking();
				return;
			}
		}
		++current_position;
		if (current_position < HASHSIZE) {
			gCurrentChunkInZombieLoop = gChunksMetadata->chunkhash[current_position];
		}
	}
	if (current_position >= HASHSIZE) {
		--gDisconnectedCounter;
		current_position = 0;
		gCurrentChunkInZombieLoop = gChunksMetadata->chunkhash[0];
	}
	eventloop_make_next_poll_nonblocking();
}

void chunk_got_delete_status(matocsserventry *ptr, uint64_t chunkId, ChunkPartType chunkType, uint8_t /*status*/) {
	Chunk *c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	auto server_csid = matocsserv_get_csdb(ptr)->csid;
	auto it = std::remove_if(c->parts.begin(), c->parts.end(), [server_csid, chunkType](const ChunkPart& part) {
		if (part.csid == server_csid && part.type == chunkType) {
			if (part.state != ChunkPart::DEL) {
				lzfs_pretty_syslog(LOG_WARNING, "got unexpected delete status");
			}
			return true;
		}
		return false;
	});
	if (it != c->parts.end()) {
		c->parts.erase(it, c->parts.end());
		c->updateStats();
	}
}

void chunk_got_replicate_status(matocsserventry *ptr, uint64_t chunkId, uint32_t chunkVersion,
		ChunkPartType chunkType, uint8_t status) {
	Chunk *c = chunk_find(chunkId);
	if (c == NULL || status != 0) {
		return;
	}

	auto server_csid = matocsserv_get_csdb(ptr)->csid;
	for (auto &part : c->parts) {
		if (part.type == chunkType && part.csid == server_csid) {
			lzfs_pretty_syslog(LOG_WARNING,
					"got replication status from server which had had that chunk before (chunk:%016"
					PRIX64 "_%08" PRIX32 ")", chunkId, chunkVersion);
			if (part.state == ChunkPart::VALID && chunkVersion != c->version) {
				part.version = chunkVersion;
				c->markCopyAsHavingWrongVersion(part);
			}
			return;
		}
	}
	const uint8_t state = (c->isLocked() || chunkVersion != c->version) ? ChunkPart::INVALID : ChunkPart::VALID;
	c->parts.push_back(ChunkPart(server_csid, state, chunkVersion, chunkType));
	c->updateStats();
}

void chunk_operation_status(Chunk *c, ChunkPartType chunkType, uint8_t status,matocsserventry *ptr) {
	bool any_copy_busy = false;
	auto server_csid = matocsserv_get_csdb(ptr)->csid;
	for (auto &part : c->parts) {
		if (part.csid == server_csid && part.type == chunkType) {
			if (status!=0) {
				c->interrupted = 1; // increase version after finish, just in case
				c->invalidateCopy(part);
			} else {
				if (part.is_busy()) {
					part.unmark_busy();
				}
			}
		}
		any_copy_busy |= part.is_busy();
	}
	if (!any_copy_busy) {
		if (c->isWritable()) {
			if (c->interrupted) {
				chunk_emergency_increase_version(c);
			} else {
				matoclserv_chunk_status(c->chunkid,LIZARDFS_STATUS_OK);
				c->operation = Chunk::NONE;
				c->needverincrease = 0;
			}
		} else {
			matoclserv_chunk_status(c->chunkid,LIZARDFS_ERROR_NOTDONE);
			c->operation = Chunk::NONE;
		}
	}
}

void chunk_got_create_status(matocsserventry *ptr,uint64_t chunkId, ChunkPartType chunkType, uint8_t status) {
	Chunk *c;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_duplicate_status(matocsserventry *ptr, uint64_t chunkId, ChunkPartType chunkType, uint8_t status) {
	Chunk *c;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_setversion_status(matocsserventry *ptr, uint64_t chunkId, ChunkPartType chunkType, uint8_t status) {
	Chunk *c;
	c = chunk_find(chunkId);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_truncate_status(matocsserventry *ptr, uint64_t chunkid, ChunkPartType chunkType, uint8_t status) {
	Chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c, chunkType, status, ptr);
}

void chunk_got_duptrunc_status(matocsserventry *ptr, uint64_t chunkId, ChunkPartType chunkType, uint8_t status) {
	Chunk *c;
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

class ChunkWorker : public coroutine {
public:
	ChunkWorker();
	void doEveryLoopTasks();
	void doEverySecondTasks();
	void doChunkJobs(Chunk *c, uint16_t serverCount);
	void mainLoop();

private:
	typedef std::vector<ServerWithUsage> ServersWithUsage;

	struct MainLoopStack {
		uint32_t current_bucket;
		uint16_t usable_server_count;
		uint32_t chunks_done_count;
		uint32_t buckets_done_count;
		std::size_t endangered_to_serve;
		Chunk* node;
		Chunk* prev;
		ActiveLoopWatchdog work_limit;
		ActiveLoopWatchdog watchdog;
	};

	bool deleteUnusedChunks();

	uint32_t getMinChunkserverVersion(Chunk *c, ChunkPartType type);
	bool tryReplication(Chunk *c, ChunkPartType type, matocsserventry *destinationServer);

	void deleteInvalidChunkParts(Chunk *c);
	void deleteAllChunkParts(Chunk *c);
	bool replicateChunkPart(Chunk *c, Goal::Slice::Type slice_type, int slice_part, ChunkCopiesCalculator& calc, const IpCounter &ip_counter);
	bool removeUnneededChunkPart(Chunk *c, Goal::Slice::Type slice_type, int slice_part,
	                             ChunkCopiesCalculator& calc, const IpCounter &ip_counter);
	bool rebalanceChunkParts(Chunk *c, ChunkCopiesCalculator& calc, bool only_todel, const IpCounter &ip_counter);
	bool rebalanceChunkPartsWithSameIp(Chunk *c, ChunkCopiesCalculator &calc, const IpCounter &ip_counter);

	loop_info inforec_;
	uint32_t deleteNotDone_;
	uint32_t deleteDone_;
	uint32_t prevToDeleteCount_;
	uint32_t deleteLoopCount_;

	/// All chunkservers sorted by disk usage.
	ServersWithUsage sortedServers_;

	/// For each label, all servers with this label sorted by disk usage.
	std::map<MediaLabel, ServersWithUsage> labeledSortedServers_;

	MainLoopStack stack_;
};

ChunkWorker::ChunkWorker()
		: deleteNotDone_(0),
		  deleteDone_(0),
		  prevToDeleteCount_(0),
		  deleteLoopCount_(0) {
	memset(&inforec_,0,sizeof(loop_info));
	stack_.current_bucket = 0;
}

void ChunkWorker::doEveryLoopTasks() {
	deleteLoopCount_++;
	if (deleteLoopCount_ >= 16) {
		uint32_t toDeleteCount = deleteDone_ + deleteNotDone_;
		deleteLoopCount_ = 0;
		if ((deleteNotDone_ > deleteDone_) && (toDeleteCount > prevToDeleteCount_)) {
			TmpMaxDelFrac *= 1.5;
			if (TmpMaxDelFrac>MaxDelHardLimit) {
				lzfs_pretty_syslog(LOG_NOTICE,"DEL_LIMIT hard limit (%" PRIu32 " per server) reached",MaxDelHardLimit);
				TmpMaxDelFrac=MaxDelHardLimit;
			}
			TmpMaxDel = TmpMaxDelFrac;
			lzfs_pretty_syslog(LOG_NOTICE,"DEL_LIMIT temporary increased to: %" PRIu32 " per server",TmpMaxDel);
		}
		if ((toDeleteCount < prevToDeleteCount_) && (TmpMaxDelFrac > MaxDelSoftLimit)) {
			TmpMaxDelFrac /= 1.5;
			if (TmpMaxDelFrac<MaxDelSoftLimit) {
				lzfs_pretty_syslog(LOG_NOTICE,"DEL_LIMIT back to soft limit (%" PRIu32 " per server)",MaxDelSoftLimit);
				TmpMaxDelFrac = MaxDelSoftLimit;
			}
			TmpMaxDel = TmpMaxDelFrac;
			lzfs_pretty_syslog(LOG_NOTICE,"DEL_LIMIT decreased back to: %" PRIu32 " per server",TmpMaxDel);
		}
		prevToDeleteCount_ = toDeleteCount;
		deleteNotDone_ = 0;
		deleteDone_ = 0;
	}
	chunksinfo = inforec_;
	memset(&inforec_,0,sizeof(inforec_));
	chunksinfo_loopstart = chunksinfo_loopend;
	chunksinfo_loopend = eventloop_time();
}

void ChunkWorker::doEverySecondTasks() {
	sortedServers_ = matocsserv_getservers_sorted();
	labeledSortedServers_.clear();
	for (const ServerWithUsage& sw : sortedServers_) {
		labeledSortedServers_[sw.label].push_back(sw);
	}
}

static bool chunkPresentOnServer(Chunk *c, matocsserventry *server) {
	auto server_csid = matocsserv_get_csdb(server)->csid;
	return std::any_of(c->parts.begin(), c->parts.end(), [server_csid](const ChunkPart &part) {
		return part.csid == server_csid;
	});
}

static bool chunkPresentOnServer(Chunk *c, Goal::Slice::Type slice_type, matocsserventry *server) {
	auto server_csid = matocsserv_get_csdb(server)->csid;
	return std::any_of(c->parts.begin(), c->parts.end(), [server_csid, slice_type](const ChunkPart &part) {
		return part.csid == server_csid && part.type.getSliceType() == slice_type;
	});
}

uint32_t ChunkWorker::getMinChunkserverVersion(Chunk */*c*/, ChunkPartType /*type*/) {
	return kFirstECVersion;
}

bool ChunkWorker::tryReplication(Chunk *c, ChunkPartType part_to_recover,
				matocsserventry *destination_server) {
	// TODO(msulikowski) Prefer VALID over TDVALID copies.
	std::vector<matocsserventry *> standard_servers;
	std::vector<matocsserventry *> all_servers;
	std::vector<ChunkPartType> all_parts;
	ChunkCopiesCalculator calc(c->getGoal());

	uint32_t destination_version = matocsserv_get_version(destination_server);

	assert(destination_version >= getMinChunkserverVersion(c, part_to_recover));
	for (const auto &part : c->parts) {
		if (!part.is_valid() || part.is_busy() || matocsserv_replication_read_counter(part.server()) >= MaxReadRepl) {
			continue;
		}

		if (slice_traits::isStandard(part.type)) {
			standard_servers.push_back(part.server());
		}

		if (destination_version >= kFirstXorVersion && destination_version < kFirstECVersion
			&& slice_traits::isXor(part_to_recover) && matocsserv_get_version(part.server()) < kFirstXorVersion) {
			continue;
		}

		if (destination_version < kFirstXorVersion && !slice_traits::isStandard(part.type)) {
			continue;
		}

		all_servers.push_back(part.server());
		all_parts.push_back(part.type);
		calc.addPart(part.type, matocsserv_get_label(part.server()));
	}

	calc.evalRedundancyLevel();
	if (!calc.isRecoveryPossible()) {
		return false;
	}

	if (destination_version >= kFirstECVersion ||
	    (destination_version >= kFirstXorVersion && slice_traits::isXor(part_to_recover))) {
		matocsserv_send_liz_replicatechunk(destination_server, c->chunkid, c->version,
		                                   part_to_recover, all_servers,
		                                   all_parts);
		stats_replications++;
		c->needverincrease = 1;
		return true;
	}

	// fall back to legacy replication
	assert(slice_traits::isStandard(part_to_recover));

	if (standard_servers.empty()) {
		return false;
	}

	matocsserv_send_replicatechunk(destination_server, c->chunkid, c->version,
	                               standard_servers[rnd_ranged<uint32_t>(standard_servers.size())]);

	stats_replications++;
	c->needverincrease = 1;
	return true;
}

void ChunkWorker::deleteInvalidChunkParts(Chunk *c) {
	for (auto &part : c->parts) {
		if (matocsserv_deletion_counter(part.server()) < TmpMaxDel) {
			if (!part.is_valid()) {
				if (part.state == ChunkPart::DEL) {
					lzfs_pretty_syslog(LOG_WARNING,
					       "chunk hasn't been deleted since previous loop - "
					       "retry");
				}
				part.state = ChunkPart::DEL;
				stats_deletions++;
				matocsserv_send_deletechunk(part.server(), c->chunkid, 0, part.type);
				inforec_.done.del_invalid++;
				deleteDone_++;
			}
		} else {
			if (part.state == ChunkPart::INVALID) {
				inforec_.notdone.del_invalid++;
				deleteNotDone_++;
			}
		}
	}
}

void ChunkWorker::deleteAllChunkParts(Chunk *c) {
	for (auto &part : c->parts) {
		if (matocsserv_deletion_counter(part.server()) < TmpMaxDel) {
			if (part.is_valid() && !part.is_busy()) {
				c->deleteCopy(part);
				c->needverincrease = 1;
				stats_deletions++;
				matocsserv_send_deletechunk(part.server(), c->chunkid, c->version,
				                            part.type);
				inforec_.done.del_unused++;
				deleteDone_++;
			}
		} else {
			if (part.state == ChunkPart::VALID || part.state == ChunkPart::TDVALID) {
				inforec_.notdone.del_unused++;
				deleteNotDone_++;
			}
		}
	}
}

bool ChunkWorker::replicateChunkPart(Chunk *c, Goal::Slice::Type slice_type, int slice_part,
					ChunkCopiesCalculator &calc, const IpCounter &ip_counter) {
	std::vector<matocsserventry *> servers;
	int skipped_replications = 0, valid_parts_count = 0, expected_copies = 0;
	bool tried_to_replicate = false;
	Goal::Slice::Labels replicate_labels;

	replicate_labels = calc.getLabelsToRecover(slice_type, slice_part);

	if (calc.getAvailable().find(slice_type) != calc.getAvailable().end()) {
		valid_parts_count =
		        Goal::Slice::countLabels(calc.getAvailable()[slice_type][slice_part]);
	}

	expected_copies = Goal::Slice::countLabels(calc.getTarget()[slice_type][slice_part]);

	uint32_t min_chunkserver_version = getMinChunkserverVersion(c, ChunkPartType(slice_type, slice_part));

	for (const auto &label_and_count : replicate_labels) {
		tried_to_replicate = true;

		if (jobsnorepbefore >= eventloop_time()) {
			break;
		}

		if (label_and_count.first == MediaLabel::kWildcard) {
			if (!replicationDelayInfoForAll.replicationAllowed(
			            label_and_count.second)) {
				continue;
			}
		} else if (!replicationDelayInfoForLabel[label_and_count.first].replicationAllowed(
		                   label_and_count.second)) {
			skipped_replications += label_and_count.second;
			continue;
		}

		// Get a list of possible destination servers
		int total_matching, returned_matching, temporarily_unavailable;
		matocsserv_getservers_lessrepl(label_and_count.first, min_chunkserver_version, MaxWriteRepl,
		                               ip_counter, servers, total_matching, returned_matching,
		                               temporarily_unavailable);

		// Find a destination server for replication -- the first one without a copy of 'c'
		matocsserventry *destination = nullptr;
		matocsserventry *backup_destination = nullptr;
		for (const auto &server : servers) {
			if (!chunkPresentOnServer(c, server)) {
				destination = server;
				break;
			}
			if (backup_destination == nullptr && !chunkPresentOnServer(c, slice_type, server)) {
				backup_destination = server;
			}
		}

		// If destination was not found, use a backup one, i.e. server where
		// there is a copy of this chunk, but from different slice.
		// Do it only if there are no available chunkservers in the system,
		// not if they merely reached their replication limit.
		if (destination == nullptr && temporarily_unavailable == 0) {
			// there are no servers which are expected to be available soon,
			// so backup server must be used
			destination = backup_destination;
		}

		if (destination == nullptr) {
			// there is no server suitable for replication to be written to
			break;
		}

		if (!(label_and_count.first == MediaLabel::kWildcard ||
		      matocsserv_get_label(destination) == label_and_count.first)) {
			// found server doesn't match requested label
			if (total_matching > returned_matching) {
				// There is a server which matches the current label, but it has
				// exceeded the
				// replication limit. In this case we won't try to use servers with
				// non-matching
				// labels as our destination -- we will wait for that server to be
				// ready.
				skipped_replications += label_and_count.second;
				continue;
			}
			if (!RebalancingBetweenLabels && !c->isEndangered()
			    && calc.isSafeEnoughToWrite(gRedundancyLevel)) {
				// Chunk is not endangered, so we should prevent label spilling.
				// Only endangered chunks will be replicated across labels.
				skipped_replications += label_and_count.second;
				continue;
			}
			if (valid_parts_count + skipped_replications >= expected_copies) {
				// Don't create copies on non-matching servers if there already are
				// enough replicas.
				continue;
			}
		}

		if (tryReplication(c, ChunkPartType(slice_type, slice_part), destination)) {
			inforec_.done.copy_undergoal++;
			return true;
		} else {
			// There is no server suitable for replication to be read from
			skipped_replications += label_and_count.second;
			break;  // there's no need to analyze other labels if there's no free source
			        // server
		}
	}
	if (tried_to_replicate) {
		inforec_.notdone.copy_undergoal++;
		// Enqueue chunk again only if it was taken directly from endangered chunks queue
		// to avoid repetitions. If it was taken from chunk hashmap, inEndangeredQueue bit
		// would be still up.
		if (gEndangeredChunksServingLimit > 0 && Chunk::endangeredChunks.size() < gEndangeredChunksMaxCapacity
			&& !c->inEndangeredQueue && calc.getState() == ChunksAvailabilityState::kEndangered) {
			c->inEndangeredQueue = 1;
			Chunk::endangeredChunks.push_back(c);
		}
	}

	return false;
}

bool ChunkWorker::removeUnneededChunkPart(Chunk *c, Goal::Slice::Type slice_type, int slice_part,
					ChunkCopiesCalculator &calc, const IpCounter &ip_counter) {
	Goal::Slice::Labels remove_pool = calc.getRemovePool(slice_type, slice_part);
	if (remove_pool.empty()) {
		return false;
	}

	ChunkPart *candidate = nullptr;
	bool candidate_todel = false;
	int candidate_occurrence = 0;
	double candidate_usage = std::numeric_limits<double>::lowest();

	for (auto &part : c->parts) {
		if (!part.is_valid() || part.type != ChunkPartType(slice_type, slice_part)) {
			continue;
		}
		if (matocsserv_deletion_counter(part.server()) >= TmpMaxDel) {
			continue;
		}

		MediaLabel server_label = matocsserv_get_label(part.server());
		if (remove_pool.find(server_label) == remove_pool.end()) {
			continue;
		}

		bool is_todel = part.is_todel();
		double usage = matocsserv_get_usage(part.server());
		int occurrence = ip_counter.empty() ? 1 : ip_counter.at(matocsserv_get_servip(part.server()));

		if (std::make_tuple(is_todel, occurrence, usage) >
		      std::make_tuple(candidate_todel, candidate_occurrence, candidate_usage)) {
			candidate = &part;
			candidate_usage = usage;
			candidate_todel = is_todel;
			candidate_occurrence = occurrence;
		}
	}

	if (candidate &&
	    calc.canRemovePart(slice_type, slice_part, matocsserv_get_label(candidate->server()))) {
		c->deleteCopy(*candidate);
		c->needverincrease = 1;
		stats_deletions++;
		matocsserv_send_deletechunk(candidate->server(), c->chunkid, 0, candidate->type);

		int overgoal_copies = calc.countPartsToMove(slice_type, slice_part).second;

		inforec_.done.del_overgoal++;
		deleteDone_++;
		inforec_.notdone.del_overgoal += overgoal_copies - 1;
		deleteNotDone_ += overgoal_copies - 1;

		return true;
	}

	return false;
}

bool ChunkWorker::rebalanceChunkParts(Chunk *c, ChunkCopiesCalculator &calc, bool only_todel, const IpCounter &ip_counter) {
	if(!only_todel) {
		double min_usage = sortedServers_.front().disk_usage;
		double max_usage = sortedServers_.back().disk_usage;
		if ((max_usage - min_usage) <= gAcceptableDifference) {
			return false;
		}
	}

	// Consider each copy to be moved to a server with disk usage much less than actual.
	// There are at least two servers with a disk usage difference grater than
	// gAcceptableDifference, so it's worth checking.
	for (const auto &part : c->parts) {
		if (!part.is_valid()) {
			continue;
		}

		if(only_todel && !part.is_todel()) {
			continue;
		}

		auto current_ip = matocsserv_get_servip(part.server());
		auto it = ip_counter.find(current_ip);
		auto current_ip_count = it != ip_counter.end() ? it->second : 0;

		MediaLabel current_copy_label = matocsserv_get_label(part.server());
		double current_copy_disk_usage = matocsserv_get_usage(part.server());
		// Look for a server that has disk usage much less than currentCopyDiskUsage.
		// If such a server exists consider creating a new copy of this chunk there.
		// First, choose all possible candidates for the destination server: we consider
		// only
		// servers with the same label is rebalancing between labels if turned off or the
		// goal
		// requires our copy to exist on a server labeled 'currentCopyLabel'.
		bool multi_label_rebalance =
		        RebalancingBetweenLabels &&
		        (current_copy_label == MediaLabel::kWildcard ||
		         calc.canMovePartToDifferentLabel(part.type.getSliceType(),
		                                          part.type.getSlicePart(),
		                                          current_copy_label));

		uint32_t min_chunkserver_version = getMinChunkserverVersion(c, part.type);

		const ServersWithUsage &sorted_servers =
		        multi_label_rebalance ? sortedServers_
		                              : labeledSortedServers_[current_copy_label];

		for (const auto &empty_server : sorted_servers) {
			if (!only_todel && gAvoidSameIpChunkservers) {
				auto empty_server_ip = matocsserv_get_servip(empty_server.server);
				auto it = ip_counter.find(empty_server_ip);
				auto empty_server_ip_count = it != ip_counter.end() ? it->second : 0;
				if (empty_server_ip != current_ip && empty_server_ip_count >= current_ip_count) {
					continue;
				}
			}

			if (!only_todel && empty_server.disk_usage >
			    current_copy_disk_usage - gAcceptableDifference) {
				break;  // No more suitable destination servers (next servers have
				        // higher usage)
			}
			if (matocsserv_get_version(empty_server.server) < min_chunkserver_version) {
				continue;
			}
			if (chunkPresentOnServer(c, part.type.getSliceType(), empty_server.server)) {
				continue;  // A copy is already here
			}
			if (matocsserv_replication_write_counter(empty_server.server) >= MaxWriteRepl) {
				continue;  // We can't create a new copy here
			}
			if (tryReplication(c, part.type, empty_server.server)) {
				inforec_.copy_rebalance++;
				return true;
			}
		}
	}

	return false;
}

bool ChunkWorker::rebalanceChunkPartsWithSameIp(Chunk *c, ChunkCopiesCalculator &calc, const IpCounter &ip_counter) {
	if (!gAvoidSameIpChunkservers) {
		return false;
	}

	for (const auto &part : c->parts) {
		if (!part.is_valid()) {
			continue;
		}

		auto current_ip = matocsserv_get_servip(part.server());
		auto it = ip_counter.find(current_ip);
		auto current_ip_count = it != ip_counter.end() ? it->second : 0;

		MediaLabel current_copy_label = matocsserv_get_label(part.server());

		bool multi_label_rebalance =
		        RebalancingBetweenLabels &&
		        (current_copy_label == MediaLabel::kWildcard ||
		         calc.canMovePartToDifferentLabel(part.type.getSliceType(),
		                                          part.type.getSlicePart(),
		                                          current_copy_label));

		uint32_t min_chunkserver_version = getMinChunkserverVersion(c, part.type);

		const ServersWithUsage &sorted_servers =
		        multi_label_rebalance ? sortedServers_
		                              : labeledSortedServers_[current_copy_label];

		ServersWithUsage sorted_by_ip_count;
		sorted_by_ip_count.resize(sorted_servers.size());
		counting_sort_copy(sorted_servers.begin(), sorted_servers.end(), sorted_by_ip_count.begin(),
			           [&ip_counter](const ServerWithUsage& elem) {
			                  auto ip = matocsserv_get_servip(elem.server);
			                  auto it = ip_counter.find(ip);
			                  return it != ip_counter.end() ? it->second : 0;
			           });

		for (const auto &empty_server : sorted_by_ip_count) {
			auto empty_server_ip = matocsserv_get_servip(empty_server.server);
			auto it = ip_counter.find(empty_server_ip);
			auto empty_server_ip_count = it != ip_counter.end() ? it->second : 0;
			if (empty_server_ip_count >= (current_ip_count - 1)) {
				break;
			}

			if (matocsserv_get_version(empty_server.server) < min_chunkserver_version) {
				continue;
			}
			if (chunkPresentOnServer(c, part.type.getSliceType(), empty_server.server)) {
				continue;  // A copy is already here
			}
			if (matocsserv_replication_write_counter(empty_server.server) >= MaxWriteRepl) {
				continue;  // We can't create a new copy here
			}
			if (tryReplication(c, part.type, empty_server.server)) {
				inforec_.copy_rebalance++;
				return true;
			}
		}
	}

	return false;
}


void ChunkWorker::doChunkJobs(Chunk *c, uint16_t serverCount) {
	// step 0. Update chunk's statistics
	// Useful e.g. if definitions of goals did change.
	chunk_handle_disconnected_copies(c);
	c->updateStats();
	if (serverCount == 0) {
		return;
	}

	int invalid_parts = 0;
	ChunkCopiesCalculator calc(c->getGoal());

	// Chunk is in degenerate state if it has more than 1 part
	// on the same chunkserver (i.e. 1 std and 1 xor)
	// TODO(sarna): this flat_set should be removed after
	// 'slists' are rewritten to use sensible data structures
	bool degenerate = false;
	flat_set<matocsserventry *, small_vector<matocsserventry *, 64>> servers;

	// step 1. calculate number of valid and invalid copies
	for (const auto &part : c->parts) {
		if (part.is_valid()) {
			calc.addPart(part.type, matocsserv_get_label(part.server()));
			if (!degenerate) {
				degenerate = servers.count(part.server()) > 0;
				servers.insert(part.server());
			}
		} else {
			++invalid_parts;
		}
	}
	calc.optimize();

	// step 1a. count number of chunk parts on servers with the same ip
	IpCounter ip_occurrence;
	if (gAvoidSameIpChunkservers) {
		for (auto &part : c->parts) {
			if (part.is_valid()) {
				++ip_occurrence[matocsserv_get_servip(part.server())];
			}
		}
	}

	// step 2. check number of copies
	if (c->isLost() && invalid_parts > 0 && c->fileCount() > 0) {
		lzfs_pretty_syslog(LOG_WARNING, "chunk %016" PRIx64 " has not enough valid parts (%d)"
		                   " consider repairing it manually", c->chunkid, invalid_parts);
		for (const auto &part : c->parts) {
			if (!part.is_valid()) {
				lzfs_pretty_syslog(LOG_NOTICE, "chunk %016" PRIx64 "_%08x - invalid part on (%s - ver:%08x)",
				c->chunkid, c->version, matocsserv_getstrip(part.server()), part.version);
			}
		}
		return;
	}

	// step 3. delete invalid parts
	deleteInvalidChunkParts(c);

	// step 4. return if chunk is during some operation
	if (c->operation != Chunk::NONE || (c->isLocked())) {
		return;
	}

	// step 5. check busy count
	for (const auto &part : c->parts) {
		if (part.is_busy()) {
			lzfs_pretty_syslog(LOG_WARNING, "chunk %016" PRIX64 " has unexpected BUSY copies",
			       c->chunkid);
			return;
		}
	}

	// step 6. delete unused chunk
	if (c->fileCount() == 0) {
		deleteAllChunkParts(c);
		return;
	}

	if (c->isLost()) {
		return;
	}

	// step 7. check if chunk needs any replication
	for (const auto &slice : calc.getTarget()) {
		for (int i = 0; i < slice.size(); ++i) {
			if (replicateChunkPart(c, slice.getType(), i, calc, ip_occurrence)) {
				return;
			}
		}
	}

	// Do not remove any parts if more than 1 part resides on 1 chunkserver
	if (degenerate && calc.countPartsToRecover() > 0) {
		return;
	}

	// step 8. if chunk has too many copies then delete some of them
	for (const auto &slice : calc.getAvailable()) {
		for (int i = 0; i < slice.size(); ++i) {
			std::pair<int, int> operations = calc.countPartsToMove(slice.getType(), i);
			if (operations.first > 0 || operations.second == 0) {
				// do not remove elements if some are missing
				continue;
			}

			if (removeUnneededChunkPart(c, slice.getType(), i, calc, ip_occurrence)) {
				return;
			}
		}
	}

	// step 9. If chunk has parts marked as "to delete" then move them to other servers
	if(rebalanceChunkParts(c, calc, true, ip_occurrence)) {
		return;
	}

	if (chunksinfo.notdone.copy_undergoal > 0 && chunksinfo.done.copy_undergoal > 0) {
		return;
	}

	// step 10. Move chunk parts residing on chunkservers with the same ip.
	if (rebalanceChunkPartsWithSameIp(c, calc, ip_occurrence)) {
		return;
	}

	// step 11. if there is too big difference between chunkservers then make copy of chunk from
	// a server with a high disk usage on a server with low disk usage
	if (rebalanceChunkParts(c, calc, false, ip_occurrence)) {
		return;
	}

}

bool ChunkWorker::deleteUnusedChunks() {
	while (stack_.node != nullptr) {
		chunk_handle_disconnected_copies(stack_.node);
		if (stack_.node->fileCount() == 0 && stack_.node->parts.empty()) {
			// If current chunk in zombie loop is to be deleted, it must be updated
			// to the next chunk
			if (stack_.node == gCurrentChunkInZombieLoop) {
				gCurrentChunkInZombieLoop = gCurrentChunkInZombieLoop->next;
			}
			// Something could be inserted between prev and node (when we yielded)
			// so we need to make prev valid.
			while (stack_.prev && stack_.prev->next != stack_.node) {
				stack_.prev = stack_.prev->next;
			}

			assert((!stack_.prev && gChunksMetadata->chunkhash[stack_.current_bucket] == stack_.node) ||
			       (stack_.prev && stack_.prev->next == stack_.node));

			if (stack_.prev) {
				stack_.prev->next = stack_.node->next;
			} else {
				gChunksMetadata->chunkhash[stack_.current_bucket] =
				        stack_.node->next;
			}

			Chunk *tmp = stack_.node->next;
			chunk_delete(stack_.node);
			stack_.node = tmp;
		} else {
			stack_.prev = stack_.node;
			stack_.node = stack_.node->next;
		}

		if (stack_.watchdog.expired()) {
			return false;
		}
	}

	return true;
}

void ChunkWorker::mainLoop() {
	Chunk *c;

	reenter(this) {
		stack_.work_limit.setMaxDuration(std::chrono::milliseconds(ChunksLoopTimeout));
		stack_.work_limit.start();
		stack_.watchdog.start();
		stack_.chunks_done_count = 0;
		stack_.buckets_done_count = 0;

		if (starttime + gOperationsDelayInit > eventloop_time()) {
			return;
		}

		double min_usage, max_usage;
		matocsserv_usagedifference(&min_usage, &max_usage, &stack_.usable_server_count,
		                           nullptr);

		if (min_usage > max_usage) {
			return;
		}

		doEverySecondTasks();

		if (jobsnorepbefore < eventloop_time()) {
			stack_.endangered_to_serve = gEndangeredChunksServingLimit;
			while (stack_.endangered_to_serve > 0 && !Chunk::endangeredChunks.empty()) {
				c = Chunk::endangeredChunks.front();
				Chunk::endangeredChunks.pop_front();
				// If queued chunk is obsolete (e.g. was freed while in queue),
				// do not proceed with chunk jobs.
				if (c->inEndangeredQueue == 1) {
					c->inEndangeredQueue = 0;
					doChunkJobs(c, stack_.usable_server_count);
				}
				--stack_.endangered_to_serve;

				if (stack_.watchdog.expired()) {
					yield;
					stack_.watchdog.start();
				}
			}
		}

		while (stack_.buckets_done_count < HashSteps &&
		       stack_.chunks_done_count < HashCPS) {
			if (stack_.current_bucket == 0) {
				doEveryLoopTasks();
			}

			if (stack_.watchdog.expired()) {
				yield;
				stack_.watchdog.start();
			}

			// delete unused chunks
			stack_.prev = nullptr;
			stack_.node = gChunksMetadata->chunkhash[stack_.current_bucket];
			while (!deleteUnusedChunks()) {
				yield;
				stack_.watchdog.start();
			}

			// regenerate usable_server_count
			matocsserv_usagedifference(nullptr, nullptr, &stack_.usable_server_count,
			                           nullptr);

			stack_.node = gChunksMetadata->chunkhash[stack_.current_bucket];
			while (stack_.node) {
				doChunkJobs(stack_.node, stack_.usable_server_count);
				++stack_.chunks_done_count;
				stack_.node = stack_.node->next;

				if (stack_.watchdog.expired()) {
					yield;
					stack_.watchdog.start();
					matocsserv_usagedifference(nullptr, nullptr,
					                           &stack_.usable_server_count,
					                           nullptr);
				}
			}

			stack_.current_bucket +=
			        123;  // if HASHSIZE is any power of 2 then any odd number is
			              // good here
			stack_.current_bucket %= HASHSIZE;
			++stack_.buckets_done_count;

			if (stack_.work_limit.expired()) {
				break;
			}
		}
	}
}

static std::unique_ptr<ChunkWorker> gChunkWorker;

void chunk_jobs_main(void) {
	if (gChunkWorker->is_complete()) {
		gChunkWorker->reset();
	}
}

void chunk_jobs_process_bit(void) {
	if (!gChunkWorker->is_complete()) {
		gChunkWorker->mainLoop();
		if (!gChunkWorker->is_complete()) {
			eventloop_make_next_poll_nonblocking();
		}
	}
}

#endif

constexpr uint32_t kSerializedChunkSizeNoLockId = 16;
constexpr uint32_t kSerializedChunkSizeWithLockId = 20;
#define CHUNKCNT 1000

#ifdef METARESTORE

void chunk_dump(void) {
	Chunk *c;
	uint32_t i;

	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=gChunksMetadata->chunkhash[i] ; c ; c=c->next) {
			printf("*|i:%016" PRIX64 "|v:%08" PRIX32 "|g:%" PRIu8 "|t:%10" PRIu32 "\n",c->chunkid,c->version,c->highestIdGoal(),c->lockedto);
		}
	}
}

#endif

int chunk_load(FILE *fd, bool loadLockIds) {
	uint8_t hdr[8];
	const uint8_t *ptr;
	int32_t r;
	Chunk *c;
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
	Chunk *c;
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
	Chunk::count = 0;
#endif
	gChunksMetadata->nextchunkid = 1;
}

#ifndef METARESTORE
void chunk_become_master() {
	starttime = eventloop_time();
	jobsnorepbefore = starttime + gOperationsDelayInit;
	gChunkWorker = std::unique_ptr<ChunkWorker>(new ChunkWorker());
	gChunkLoopEventHandle = eventloop_timeregister_ms(ChunksLoopPeriod, chunk_jobs_main);
	eventloop_eachloopregister(chunk_jobs_process_bit);
	return;
}

void chunk_reload(void) {
	uint32_t repl;
	uint32_t looptime;

	// Set deprecated values first and override them if newer definition is found
	gOperationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT", 300);
	gOperationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT", 3600);
	gOperationsDelayInit = cfg_getuint32("OPERATIONS_DELAY_INIT", gOperationsDelayInit);
	gOperationsDelayDisconnect = cfg_getuint32("OPERATIONS_DELAY_DISCONNECT", gOperationsDelayDisconnect);
	gAvoidSameIpChunkservers = cfg_getuint32("AVOID_SAME_IP_CHUNKSERVERS", 0);
	gRedundancyLevel = cfg_getuint32("REDUNDANCY_LEVEL", 0);

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
				lzfs_pretty_syslog(LOG_WARNING,"CHUNKS_SOFT_DEL_LIMIT is greater than CHUNKS_HARD_DEL_LIMIT - using CHUNKS_HARD_DEL_LIMIT for both");
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

	repl = cfg_getuint32("CHUNKS_WRITE_REP_LIMIT", 2);
	if (repl > 0) {
		MaxWriteRepl = repl;
	}

	repl = cfg_getuint32("CHUNKS_READ_REP_LIMIT", 10);
	if (repl > 0) {
		MaxReadRepl = repl;
	}

	ChunksLoopPeriod = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_PERIOD", 1000, MINCHUNKSLOOPPERIOD, MAXCHUNKSLOOPPERIOD);
	if (gChunkLoopEventHandle) {
		eventloop_timechange_ms(gChunkLoopEventHandle, ChunksLoopPeriod);
	}

	repl = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_MAX_CPU", 60, MINCHUNKSLOOPCPU, MAXCHUNKSLOOPCPU);
	ChunksLoopTimeout = repl * ChunksLoopPeriod / 100;

	if (cfg_isdefined("CHUNKS_LOOP_TIME")) {
		looptime = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_TIME", 300, MINLOOPTIME, MAXLOOPTIME);
		uint64_t scaled_looptime = std::max((uint64_t)1000 * looptime / ChunksLoopPeriod, (uint64_t)1);
		HashSteps = 1 + ((HASHSIZE) / scaled_looptime);
		HashCPS   = 0xFFFFFFFF;
	} else {
		looptime = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_MIN_TIME", 300, MINLOOPTIME, MAXLOOPTIME);
		HashCPS = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_MAX_CPS", 100000, MINCPS, MAXCPS);
		uint64_t scaled_looptime = std::max((uint64_t)1000 * looptime / ChunksLoopPeriod, (uint64_t)1);
		HashSteps = 1 + ((HASHSIZE) / scaled_looptime);
		HashCPS   = (uint64_t)ChunksLoopPeriod * HashCPS / 1000;
	}
	double endangeredChunksPriority = cfg_ranged_get("ENDANGERED_CHUNKS_PRIORITY", 0.0, 0.0, 1.0);
	gEndangeredChunksServingLimit = HashSteps * endangeredChunksPriority;
	gEndangeredChunksMaxCapacity = cfg_get("ENDANGERED_CHUNKS_MAX_CAPACITY", static_cast<uint64_t>(1024*1024UL));
	gAcceptableDifference = cfg_ranged_get("ACCEPTABLE_DIFFERENCE",0.1, 0.001, 10.0);
	RebalancingBetweenLabels = cfg_getuint32("CHUNKS_REBALANCING_BETWEEN_LABELS", 0) == 1;
}
#endif

int chunk_strinit(void) {
	gChunksMetadata = new ChunksMetadata;

#ifndef METARESTORE
	Chunk::count = 0;
	for (int i = 0; i < CHUNK_MATRIX_SIZE; ++i) {
		for (int j = 0; j < CHUNK_MATRIX_SIZE; ++j) {
			Chunk::allFullChunkCopies[i][j] = 0;
		}
	}
	Chunk::allChunksAvailability = ChunksAvailabilityState();
	Chunk::allChunksReplicationState = ChunksReplicationState();

	uint32_t disableChunksDel = cfg_getuint32("DISABLE_CHUNKS_DEL", 0);
	gOperationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT", 300);
	gOperationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT", 3600);
	gOperationsDelayInit = cfg_getuint32("OPERATIONS_DELAY_INIT", gOperationsDelayInit);
	gOperationsDelayDisconnect = cfg_getuint32("OPERATIONS_DELAY_DISCONNECT", gOperationsDelayDisconnect);
	gAvoidSameIpChunkservers = cfg_getuint32("AVOID_SAME_IP_CHUNKSERVERS", 0);
	gRedundancyLevel = cfg_getuint32("REDUNDANCY_LEVEL", 0);

	if (disableChunksDel) {
		MaxDelHardLimit = MaxDelSoftLimit = 0;
	} else {
		MaxDelSoftLimit = cfg_getuint32("CHUNKS_SOFT_DEL_LIMIT",10);
		if (cfg_isdefined("CHUNKS_HARD_DEL_LIMIT")) {
			MaxDelHardLimit = cfg_getuint32("CHUNKS_HARD_DEL_LIMIT",25);
			if (MaxDelHardLimit<MaxDelSoftLimit) {
				MaxDelSoftLimit = MaxDelHardLimit;
				lzfs_pretty_syslog(LOG_WARNING, "%s: CHUNKS_SOFT_DEL_LIMIT is greater than "
					"CHUNKS_HARD_DEL_LIMIT - using CHUNKS_HARD_DEL_LIMIT for both",
					cfg_filename().c_str());
			}
		} else {
			MaxDelHardLimit = 3 * MaxDelSoftLimit;
		}
		if (MaxDelSoftLimit == 0) {
			throw InitializeException(cfg_filename() + ": CHUNKS_SOFT_DEL_LIMIT is zero");
		}
	}
	TmpMaxDelFrac = MaxDelSoftLimit;
	TmpMaxDel = MaxDelSoftLimit;
	MaxWriteRepl = cfg_getuint32("CHUNKS_WRITE_REP_LIMIT",2);
	MaxReadRepl = cfg_getuint32("CHUNKS_READ_REP_LIMIT",10);
	if (MaxReadRepl==0) {
		throw InitializeException(cfg_filename() + ": CHUNKS_READ_REP_LIMIT is zero");
	}
	if (MaxWriteRepl==0) {
		throw InitializeException(cfg_filename() + ": CHUNKS_WRITE_REP_LIMIT is zero");
	}

	ChunksLoopPeriod  = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_PERIOD", 1000, MINCHUNKSLOOPPERIOD, MAXCHUNKSLOOPPERIOD);
	uint32_t repl = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_MAX_CPU", 60, MINCHUNKSLOOPCPU, MAXCHUNKSLOOPCPU);
	ChunksLoopTimeout = repl * ChunksLoopPeriod / 100;

	uint32_t looptime;
	if (cfg_isdefined("CHUNKS_LOOP_TIME")) {
		lzfs_pretty_syslog(LOG_WARNING,
				"%s: defining loop time by CHUNKS_LOOP_TIME option is "
				"deprecated - use CHUNKS_LOOP_MAX_CPS and CHUNKS_LOOP_MIN_TIME",
				cfg_filename().c_str());
		looptime = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_TIME", 300, MINLOOPTIME, MAXLOOPTIME);
		uint64_t scaled_looptime = std::max((uint64_t)1000 * looptime / ChunksLoopPeriod, (uint64_t)1);
		HashSteps = 1 + ((HASHSIZE) / scaled_looptime);
		HashCPS   = 0xFFFFFFFF;
	} else {
		looptime = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_MIN_TIME", 300, MINLOOPTIME, MAXLOOPTIME);
		HashCPS = cfg_get_minmaxvalue<uint32_t>("CHUNKS_LOOP_MAX_CPS", 100000, MINCPS, MAXCPS);
		uint64_t scaled_looptime = std::max((uint64_t)1000 * looptime / ChunksLoopPeriod, (uint64_t)1);
		HashSteps = 1 + ((HASHSIZE) / scaled_looptime);
		HashCPS   = (uint64_t)ChunksLoopPeriod * HashCPS / 1000;
	}
	double endangeredChunksPriority = cfg_ranged_get("ENDANGERED_CHUNKS_PRIORITY", 0.0, 0.0, 1.0);
	gEndangeredChunksServingLimit = HashSteps * endangeredChunksPriority;
	gEndangeredChunksMaxCapacity = cfg_get("ENDANGERED_CHUNKS_MAX_CAPACITY", static_cast<uint64_t>(1024*1024UL));
	gAcceptableDifference = cfg_ranged_get("ACCEPTABLE_DIFFERENCE", 0.1, 0.001, 10.0);
	RebalancingBetweenLabels = cfg_getuint32("CHUNKS_REBALANCING_BETWEEN_LABELS", 0) == 1;
	eventloop_reloadregister(chunk_reload);
	metadataserver::registerFunctionCalledOnPromotion(chunk_become_master);
	eventloop_eachloopregister(chunk_clean_zombie_servers_a_bit);
	if (metadataserver::isMaster()) {
		chunk_become_master();
	}
#endif
	return 1;
}
