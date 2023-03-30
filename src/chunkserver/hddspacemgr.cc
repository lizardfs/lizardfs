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

#ifdef LIZARDFS_HAVE_FALLOC_FL_PUNCH_HOLE_IN_LINUX_FALLOC_H
#  define LIZARDFS_HAVE_FALLOC_FL_PUNCH_HOLE
#endif

#if defined(LIZARDFS_HAVE_FALLOCATE) && defined(LIZARDFS_HAVE_FALLOC_FL_PUNCH_HOLE) && !defined(_GNU_SOURCE)
  #define _GNU_SOURCE
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#ifdef LIZARDFS_HAVE_FALLOC_FL_PUNCH_HOLE_IN_LINUX_FALLOC_H
#  include <linux/falloc.h>
#endif
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#ifndef LIZARDFS_HAVE_THREAD_LOCAL
#include <pthread.h>
#endif // LIZARDFS_HAVE_THREAD_LOCAL
#include <signal.h>
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

#ifdef LIZARDFS_HAVE_THREAD_LOCAL
#include <array>
#endif // LIZARDFS_HAVE_THREAD_LOCAL
#include <atomic>
#include <deque>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "chunkserver/chunk.h"
#include "chunkserver/chunk_filename_parser.h"
#include "chunkserver/chunk_signature.h"
#include "chunkserver/indexed_resource_pool.h"
#include "chunkserver/iostat.h"
#include "chunkserver/open_chunk.h"
#include "common/cfg.h"
#include "common/chunk_version_with_todel_flag.h"
#include "common/cwrap.h"
#include "common/crc.h"
#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/disk_info.h"
#include "common/exceptions.h"
#include "common/event_loop.h"
#include "common/massert.h"
#include "common/moosefs_vector.h"
#include "common/random.h"
#include "common/serialization.h"
#include "common/slice_traits.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "common/unique_queue.h"
#include "devtools/TracePrinter.h"
#include "devtools/request_log.h"
#include "protocol/MFSCommunication.h"

#define LOSTCHUNKSBLOCKSIZE 1024
#define NEWCHUNKSBLOCKSIZE 4096 // TODO consider sending more chunks in one packet

#define ERRORLIMIT 2
#define LASTERRTIME 60

#define CH_NEW_NONE 0
#define CH_NEW_AUTO 1
#define CH_NEW_EXCLUSIVE 2

static std::atomic<unsigned> HDDTestFreq_ms(10 * 1000);

static std::atomic<bool> ScrubIsRunning(0);

/// Number of bytes which should be addded to each disk's used space
static uint64_t gLeaveFree;

/// Default value for HDD_LEAVE_SPACE_DEFAULT
static const char gLeaveSpaceDefaultDefaultStrValue[] = "4GiB";

/// Value of HDD_ADVISE_NO_CACHE from config
static std::atomic_bool gAdviseNoCache;

static std::atomic<bool> MooseFSChunkFormat;

static std::atomic<bool> PerformFsync;

static bool gPunchHolesInFiles;

/* folders data */
static folder *folderhead = NULL;

/// Active folder scans in progress
/* theoretically it would return a false positive if scans haven't started yet,
 * but it's a _very_ unlikely situation */
static std::atomic_int gScansInProgress(0);

namespace {

/**
 * Defines hash and equal operations on ChunkWithType type, so it can be used as
 * the key type in an std::unordered_map.
 */
struct KeyOperations {
	constexpr KeyOperations() = default;
	constexpr std::size_t operator()(const ChunkWithType &chunkWithType) const {
		return hash(chunkWithType);
	}
	constexpr bool operator()(const ChunkWithType &lhs, const ChunkWithType &rhs) const {
		return equal(lhs, rhs);
	}

private:
	constexpr std::size_t hash(const ChunkWithType &chunkWithType) const {
		return chunkWithType.id;
	}
	constexpr bool equal(const ChunkWithType &lhs, const ChunkWithType &rhs) const {
		return (lhs.id == rhs.id && lhs.type == rhs.type);
	}
};

/**
 * std::unique_ptr on Chunk is used here as the stored objects are of Chunk's subclasses types.
 */
using chunk_registry_t = std::unordered_map<ChunkWithType, std::unique_ptr<Chunk>, KeyOperations, KeyOperations>;

/** \brief Global registry of all chunks stored on chunkserver.
 */
chunk_registry_t gChunkRegistry;

inline ChunkWithType makeChunkKey(uint64_t id, ChunkPartType type) {
	return {id, type};
}
inline ChunkWithType chunkToKey(const Chunk &chunk) {
	return makeChunkKey(chunk.chunkid, chunk.type());
}

} // unnamed namespace

// master reports
static std::deque<ChunkWithType> gDamagedChunks;
static std::deque<ChunkWithType> gLostChunks;
static std::deque<ChunkWithVersionAndType> gNewChunks;
static std::atomic<uint32_t> errorcounter(0);
static std::atomic_int hddspacechanged(0);

static std::thread foldersthread, delayedthread, testerthread, scrubberthread;
static std::thread test_chunk_thread;

static std::atomic<int> term(0);
static uint8_t folderactions = 0; // no need for atomic; guarded by folderlock anyway
static std::atomic<uint8_t> testerreset(0);

// master reports = damaged chunks, lost chunks, new chunks
static std::mutex gMasterReportsLock;

/** \brief gChunkRegistry mutex
 *
 * This mutex only guards access to gChunkRegistry.
 * Chunk objects stored in the registry have their own separate locks.
 */
static std::mutex gChunkRegistryLock;
static cntcond *cclist = NULL;

// folderhead + all data in structures (except folder::cstat)
static std::mutex folderlock;

// chunk tester
static std::mutex testlock;

#ifndef LIZARDFS_HAVE_THREAD_LOCAL
static pthread_key_t hdrbufferkey;
static pthread_key_t blockbufferkey;
#endif // LIZARDFS_HAVE_THREAD_LOCAL

static uint32_t emptyblockcrc;

static IndexedResourcePool<OpenChunk> gOpenChunks;

// These stats_* variables are for charts only. Therefore there's no need
// to keep an absolute consistency with a mutex.
static std::atomic<uint64_t> stats_overheadbytesr(0);
static std::atomic<uint64_t> stats_overheadbytesw(0);
static std::atomic<uint32_t> stats_overheadopr(0);
static std::atomic<uint32_t> stats_overheadopw(0);
static std::atomic<uint64_t> stats_totalbytesr(0);
static std::atomic<uint64_t> stats_totalbytesw(0);
static std::atomic<uint32_t> stats_totalopr(0);
static std::atomic<uint32_t> stats_totalopw(0);
static std::atomic<uint64_t> stats_totalrtime(0);
static std::atomic<uint64_t> stats_totalwtime(0);

static std::atomic<uint32_t> stats_create(0);
static std::atomic<uint32_t> stats_delete(0);
static std::atomic<uint32_t> stats_test(0);
static std::atomic<uint32_t> stats_version(0);
static std::atomic<uint32_t> stats_duplicate(0);
static std::atomic<uint32_t> stats_truncate(0);
static std::atomic<uint32_t> stats_duptrunc(0);

static const int kOpenRetryCount = 4;
static const int kOpenRetry_ms = 5;
static IoStat gIoStat;

void hdd_report_damaged_chunk(uint64_t chunkid, ChunkPartType chunk_type) {
	TRACETHIS1(chunkid);
	std::lock_guard<std::mutex> lock_guard(gMasterReportsLock);
	gDamagedChunks.push_back({chunkid, chunk_type});
}

void hdd_get_damaged_chunks(std::vector<ChunkWithType>& buffer, std::size_t limit) {
	TRACETHIS();
	std::lock_guard<std::mutex> lock_guard(gMasterReportsLock);
	std::size_t size = std::min(gDamagedChunks.size(), limit);
	buffer.assign(gDamagedChunks.begin(), gDamagedChunks.begin() + size);
	gDamagedChunks.erase(gDamagedChunks.begin(), gDamagedChunks.begin() + size);
}

void hdd_report_lost_chunk(uint64_t chunkid, ChunkPartType chunk_type) {
	TRACETHIS1(chunkid);
	std::lock_guard<std::mutex> lock_guard(gMasterReportsLock);
	gLostChunks.push_back({chunkid, chunk_type});
}

void hdd_get_lost_chunks(std::vector<ChunkWithType>& buffer, std::size_t limit) {
	TRACETHIS();
	std::lock_guard<std::mutex> lock_guard(gMasterReportsLock);
	std::size_t size = std::min(gLostChunks.size(), limit);
	buffer.assign(gLostChunks.begin(), gLostChunks.begin() + size);
	gLostChunks.erase(gLostChunks.begin(), gLostChunks.begin() + size);
}

void hdd_report_new_chunk(uint64_t chunkid, uint32_t version, bool todel, ChunkPartType type) {
	TRACETHIS();
	uint32_t versionWithTodelFlag = common::combineVersionWithTodelFlag(version, todel);
	std::lock_guard<std::mutex> lock_guard(gMasterReportsLock);
	gNewChunks.push_back(ChunkWithVersionAndType(chunkid, versionWithTodelFlag, type));
}

void hdd_get_new_chunks(std::vector<ChunkWithVersionAndType>& buffer, std::size_t limit) {
	TRACETHIS();
	std::lock_guard<std::mutex> lock_guard(gMasterReportsLock);
	std::size_t size = std::min(gNewChunks.size(), limit);
	buffer.assign(gNewChunks.begin(), gNewChunks.begin() + size);
	gNewChunks.erase(gNewChunks.begin(), gNewChunks.begin() + size);
}

uint32_t hdd_errorcounter(void) {
	TRACETHIS();
	return errorcounter.exchange(0);
}

int hdd_spacechanged(void) {
	TRACETHIS();
	return hddspacechanged.exchange(0);
}

void hdd_stats(uint64_t *over_bytesr, uint64_t *over_bytesw, uint32_t *over_opr, uint32_t *over_opw, uint64_t *total_bytesr, uint64_t *total_bytesw,
		uint32_t *total_opr, uint32_t *total_opw, uint64_t *total_rtime, uint64_t *total_wtime) {
	TRACETHIS();
	*over_bytesr = stats_overheadbytesr.exchange(0);
	*over_bytesw = stats_overheadbytesw.exchange(0);
	*over_opr = stats_overheadopr.exchange(0);
	*over_opw = stats_overheadopw.exchange(0);
	*total_bytesr = stats_totalbytesr.exchange(0);
	*total_bytesw = stats_totalbytesw.exchange(0);
	*total_opr = stats_totalopr.exchange(0);
	*total_opw = stats_totalopw.exchange(0);
	*total_rtime = stats_totalrtime.exchange(0);
	*total_wtime = stats_totalwtime.exchange(0);
}

void hdd_op_stats(uint32_t *op_create,uint32_t *op_delete,uint32_t *op_version,uint32_t *op_duplicate,uint32_t *op_truncate,uint32_t *op_duptrunc,uint32_t *op_test) {
	TRACETHIS();
	*op_create = stats_create.exchange(0);
	*op_delete = stats_delete.exchange(0);
	*op_test = stats_test.exchange(0);
	*op_version = stats_version.exchange(0);
	*op_duplicate = stats_duplicate.exchange(0);
	*op_truncate = stats_truncate.exchange(0);
	*op_duptrunc = stats_duptrunc.exchange(0);
}

static inline void hdd_stats_overheadread(uint32_t size) {
	TRACETHIS();
	stats_overheadopr++;
	stats_overheadbytesr += size;
}

static inline void hdd_stats_overheadwrite(uint32_t size) {
	TRACETHIS();
	stats_overheadopw++;
	stats_overheadbytesw += size;
}

template<typename T>
void atomic_max(std::atomic<T> &result, T value) {
	T prev_value = result;
	while(prev_value < value && !result.compare_exchange_weak(prev_value, value)) {
	}
}

static inline void hdd_stats_totalread(folder *f, uint64_t size, uint64_t rtime) {
	TRACETHIS();
	if (rtime<=0) {
		return;
	}
	stats_totalopr++;
	stats_totalbytesr += size;
	stats_totalrtime += rtime;

	f->cstat.rops++;
	f->cstat.rbytes += size;
	f->cstat.usecreadsum += rtime;
	atomic_max<uint32_t>(f->cstat.usecreadmax, rtime);
}

static inline void hdd_stats_totalwrite(folder *f, uint64_t size, uint64_t wtime) {
	TRACETHIS();
	if (wtime <= 0) {
		return;
	}
	stats_totalopw++;
	stats_totalbytesw += size;
	stats_totalwtime += wtime;

	f->cstat.wops++;
	f->cstat.wbytes += size;
	f->cstat.usecwritesum += wtime;
	atomic_max<uint32_t>(f->cstat.usecwritemax, wtime);
}

static inline void hdd_stats_datafsync(folder *f, uint64_t fsynctime) {
	TRACETHIS();
	if (fsynctime<=0) {
		return;
	}
	stats_totalwtime += fsynctime;

	f->cstat.fsyncops++;
	f->cstat.usecfsyncsum += fsynctime;
	atomic_max<uint32_t>(f->cstat.usecfsyncmax, fsynctime);
}

static inline uint64_t get_usectime() {
	TRACETHIS();
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return ((uint64_t)(tv.tv_sec))*1000000+tv.tv_usec;
}

class IOStatsUpdater {
public:
	using StatsUpdateFunc = void(*)(folder *f, uint64_t size, uint64_t time);

	IOStatsUpdater(folder *folder, uint64_t dataSize, StatsUpdateFunc updateFunc)
		: startTime_(get_usectime()), dataSize_(dataSize), folder_(folder), updateFunc_(updateFunc), success_(true) {
	}
	~IOStatsUpdater() {
		if (success_) {
			uint64_t duration = get_usectime() - startTime_;
			updateFunc_(folder_, dataSize_, duration);
		}
	}

	void markIOAsFailed() noexcept {
		success_ = false;
	}

private:
	uint64_t startTime_;
	uint64_t dataSize_;
	folder *folder_;
	StatsUpdateFunc updateFunc_;
	bool success_;
};

class FolderWriteStatsUpdater {
public:
	FolderWriteStatsUpdater(folder *folder, uint64_t dataSize)
		: updater_(folder, dataSize, hdd_stats_totalwrite) {
	}

	void markWriteAsFailed() noexcept {
		updater_.markIOAsFailed();
	}

private:
	IOStatsUpdater updater_;
};

class FolderReadStatsUpdater {
public:
	FolderReadStatsUpdater(folder *folder, uint64_t dataSize)
		: updater_(folder, dataSize, hdd_stats_totalread) {
	}

	void markReadAsFailed() noexcept {
		updater_.markIOAsFailed();
	}

private:
	IOStatsUpdater updater_;
};



uint32_t hdd_diskinfo_v1_size() {
	TRACETHIS();
	folder *f;
	uint32_t s,sl;

	s = 0;
	folderlock.lock();
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
	folderlock.unlock();
}

uint32_t hdd_diskinfo_v2_size() {
	TRACETHIS();
	folder *f;
	uint32_t s,sl;

	s = 0;
	folderlock.lock();
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
		serialize(&buff, diskInfoVector);
	}
	folderlock.unlock();
}

void hdd_diskinfo_movestats(void) {
	TRACETHIS();
	folder *f;
	std::lock_guard<std::mutex> folderlock_guard(folderlock);
	for (f=folderhead ; f ; f=f->next) {
		if (f->statspos==0) {
			f->statspos = STATSHISTORY-1;
		} else {
			f->statspos--;
		}
		f->stats[f->statspos] = f->cstat;
		f->cstat.clear();
	}
}

static inline void hdd_chunk_remove(Chunk *c) {
	TRACETHIS();
	assert(c);
	auto chunkIter = gChunkRegistry.find(chunkToKey(*c));
	if (chunkIter == gChunkRegistry.end()) {
		lzfs::log_warn("Chunk to be removed wasn't found on the chunkserver. (chunkid: {:#04x}, chunktype: {})", c->chunkid, c->type().toString());
		return;
	}
	const Chunk *cp = chunkIter->second.get();
	gOpenChunks.purge(cp->fd);
	if (cp->owner) {
		// remove this chunk from its folder's testlist
		std::lock_guard<std::mutex> testlock_guard(testlock);
		if (cp->testnext) {
			cp->testnext->testprev = cp->testprev;
		} else {
			cp->owner->testtail = cp->testprev;
		}
		*(cp->testprev) = cp->testnext;
	}
	gChunkRegistry.erase(chunkIter);
}

void hdd_chunk_release(Chunk *c) {
	TRACETHIS();
	assert(c);
	std::lock_guard<std::mutex> registryLockGuard(gChunkRegistryLock);
//      syslog(LOG_WARNING,"hdd_chunk_release got chunk: %016" PRIX64 " (c->state:%u)",c->chunkid,c->state);
	if (c->state==CH_LOCKED) {
		c->state = CH_AVAIL;
		if (c->ccond) {
//                      printf("wake up one thread waiting for AVAIL chunk: %" PRIu64 " on ccond:%p\n",c->chunkid,c->ccond);
			c->ccond->cond.notify_one();
		}
	} else if (c->state==CH_TOBEDELETED) {
		if (c->ccond) {
			c->state = CH_DELETED;
//                      printf("wake up one thread waiting for DELETED chunk: %" PRIu64 " on ccond:%p\n",c->chunkid,c->ccond);
			c->ccond->cond.notify_one();
		} else {
			hdd_chunk_remove(c);
		}
	}
}

static int hdd_chunk_getattr(Chunk *c) {
	assert(c);
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

bool hdd_chunk_trylock(Chunk *c) {
	assert(gChunkRegistryLock.try_lock() == false);
	assert(c);
	bool ret = false;
	TRACETHIS1(c->chunkid);
	if (c != nullptr && c->state == CH_AVAIL) {
		c->state = CH_LOCKED;
		ret = true;
	}
	return ret;
}

static void hdd_chunk_delete(Chunk *c);

/*! \brief Remove old chunk c and create new one in its place.
 *
 * Before introduction of interleaved chunk format it was sufficient
 * to clear chunk data and reuse object. Now with the change of chunk
 * format we need to create new object with different size and different
 * virtual table.
 *
 * We preserve chunk id and threads waiting for this object.
 *
 * \param c pointer to old object
 * \param chunkid chunk id that will be reused
 * \param type type of new chunk object
 * \param format format of new chunk object
 * \return address of new object
 */
static Chunk *hdd_chunk_recreate(Chunk *c, uint64_t chunkid, ChunkPartType type,
		ChunkFormat format) {
	cntcond *waiting = nullptr;

	if (c) {
		assert(c->chunkid == chunkid);

		if (c->state != CH_DELETED && c->owner) {
			std::lock_guard<std::mutex> folderlock_guard(folderlock);
			c->owner->chunkcount--;
			c->owner->needrefresh = 1;
		}

		waiting = c->ccond;

		// It's possible to reuse object c
		// if the format is the same,
		// but it doesn't happen often enough
		// to justify adding extra code.
		hdd_chunk_remove(c);
	}

	if (format == ChunkFormat::MOOSEFS) {
		c = new MooseFSChunk(chunkid, type, CH_LOCKED);
	} else {
		sassert(format == ChunkFormat::INTERLEAVED);
		c = new InterleavedChunk(chunkid, type, CH_LOCKED);
	}
	passert(c);
	bool success = gChunkRegistry.insert({makeChunkKey(chunkid, type), std::unique_ptr<Chunk>(c)}).second;
	massert(success, "Cannot insert new chunk to the registry as a chunk with its chunkId and chunkPartType already exists");

	c->ccond = waiting;
	if (waiting) {
		waiting->owner = c;
	}

	return c;
}

static Chunk* hdd_chunk_get(
		uint64_t chunkid,
		ChunkPartType chunkType,
		uint8_t cflag,
		ChunkFormat format) {
	TRACETHIS2(chunkid, (unsigned)cflag);
	Chunk *c = nullptr;
	cntcond *cc = nullptr;

	std::unique_lock<std::mutex> registryLockGuard(gChunkRegistryLock);
	auto chunkIter = gChunkRegistry.find(makeChunkKey(chunkid, chunkType));
	if (chunkIter == gChunkRegistry.end()) {
		if (cflag!=CH_NEW_NONE) {
			c = hdd_chunk_recreate(nullptr, chunkid, chunkType, format);
		}
		return c;
	}
	c = chunkIter->second.get();
	if (cflag==CH_NEW_EXCLUSIVE) {
		if (c->state==CH_AVAIL || c->state==CH_LOCKED) {
			return NULL;
		}
	}
	for (;;) {
		switch (c->state) {
		case CH_AVAIL:
			c->state = CH_LOCKED;
//                      syslog(LOG_WARNING,"hdd_chunk_get returns chunk: %016" PRIX64 " (c->state:%u)",c->chunkid,c->state);
			registryLockGuard.unlock();
			if (c->validattr==0) {
				if (hdd_chunk_getattr(c) == -1) {
					if (cflag != CH_NEW_NONE) {
						unlink(c->filename().c_str());
						registryLockGuard.lock();
						c = hdd_chunk_recreate(c, chunkid, chunkType, format);
						return c;
					}
					hdd_report_damaged_chunk(c->chunkid, c->type());
					unlink(c->filename().c_str());
					hdd_chunk_delete(c);
					return NULL;
				}
			}
			return c;
		case CH_DELETED:
			if (cflag!=CH_NEW_NONE) {
				c = hdd_chunk_recreate(c, chunkid, chunkType, format);
				return c;
			}
			if (c->ccond==NULL) {   // no more waiting threads - remove
				hdd_chunk_remove(c);
			} else {        // there are waiting threads - wake them up
//                              printf("wake up one thread waiting for DELETED chunk: %" PRIu64 " on ccond:%p\n",c->chunkid,c->ccond);
				c->ccond->cond.notify_one();
			}
			return NULL;
		case CH_TOBEDELETED:
		case CH_LOCKED:
			cc = c->ccond;
			if (cc == nullptr) {
				for (cc = cclist; cc && cc->wcnt; cc = cc->next) {
				}
				if (cc == nullptr) {
					cc = new cntcond();
					passert(cc);
					cc->wcnt = 0;
					cc->next = cclist;
					cclist = cc;
				}
				cc->owner = c;
				c->ccond = cc;
			}
			cc->wcnt++;
			cc->cond.wait(registryLockGuard);
			// Chunk could be recreated (different address)
			// so we need to get it's new address.
			c = cc->owner;
			assert(c);
			cc->wcnt--;
			if (cc->wcnt == 0) {
				c->ccond = nullptr;
				cc->owner = nullptr;
			}
		}
	}
}

static void hdd_chunk_delete(Chunk *c) {
	TRACETHIS();
	assert(c);
	folder *f;
	{
		std::lock_guard<std::mutex> registryLockGuard(gChunkRegistryLock);
		f = c->owner;
		if (c->ccond) {
			c->state = CH_DELETED;
			//printf("wake up one thread waiting for DELETED chunk: %" PRIu64 " ccond:%p\n",c->chunkid,c->ccond);
			c->ccond->cond.notify_one();
		} else {
			hdd_chunk_remove(c);
		}
	}
	std::lock_guard<std::mutex> folderlock_guard(folderlock);
	f->chunkcount--;
	f->needrefresh = 1;
}

static Chunk* hdd_chunk_create(
		folder *f,
		uint64_t chunkid,
		ChunkPartType chunkType,
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
	c->setFilenameLayout(Chunk::kCurrentDirectoryLayout);
	std::lock_guard<std::mutex> testlock_guard(testlock);
	c->testnext = NULL;
	c->testprev = f->testtail;
	(*c->testprev) = c;
	f->testtail = &(c->testnext);
	return c;
}

static inline Chunk* hdd_chunk_find(uint64_t chunkId, ChunkPartType chunkType) {
	LOG_AVG_TILL_END_OF_SCOPE0("chunk_find");
	return hdd_chunk_get(chunkId, chunkType, CH_NEW_NONE, ChunkFormat::IMPROPER);
}

static void hdd_chunk_testmove(Chunk *c) {
	TRACETHIS();
	assert(c);
	std::lock_guard<std::mutex> testlock_guard(testlock);
	if (c->testnext) {
		*(c->testprev) = c->testnext;
		c->testnext->testprev = c->testprev;
		c->testnext = NULL;
		c->testprev = c->owner->testtail;
		*(c->testprev) = c;
		c->owner->testtail = &(c->testnext);
	}
}

// no locks - locked by caller
static inline void hdd_refresh_usage(folder *f) {
	TRACETHIS();
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
	uint8_t todel = f->todel;

	std::lock_guard<std::mutex> registryLockGuard(gChunkRegistryLock);
	std::lock_guard<std::mutex> testlock_guard(testlock);

	// Until C++14 the order of the elements that are not erased is not guaranteed to be preserved in std::unordered_map.
	// Thus, to be truly portable, all elements to be removed from gChunkRegistry are first stored in an auxiliary container
	// and then each is erased from gChunkRegistry outside the loop over gChunkRegistry's entries.
	std::vector<Chunk *> chunksToRemove;
	if (rmflag) {
		chunksToRemove.reserve(f->chunkcount);
	}
	for (const auto &chunkEntry : gChunkRegistry) {
		Chunk *c = chunkEntry.second.get();
		if (c->owner==f) {
			c->todel = todel;
			if (rmflag) {
				chunksToRemove.push_back(c);
			} else {
				hdd_report_new_chunk(c->chunkid,
					c->version, c->todel, c->type());
			}
		}
	}
	for (auto c : chunksToRemove) {
		hdd_report_lost_chunk(c->chunkid, c->type());
		if (c->state==CH_AVAIL) {
			gOpenChunks.purge(c->fd);
			if (c->testnext) {
				c->testnext->testprev = c->testprev;
			} else {
				c->owner->testtail = c->testprev;
			}
			*(c->testprev) = c->testnext;
			gChunkRegistry.erase(chunkToKey(*c));
		} else if (c->state==CH_LOCKED) {
			c->state = CH_TOBEDELETED;
		}
	}
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

	std::unique_lock<std::mutex> folderlock_guard(folderlock);
	if (folderactions==0) {
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
				f->scanthread.join();
				/* fallthrough */
			case SCST_SENDNEEDED:
			case SCST_SCANNEEDED:
				f->scanstate = SCST_WORKING;
				/* fallthrough */
			case SCST_WORKING:
				hdd_senddata(f,1);
				changed = 1;
				f->toremove = 0;
				break;
			}
			if (f->migratestate == MGST_MIGRATEFINISHED) {
				f->migratethread.join();
				f->migratestate = MGST_MIGRATEDONE;
			}
			if (f->toremove==0) { // 0 here means 'removed', so delete it from data structures
				*fptr = f->next;
				lzfs_pretty_syslog(LOG_NOTICE,"folder %s successfully removed",f->path);
				if (f->lfd>=0) {
					close(f->lfd);
				}
				free(f->path);
				delete f;
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
			f->scanthread = std::thread(hdd_folder_scan, f);
			break;
		case SCST_SCANFINISHED:
			f->scanthread.join();
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
				lzfs_pretty_syslog(LOG_WARNING,"%u errors occurred in %u seconds on folder: %s",err,LASTERRTIME,f->path);
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
		if (f->migratestate == MGST_MIGRATEFINISHED) {
			f->migratethread.join();
			f->migratestate = MGST_MIGRATEDONE;
		}
	}
	folderlock_guard.unlock();
	if (changed) {
		hddspacechanged = 1;
	}
}

void hdd_error_occured(Chunk *c) {
	TRACETHIS();
	assert(c);
	uint32_t i;
	folder *f;
	struct timeval tv;
	int errmem = errno;

	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		gettimeofday(&tv,NULL);
		f = c->owner;
		i = f->lasterrindx;
		f->lasterrtab[i].chunkid = c->chunkid;
		f->lasterrtab[i].errornumber = errmem;
		f->lasterrtab[i].timestamp = tv.tv_sec;
		i = (i+1)%LASTERRSIZE;
		f->lasterrindx = i;
	}

	++errorcounter;

	errno = errmem;
}


void hdd_foreach_chunk_in_bulks(
	std::function<void(std::vector<ChunkWithVersionAndType>&)> chunk_bulk_callback,
	std::size_t chunk_bulk_size
) {
	TRACETHIS();
	std::vector<ChunkWithVersionAndType> bulk;
	std::vector<ChunkWithType> recheckList;
	bulk.reserve(chunk_bulk_size);

	enum class BulkReadyWhen { FULL, NONEMPTY };
	auto handleBulkIfReady = [&bulk, &chunk_bulk_callback, chunk_bulk_size](BulkReadyWhen whatIsReady) {
		if (
			(whatIsReady == BulkReadyWhen::FULL && bulk.size() >= chunk_bulk_size)
			|| (whatIsReady == BulkReadyWhen::NONEMPTY && !bulk.empty())
		) {
			chunk_bulk_callback(bulk);
			bulk.clear();
		}
	};
	auto addChunkToBulk = [&bulk](const Chunk *chunk) {
		common::chunk_version_t versionWithTodelFlag = common::combineVersionWithTodelFlag(chunk->version, chunk->todel);
		bulk.push_back(ChunkWithVersionAndType(chunk->chunkid, versionWithTodelFlag, chunk->type()));
	};

	{
		// do the operation for all immediately available (not-locked) chunks
		// add all other chunks to recheckList
		std::lock_guard<std::mutex> registryLockGuard(gChunkRegistryLock);

		for (const auto &chunkEntry : gChunkRegistry) {
			const Chunk *chunk = chunkEntry.second.get();
			if (chunk->state != CH_AVAIL) {
				recheckList.push_back(ChunkWithType(chunk->chunkid, chunk->type()));
				continue;
			}
			handleBulkIfReady(BulkReadyWhen::FULL);
			addChunkToBulk(chunk);
		}
		handleBulkIfReady(BulkReadyWhen::NONEMPTY);
	}

	// wait till each chunk from recheckList becomes available, lock (acquire) it and then do the operation
	for (const auto &chunkWithType : recheckList) {
		handleBulkIfReady(BulkReadyWhen::FULL);
		Chunk *chunk = hdd_chunk_find(chunkWithType.id, chunkWithType.type);
		if (chunk) {
			addChunkToBulk(chunk);
			hdd_chunk_release(chunk);
		}
	}
	handleBulkIfReady(BulkReadyWhen::NONEMPTY);
}


void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount) {
	TRACETHIS();
	folder *f;
	uint64_t avail,total;
	uint64_t tdavail,tdtotal;
	uint32_t chunks,tdchunks;
	avail = total = tdavail = tdtotal = 0ULL;
	chunks = tdchunks = 0;
	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
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
	}
	*usedspace = total-avail;
	*totalspace = total;
	*chunkcount = chunks;
	*tdusedspace = tdtotal-tdavail;
	*tdtotalspace = tdtotal;
	*tdchunkcount = tdchunks;
}

int hdd_get_load_factor() {
	return gIoStat.getLoadFactor();
}

static inline int hdd_int_chunk_readcrc(MooseFSChunk *c, uint32_t chunk_version) {
	TRACETHIS();
	assert(c);
	ChunkSignature chunkSignature;
	if (!chunkSignature.readFromDescriptor(c->fd, c->getSignatureOffset())) {
		int errmem = errno;
		lzfs_silent_errlog(LOG_WARNING,
				"chunk_readcrc: file:%s - read error", c->filename().c_str());
		errno = errmem;
		return LIZARDFS_ERROR_IO;
	}
	if (!chunkSignature.hasValidSignatureId()) {
		lzfs_pretty_syslog(LOG_WARNING,
				"chunk_readcrc: file:%s - wrong header", c->filename().c_str());
		errno = 0;
		return LIZARDFS_ERROR_IO;
	}
	if (chunk_version == std::numeric_limits<uint32_t>::max()) {
		chunk_version = c->version;
	}
	if (c->chunkid != chunkSignature.chunkId()
			|| chunk_version != chunkSignature.chunkVersion()
			|| c->type().getId() != chunkSignature.chunkType().getId()) {
		lzfs_pretty_syslog(LOG_WARNING,
				"chunk_readcrc: file:%s - wrong id/version/type in header "
				"(%016" PRIX64 "_%08" PRIX32 ", typeId %" PRIu8 ")",
				c->filename().c_str(),
				chunkSignature.chunkId(),
				chunkSignature.chunkVersion(),
				chunkSignature.chunkType().getId());
		errno = 0;
		return LIZARDFS_ERROR_IO;
	}

	uint8_t *crc_data = gOpenChunks.getResource(c->fd).crc_data();
#ifndef ENABLE_CRC /* if NOT defined */
	for (int i = 0; i < MFSBLOCKSINCHUNK; ++i) {
		memcpy(crc_data + i * sizeof(uint32_t), &emptyblockcrc, sizeof(uint32_t));
	}
#else /* if ENABLE_CRC defined */
	int ret;
	{
		FolderReadStatsUpdater updater(c->owner, c->getCrcBlockSize());
		ret = pread(c->fd, crc_data, c->getCrcBlockSize(), c->getCrcOffset());
		if ((size_t)ret != c->getCrcBlockSize()) {
			int errmem = errno;
			lzfs_silent_errlog(LOG_WARNING,
					"chunk_readcrc: file:%s - read error", c->filename().c_str());
			errno = errmem;
			updater.markReadAsFailed();
			return LIZARDFS_ERROR_IO;
		}
	}
	hdd_stats_overheadread(c->getCrcBlockSize());
#endif /* ENABLE_CRC */
	errno = 0;
	return LIZARDFS_STATUS_OK;
}

static inline int chunk_writecrc(MooseFSChunk *c) {
	TRACETHIS();
	assert(c);
	folderlock.lock();
	c->owner->needrefresh = 1;
	folderlock.unlock();
	uint8_t *crc_data = gOpenChunks.getResource(c->fd).crc_data();
	{
		FolderWriteStatsUpdater updater(c->owner, c->getCrcBlockSize());
		ssize_t ret = pwrite(c->fd, crc_data, c->getCrcBlockSize(), c->getCrcOffset());
		if (ret != static_cast<ssize_t>(c->getCrcBlockSize())) {
			int errmem = errno;
			lzfs_silent_errlog(LOG_WARNING,
					"chunk_writecrc: file:%s - write error", c->filename().c_str());
			errno = errmem;
			updater.markWriteAsFailed();
			return LIZARDFS_ERROR_IO;
		}
	}
	hdd_stats_overheadwrite(c->getCrcBlockSize());
	return LIZARDFS_STATUS_OK;
}

static int hdd_io_begin(Chunk *c,int newflag, uint32_t chunk_version = std::numeric_limits<uint32_t>::max()) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_io_begin");
	TRACETHIS();
	assert(c);
	int status;

//      syslog(LOG_NOTICE,"chunk: %" PRIu64 " - before io",c->chunkid);
	hdd_chunk_testmove(c);
	if (c->refcount==0) {
		bool add = (c->fd < 0);

		assert(!(newflag && c->fd >= 0));

		gOpenChunks.acquire(c->fd);
		if (c->fd < 0) {
			// Try to free some long unused descriptors
			gOpenChunks.freeUnused(eventloop_time(), gChunkRegistryLock);
			for (int i = 0; i < kOpenRetryCount; ++i) {
				if (newflag) {
					c->fd = open(c->filename().c_str(), O_RDWR | O_TRUNC | O_CREAT, 0666);
				} else {
					if (c->todel < 2) {
						c->fd = open(c->filename().c_str(), O_RDWR);
					} else {
						c->fd = open(c->filename().c_str(), O_RDONLY);
					}
				}
				if (c->fd < 0 && errno != ENFILE) {
					lzfs_silent_errlog(LOG_WARNING,"hdd_io_begin: file:%s - open error", c->filename().c_str());
					return LIZARDFS_ERROR_IO;
				} else if (c->fd >= 0) {
					gOpenChunks.acquire(c->fd, OpenChunk(c));
					break;
				} else { // c->fd < 0 && errno == ENFILE
					usleep((kOpenRetry_ms * 1000) << i);
					// Force free unused descriptors
					gOpenChunks.freeUnused(std::numeric_limits<uint32_t>::max(), gChunkRegistryLock, 4);
				}
			}
			if (c->fd < 0) {
				lzfs_silent_errlog(LOG_WARNING,"hdd_io_begin: file:%s - open error", c->filename().c_str());
				return LIZARDFS_ERROR_IO;
			}
		}

		IF_MOOSEFS_CHUNK(mc, c) {
			if (newflag) {
				uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data();
				memset(crc_data, 0, mc->getCrcBlockSize());
			} else if (add) {
				mc->readaheadHeader();
				status = hdd_int_chunk_readcrc(mc, chunk_version);
				if (status != LIZARDFS_STATUS_OK) {
					int errmem = errno;
					gOpenChunks.release(c->fd, eventloop_time());
					lzfs_silent_errlog(LOG_WARNING,
							"hdd_io_begin: file:%s - read error", c->filename().c_str());
					errno = errmem;
					return status;
				}
			}
		}
	}
	c->refcount++;
	errno = 0;
	return LIZARDFS_STATUS_OK;
}

static int hdd_io_end(Chunk *c) {
	assert(c);
	TRACETHIS1(c->chunkid);
	uint64_t ts,te;

	if (c->wasChanged) {
		IF_MOOSEFS_CHUNK(mc, c) {
			int status = chunk_writecrc(mc);
			PRINTTHIS(status);
			if (status != LIZARDFS_STATUS_OK) {
				//FIXME(hazeman): We are probably leaking fd here.
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
				return LIZARDFS_ERROR_IO;
			}
#else
			if (fsync(c->fd)<0) {
				int errmem = errno;
				lzfs_silent_errlog(LOG_WARNING,
						"hdd_io_end: file:%s - fsync (direct call) error", c->filename().c_str());
				errno = errmem;
				return LIZARDFS_ERROR_IO;
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
		return LIZARDFS_STATUS_OK;
	}
	c->refcount--;
	if (c->refcount==0) {
		gOpenChunks.release(c->fd, eventloop_time());
	}
	errno = 0;
	return LIZARDFS_STATUS_OK;
}

/* I/O operations */
int hdd_open(Chunk *chunk) {
	assert(chunk);
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_open");
	TRACETHIS1(chunk->chunkid);
	int status = hdd_io_begin(chunk, 0);
	PRINTTHIS(status);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		hdd_report_damaged_chunk(chunk->chunkid, chunk->type());
	}
	return status;
}

int hdd_open(uint64_t chunkid, ChunkPartType chunkType) {
	Chunk *c = hdd_chunk_find(chunkid, chunkType);
	if (c == NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	int status = hdd_open(c);
	hdd_chunk_release(c);
	return status;
}

int hdd_close(Chunk *chunk) {
	assert(chunk);
	TRACETHIS1(chunk->chunkid);
	int status = hdd_io_end(chunk);
	PRINTTHIS(status);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		hdd_report_damaged_chunk(chunk->chunkid, chunk->type());
	}
	return status;
}

int hdd_close(uint64_t chunkid, ChunkPartType chunkType) {
	Chunk *c = hdd_chunk_find(chunkid, chunkType);
	if (c == NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	int status = hdd_close(c);
	hdd_chunk_release(c);
	return status;
}

/**
 * Get thread specific buffer
 */
#ifdef LIZARDFS_HAVE_THREAD_LOCAL

uint8_t* hdd_get_block_buffer() {
	// Pad in order to make block data aligned in cache (helps CRC)
	static constexpr int kMaxCacheLine = 64;
	static constexpr int kPadding = kMaxCacheLine - sizeof(uint32_t);
	static thread_local std::array<uint8_t, kHddBlockSize + kPadding> blockbuffer;
	return blockbuffer.data() + kPadding;
}

uint8_t* hdd_get_header_buffer() {
	static thread_local std::array<uint8_t, MooseFSChunk::kMaxHeaderSize> hdrbuffer;
	return hdrbuffer.data();
}

#else // LIZARDFS_HAVE_THREAD_LOCAL

uint8_t* hdd_get_block_buffer() {
	// Pad in order to make block data aligned in cache (helps CRC)
	static constexpr int kMaxCacheLine = 64;
	static constexpr int kPadding = kMaxCacheLine - sizeof(uint32_t);
	uint8_t *blockbuffer = (uint8_t*)pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		blockbuffer = (uint8_t*)malloc(kHddBlockSize + kPadding);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
	return blockbuffer + kPadding;
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

#endif // LIZARDFS_HAVE_THREAD_LOCAL

int hdd_read_crc_and_block(Chunk* c, uint16_t blocknum, OutputBuffer* outputBuffer) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_read_block");
	assert(c);
	TRACETHIS2(c->chunkid, blocknum);
	int bytesRead = 0;

	if (blocknum >= MFSBLOCKSINCHUNK) {
		return LIZARDFS_ERROR_BNUMTOOBIG;
	}

	if (blocknum >= c->blocks) {
		bytesRead = outputBuffer->copyIntoBuffer(&emptyblockcrc, sizeof(uint32_t));
		static const std::vector<uint8_t> zeros_block(MFSBLOCKSIZE, 0);
		bytesRead += outputBuffer->copyIntoBuffer(zeros_block);
		if (static_cast<uint32_t>(bytesRead) != kHddBlockSize) {
			return LIZARDFS_ERROR_IO;
		}
	} else {
		int32_t toBeRead = c->chunkFormat() == ChunkFormat::INTERLEAVED
				? kHddBlockSize : MFSBLOCKSIZE;
		off_t off = c->getBlockOffset(blocknum);

		IF_MOOSEFS_CHUNK(mc, c) {
			assert(c->chunkFormat() == ChunkFormat::MOOSEFS);
			const uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data() + blocknum * sizeof(uint32_t);
			outputBuffer->copyIntoBuffer(crc_data, sizeof(uint32_t));
			bytesRead = outputBuffer->copyIntoBuffer(c->fd, MFSBLOCKSIZE, &off);
			if (bytesRead == toBeRead && !outputBuffer->checkCRC(bytesRead, get32bit(&crc_data))) {
				hdd_test_chunk(ChunkWithVersionAndType{c->chunkid, c->version, c->type()});
				return LIZARDFS_ERROR_CRC;
			}
		} else do {
			assert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
			uint8_t* crcBuff = hdd_get_block_buffer();
			uint8_t* data = crcBuff + sizeof(uint32_t);
			auto containsZerosOnly = [](uint8_t* buffer, uint32_t size) {
				return buffer[0] == 0 && !memcmp(buffer, buffer + 1, size - 1);
			};
			{
				FolderReadStatsUpdater updater(c->owner, 4);
				bytesRead = pread(c->fd, crcBuff, 4, off);
				if (bytesRead != 4) {
					updater.markReadAsFailed();
					break;
				}
			}
			if (containsZerosOnly(crcBuff, 4)) {
				// It looks like this is a sparse file with an empty block. Let's check it
				// and if that's the case let's recompute the CRC
				{
					FolderReadStatsUpdater updater(c->owner, MFSBLOCKSIZE);
					bytesRead = pread(c->fd, data, MFSBLOCKSIZE, off + sizeof(uint32_t));
					if (bytesRead != MFSBLOCKSIZE) {
						updater.markReadAsFailed();
						break;
					}
				}
				if (containsZerosOnly(data, MFSBLOCKSIZE)) {
					// It's indeed a sparse block, recompute the CRC in order to provide
					// backward compatibility
					memcpy(crcBuff, &emptyblockcrc, sizeof(uint32_t));
				}
				bytesRead = outputBuffer->copyIntoBuffer(hdd_get_block_buffer(), kHddBlockSize);
			} else {
				bytesRead = outputBuffer->copyIntoBuffer(c->fd, kHddBlockSize, &off);
				const uint8_t *crc = crcBuff;
				if (bytesRead == toBeRead && !outputBuffer->checkCRC(bytesRead - 4, get32bit(&crc))) {
					hdd_test_chunk(ChunkWithVersionAndType{c->chunkid, c->version, c->type()});
					return LIZARDFS_ERROR_CRC;
				}
			}
		} while (false);

		if (bytesRead != toBeRead) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"read_block_from_chunk: file:%s - read error", c->filename().c_str());
			hdd_report_damaged_chunk(c->chunkid, c->type());
			return LIZARDFS_ERROR_IO;
		}
	}

	return LIZARDFS_STATUS_OK;
}

static void hdd_prefetch(Chunk &chunk, uint16_t first_block, uint32_t block_count) {
	if (block_count > 0) {
		auto blockSize = chunk.chunkFormat() == ChunkFormat::MOOSEFS ?
				MFSBLOCKSIZE : kHddBlockSize;
#ifdef LIZARDFS_HAVE_POSIX_FADVISE
		posix_fadvise(chunk.fd, chunk.getBlockOffset(first_block),
				uint32_t(block_count) * blockSize, POSIX_FADV_WILLNEED);
#elif defined(__APPLE__)
		struct radvisory ra;
		ra.ra_offset = chunk.getBlockOffset(first_block);
		ra.ra_count = uint32_t(block_count) * blockSize;
		fcntl(chunk.fd, F_RDADVISE, &ra);
#endif
	}
}

int hdd_prefetch_blocks(uint64_t chunkid, ChunkPartType chunk_type, uint32_t first_block,
		uint16_t block_count) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_prefetch_blocks");

	Chunk *c = hdd_chunk_find(chunkid, chunk_type);
	if (!c) {
		lzfs_pretty_syslog(LOG_WARNING, "error finding chunk for prefetching: %" PRIu64, chunkid);
		return LIZARDFS_ERROR_NOCHUNK;
	}

	int status = hdd_open(c);
	if (status != LIZARDFS_STATUS_OK) {
		lzfs_pretty_syslog(LOG_WARNING, "error opening chunk for prefetching: %" PRIu64 " - %s",
				chunkid, lizardfs_error_string(status));
		hdd_chunk_release(c);
		return status;
	}

	hdd_prefetch(*c, first_block, block_count);

	lzfs_silent_syslog(LOG_DEBUG, "chunkserver.hdd_prefetch_blocks chunk: %" PRIu64
	                   "status: %u firstBlock: %u nrOfBlocks: %u",
	                   chunkid, status, first_block, block_count);

	status = hdd_close(c);
	if (status != LIZARDFS_STATUS_OK) {
		lzfs_pretty_syslog(LOG_WARNING, "error closing prefetched chunk: %" PRIu64 " - %s",
				chunkid, lizardfs_error_string(status));
	}

	hdd_chunk_release(c);


	return status;
}

int hdd_read(uint64_t chunkid, uint32_t version, ChunkPartType chunkType,
		uint32_t offset, uint32_t size, uint32_t maxBlocksToBeReadBehind,
		uint32_t blocksToBeReadAhead, OutputBuffer* outputBuffer) {
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_read");
	TRACETHIS3(chunkid, offset, size);

	uint32_t offsetWithinBlock = offset % MFSBLOCKSIZE;
	if ((size == 0) || ((offsetWithinBlock + size) > MFSBLOCKSIZE)) {
		return LIZARDFS_ERROR_WRONGSIZE;
	}

	Chunk* c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return LIZARDFS_ERROR_WRONGVERSION;
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
		OutputBuffer buffer = OutputBuffer(
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
	int status = LIZARDFS_STATUS_OK;
	if (size == MFSBLOCKSIZE) {
		status = hdd_read_crc_and_block(c, block, outputBuffer);
	} else {
		OutputBuffer tmp(kHddBlockSize);
		status = hdd_read_crc_and_block(c, block, &tmp);
		if (status == LIZARDFS_STATUS_OK) {
			uint8_t *crcBuffPointer = crcBuff;
			put32bit(&crcBuffPointer, mycrc32(0, tmp.data() + serializedSize(uint32_t()) + offsetWithinBlock, size));
			outputBuffer->copyIntoBuffer(crcBuff, sizeof(uint32_t));
			outputBuffer->copyIntoBuffer(tmp.data() + serializedSize(uint32_t()) + offsetWithinBlock, size);
		}
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
int hdd_int_read_block_and_crc(Chunk* c, uint8_t* blockBuffer, uint16_t blocknum, const char* errorMsg) {
	assert(c);
	IF_MOOSEFS_CHUNK(mc, c) {
		sassert(c->chunkFormat() == ChunkFormat::MOOSEFS);
		uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data();
		memcpy(blockBuffer, crc_data + blocknum * sizeof(uint32_t), sizeof(uint32_t));
		{
			FolderReadStatsUpdater updater(mc->owner, MFSBLOCKSIZE);
			if (pread(mc->fd, blockBuffer + sizeof(uint32_t), MFSBLOCKSIZE, mc->getBlockOffset(blocknum))
					!= MFSBLOCKSIZE) {
				hdd_error_occured(mc);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"%s: file:%s - read error", errorMsg, mc->filename().c_str());
				hdd_report_damaged_chunk(mc->chunkid, mc->type());
				updater.markReadAsFailed();
				return -1;
			}
		}
		return MFSBLOCKSIZE;
	} else {
		sassert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
		{
			FolderReadStatsUpdater updater(c->owner, kHddBlockSize);
			if (pread(c->fd, blockBuffer, kHddBlockSize, c->getBlockOffset(blocknum))
					!= kHddBlockSize) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"%s: file:%s - read error", errorMsg, c->filename().c_str());
				hdd_report_damaged_chunk(c->chunkid, c->type());
				updater.markReadAsFailed();
				return -1;
			}
		}
		hdd_int_recompute_crc_if_block_empty(blockBuffer + sizeof(uint32_t), blockBuffer);
		return kHddBlockSize;
	}
}

void hdd_int_punch_holes(Chunk *c, const uint8_t *buffer, uint32_t offset, uint32_t size) {
#if defined(LIZARDFS_HAVE_FALLOCATE) && defined(LIZARDFS_HAVE_FALLOC_FL_PUNCH_HOLE)
	if (!gPunchHolesInFiles) {
		return;
	}
	assert(c);

	constexpr uint32_t block_size = 4096;
	uint32_t p = (offset % block_size) == 0 ? 0 : block_size - (offset % block_size);
	uint32_t hole_start = 0, hole_size = 0;

	for(;(p + block_size) <= size; p += block_size) {
		const std::size_t *zero_test = reinterpret_cast<const std::size_t*>(buffer + p);
		bool is_zero = true;
		for(unsigned i = 0; i < block_size/sizeof(std::size_t); ++i) {
			if (zero_test[i] != 0) {
				is_zero = false;
				break;
			}
		}

		if (is_zero) {
			if (hole_size == 0) {
				hole_start = offset + p;
			}
			hole_size += block_size;
		} else {
			if (hole_size > 0) {
				fallocate(c->fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, hole_start, hole_size);
			}
			hole_size = 0;
		}
	}
	if (hole_size > 0) {
		fallocate(c->fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, hole_start, hole_size);
	}
#else
	(void)c;
	(void)buffer;
	(void)offset;
	(void)size;
#endif
}

/**
 * Returns number of written bytes on success, -1 on failure.
 */
int hdd_int_write_partial_block_and_crc(
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
		{
			FolderWriteStatsUpdater updater(mc->owner, size);
			auto ret = pwrite(mc->fd, buffer, size, mc->getBlockOffset(blockNum) + offset);
			if (ret != size) {
				hdd_error_occured(mc);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"%s: file:%s - write error", errorMsg, mc->filename().c_str());
				hdd_report_damaged_chunk(mc->chunkid, mc->type());
				updater.markWriteAsFailed();
				return -1;
			}
		}
		hdd_int_punch_holes(c, buffer, c->getBlockOffset(blockNum) + offset, size);
		uint8_t *crc_data = gOpenChunks.getResource(c->fd).crc_data();
		memcpy(crc_data + blockNum * sizeof(uint32_t), crcBuff, crcSize);
		return size;
	} else {
		sassert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
		{
			FolderWriteStatsUpdater updater(c->owner, crcSize);
			auto ret = pwrite(c->fd, crcBuff, crcSize, c->getBlockOffset(blockNum));
			if (ret != crcSize) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"%s: file:%s - crc write error", errorMsg, c->filename().c_str());
				hdd_report_damaged_chunk(c->chunkid, c->type());
				updater.markWriteAsFailed();
				return -1;
			}
		}
		{
			FolderWriteStatsUpdater updater(c->owner, size);
			auto ret = pwrite(c->fd, buffer, size, c->getBlockOffset(blockNum) + offset + crcSize);
			if (ret != size) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"%s: file:%s - write error", errorMsg, c->filename().c_str());
				hdd_report_damaged_chunk(c->chunkid, c->type());
				updater.markWriteAsFailed();
				return -1;
			}
		}
		hdd_int_punch_holes(c, buffer, c->getBlockOffset(blockNum) + offset + crcSize, size);
		return crcSize + size;
	}
}

int hdd_int_write_block_and_crc(
		Chunk* c,
		const uint8_t* buffer,
		const uint8_t* crcBuff,
		uint16_t blockNum,
		const char* errorMsg) {
	return hdd_int_write_partial_block_and_crc(
			c, buffer, 0, MFSBLOCKSIZE, crcBuff, blockNum, errorMsg);
}

int hdd_write(Chunk* chunk, uint32_t version,
		uint16_t blocknum, uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer) {
	assert(chunk);
	LOG_AVG_TILL_END_OF_SCOPE0("hdd_write");
	TRACETHIS3(chunk->chunkid, offset, size);
	uint32_t precrc, postcrc, combinedcrc, chcrc;

	if (chunk->version != version && version > 0) {
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	if (blocknum >= chunk->maxBlocksInFile()) {
		return LIZARDFS_ERROR_BNUMTOOBIG;
	}
	if (size > MFSBLOCKSIZE) {
		return LIZARDFS_ERROR_WRONGSIZE;
	}
	if ((offset >= MFSBLOCKSIZE) || (offset + size > MFSBLOCKSIZE)) {
		return LIZARDFS_ERROR_WRONGOFFSET;
	}
	if (crc != mycrc32(0, buffer, size)) {
		return LIZARDFS_ERROR_CRC;
	}
	chunk->wasChanged = true;
	if (offset == 0 && size == MFSBLOCKSIZE) {
		uint8_t crcBuff[sizeof(uint32_t)];
		if (blocknum >= chunk->blocks) {
			uint16_t prevBlocks = chunk->blocks;
			chunk->blocks = blocknum + 1;
			IF_MOOSEFS_CHUNK(mc, chunk) {
				uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data();
				for (uint16_t i = prevBlocks; i < blocknum; i++) {
					memcpy(crc_data + i * sizeof(uint32_t), &emptyblockcrc, sizeof(uint32_t));
				}
			}
		}
		uint8_t *crcBuffPointer = crcBuff;
		put32bit(&crcBuffPointer, crc);

		int written =
		    hdd_int_write_block_and_crc(chunk, buffer, crcBuff, blocknum, "write_block_to_chunk");
		if (written < 0) {
			return LIZARDFS_ERROR_IO;
		}
	} else {
		uint8_t *blockbuffer = hdd_get_block_buffer();
		if (blocknum < chunk->blocks) {
			auto readBytes = hdd_int_read_block_and_crc(chunk, blockbuffer, blocknum,
			                                            "write_block_to_chunk");
			uint8_t *data_in_buffer = blockbuffer + sizeof(uint32_t); // Skip crc
			if (readBytes < 0) {
				return LIZARDFS_ERROR_IO;
			}
			precrc = mycrc32(0, data_in_buffer, offset);
			chcrc = mycrc32(0, data_in_buffer + offset, size);
			postcrc = mycrc32(0, data_in_buffer + offset + size, MFSBLOCKSIZE - (offset + size));
			if (offset == 0) {
				combinedcrc = mycrc32_combine(chcrc, postcrc, MFSBLOCKSIZE - (offset + size));
			} else {
				combinedcrc = mycrc32_combine(precrc, chcrc, size);
				if ((offset + size) < MFSBLOCKSIZE) {
					combinedcrc =
					    mycrc32_combine(combinedcrc, postcrc, MFSBLOCKSIZE - (offset + size));
				}
			}
			const uint8_t *crcBuffPointer = blockbuffer;
			const uint8_t **tmpPtr = &crcBuffPointer;
			if (get32bit(tmpPtr) != combinedcrc) {
				errno = 0;
				hdd_error_occured(chunk);  // uses and preserves errno !!!
				lzfs_pretty_syslog(LOG_WARNING, "write_block_to_chunk: file:%s - crc error",
				       chunk->filename().c_str());
				hdd_report_damaged_chunk(chunk->chunkid, chunk->type());
				return LIZARDFS_ERROR_CRC;
			}
		} else {
			if (ftruncate(chunk->fd, chunk->getFileSizeFromBlockCount(blocknum + 1)) < 0) {
				hdd_error_occured(chunk);  // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING, "write_block_to_chunk: file:%s - ftruncate error",
				                   chunk->filename().c_str());
				hdd_report_damaged_chunk(chunk->chunkid, chunk->type());
				return LIZARDFS_ERROR_IO;
			}
			uint16_t prevBlocks = chunk->blocks;
			chunk->blocks = blocknum + 1;
			IF_MOOSEFS_CHUNK(mc, chunk) {
				uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data();
				for (uint16_t i = prevBlocks; i < blocknum; i++) {
					memcpy(crc_data + i * sizeof(uint32_t), &emptyblockcrc, sizeof(uint32_t));
				}
			}
			precrc = mycrc32_zeroblock(0, offset);
			postcrc = mycrc32_zeroblock(0, MFSBLOCKSIZE - (offset + size));
		}
		if (offset == 0) {
			combinedcrc = mycrc32_combine(crc, postcrc, MFSBLOCKSIZE - (offset + size));
		} else {
			combinedcrc = mycrc32_combine(precrc, crc, size);
			if ((offset + size) < MFSBLOCKSIZE) {
				combinedcrc = mycrc32_combine(combinedcrc, postcrc, MFSBLOCKSIZE - (offset + size));
			}
		}
		uint8_t *crcBuffPointer = blockbuffer;
		put32bit(&crcBuffPointer, combinedcrc);
		int written = hdd_int_write_partial_block_and_crc(chunk, buffer, offset, size, blockbuffer,
		                                                   blocknum, "write_block_to_chunk");
		if (written < 0) {
			return LIZARDFS_ERROR_IO;
		}
	}
	return LIZARDFS_STATUS_OK;
}

int hdd_write(uint64_t chunkid, uint32_t version, ChunkPartType chunkType,
		uint16_t blocknum, uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer) {
	Chunk *chunk = hdd_chunk_find(chunkid, chunkType);
	if (chunk == NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	int status = hdd_write(chunk, version, blocknum, offset, size, crc, buffer);
	hdd_chunk_release(chunk);
	return status;
}

/* chunk info */

int hdd_check_version(uint64_t chunkid, uint32_t version) {
	TRACETHIS2(chunkid, version);
	Chunk *c;
	c = hdd_chunk_find(chunkid, slice_traits::standard::ChunkPartType());
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		PRINTTHIS(LIZARDFS_ERROR_WRONGVERSION);
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	hdd_chunk_release(c);
	return LIZARDFS_STATUS_OK;
}

int hdd_get_blocks(uint64_t chunkid, ChunkPartType chunkType, uint32_t version, uint16_t *blocks) {
	TRACETHIS1(chunkid);
	Chunk *c;
	c = hdd_chunk_find(chunkid, chunkType);
	*blocks = 0;
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	*blocks = c->blocks;
	hdd_chunk_release(c);
	return LIZARDFS_STATUS_OK;
}

/* chunk operations */
static int hdd_chunk_overwrite_version(Chunk* c, uint32_t newVersion) {
	assert(c);
	IF_MOOSEFS_CHUNK(mc, c) {
		(void)mc;
		std::vector<uint8_t> buffer;
		serialize(buffer, newVersion);
		{
			FolderWriteStatsUpdater updater(c->owner, buffer.size());
			if (pwrite(c->fd, buffer.data(), buffer.size(), ChunkSignature::kVersionOffset)
					!= static_cast<ssize_t>(buffer.size())) {
				updater.markWriteAsFailed();
				return LIZARDFS_ERROR_IO;
			}
		}
		hdd_stats_overheadwrite(buffer.size());
	}
	c->version = newVersion;
	return LIZARDFS_STATUS_OK;
}

std::pair<int, Chunk *> hdd_int_create_chunk(uint64_t chunkid, uint32_t version,
		ChunkPartType chunkType) {
	TRACETHIS2(chunkid, version);
	folder *f;
	int status;

	Chunk *chunk = nullptr;
	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		f = hdd_getfolder();
		if (f == nullptr) {
			return std::pair<int, Chunk*>(LIZARDFS_ERROR_NOSPACE, nullptr);
		}
		chunk = hdd_chunk_create(f, chunkid, chunkType, version, ChunkFormat::IMPROPER);
	}
	if (chunk == nullptr) {
		return std::pair<int, Chunk*>(LIZARDFS_ERROR_CHUNKEXIST, nullptr);
	}

	status = hdd_io_begin(chunk, 1);
	PRINTTHIS(status);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		hdd_chunk_delete(chunk);
		return std::pair<int, Chunk*>(LIZARDFS_ERROR_IO, nullptr);
	}

	IF_MOOSEFS_CHUNK(mc, chunk) {
		memset(hdd_get_header_buffer(), 0, mc->getHeaderSize());
		uint8_t *ptr = hdd_get_header_buffer();
		serialize(&ptr, ChunkSignature(chunkid, version, chunkType));
		{
			FolderWriteStatsUpdater updater(chunk->owner, mc->getHeaderSize());
			if (write(chunk->fd, hdd_get_header_buffer(), mc->getHeaderSize()) !=
				static_cast<ssize_t>(mc->getHeaderSize())) {
				hdd_error_occured(chunk);  // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING, "create_newchunk: file:%s - write error",
								chunk->filename().c_str());
				hdd_io_end(chunk);
				unlink(chunk->filename().c_str());
				hdd_chunk_delete(chunk);
				updater.markWriteAsFailed();
				return std::pair<int, Chunk*>(LIZARDFS_ERROR_IO, nullptr);
			}
		}
		hdd_stats_overheadwrite(mc->getHeaderSize());
	}
	status = hdd_io_end(chunk);
	PRINTTHIS(status);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		unlink(chunk->filename().c_str());
		hdd_chunk_delete(chunk);
		return std::pair<int, Chunk*>(status, nullptr);
	}
	return std::pair<int, Chunk*>(LIZARDFS_STATUS_OK, chunk);
}

int hdd_int_create(uint64_t chunkid, uint32_t version, ChunkPartType chunkType) {
	TRACETHIS2(chunkid, version);

	stats_create++;

	auto result = hdd_int_create_chunk(chunkid, version, chunkType);
	if (result.first == LIZARDFS_STATUS_OK) {
		hdd_chunk_release(result.second);
	}
	return result.first;
}

static int hdd_int_test(uint64_t chunkid, uint32_t version, ChunkPartType chunkType) {
	TRACETHIS2(chunkid, version);
	uint16_t block;
		int status;
	Chunk *c;
	uint8_t *blockbuffer;

	stats_test++;

	blockbuffer = hdd_get_block_buffer();
	c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,0);
	PRINTTHIS(status);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	status = LIZARDFS_STATUS_OK; // will be overwritten in the loop below if the test fails
	for (block=0 ; block<c->blocks ; block++) {
		auto readBytes = hdd_int_read_block_and_crc(c, blockbuffer, block, "test_chunk");
		uint8_t *data_in_buffer = blockbuffer + sizeof(uint32_t); // Skip crc
		if (readBytes < 0) {
			status = LIZARDFS_ERROR_IO;
			break;
		}
		hdd_stats_overheadread(readBytes);
		const uint8_t* crcBuffPointer = blockbuffer;
		if (get32bit(&crcBuffPointer) != mycrc32(0, data_in_buffer, MFSBLOCKSIZE)) {
			errno = 0;      // set anything to errno
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_pretty_syslog(LOG_WARNING, "test_chunk: file:%s - crc error", c->filename().c_str());
			status = LIZARDFS_ERROR_CRC;
			break;
		}
	}
#ifdef LIZARDFS_HAVE_POSIX_FADVISE
	// Always advise the OS that tested chunks should not be cached. Don't rely on
	// hdd_delayed_ops to do it for us, because it may be disabled using a config file.
	posix_fadvise(c->fd,0,0,POSIX_FADV_DONTNEED);
#endif /* LIZARDFS_HAVE_POSIX_FADVISE */
	if (status != LIZARDFS_STATUS_OK) {
		// test failed -- chunk is damaged
		hdd_io_end(c);
		hdd_chunk_release(c);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return LIZARDFS_STATUS_OK;
}

static int hdd_int_duplicate(uint64_t chunkId, uint32_t chunkVersion, uint32_t chunkNewVersion,
		ChunkPartType chunkType, uint64_t copyChunkId, uint32_t copyChunkVersion) {
	TRACETHIS();
	folder *f;
	uint16_t block;
	int32_t retsize;
	int status;
	Chunk *c,*oc;
	uint8_t *blockbuffer;

	stats_duplicate++;

	blockbuffer = hdd_get_block_buffer();

	oc = hdd_chunk_find(chunkId, chunkType);
	if (oc==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (oc->version!=chunkVersion && chunkVersion>0) {
		hdd_chunk_release(oc);
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	if (copyChunkVersion==0) {
		copyChunkVersion = chunkNewVersion;
	}
	{
		std::unique_lock<std::mutex> folderlock_guard(folderlock);
		f = hdd_getfolder();
		if (f==NULL) {
			folderlock_guard.unlock();
			hdd_chunk_release(oc);
			return LIZARDFS_ERROR_NOSPACE;
		}
		c = hdd_chunk_create(f, copyChunkId, chunkType, copyChunkVersion, oc->chunkFormat());
	}
	if (c==NULL) {
		hdd_chunk_release(oc);
		return LIZARDFS_ERROR_CHUNKEXIST;
	}
	sassert(c->chunkFormat() == oc->chunkFormat());

	if (chunkNewVersion != chunkVersion) {
		if (c->renameChunkFile(chunkNewVersion) < 0) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - rename error", oc->filename().c_str());
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return LIZARDFS_ERROR_IO;
		}
		status = hdd_io_begin(oc, 0, chunkVersion);
		if (status!=LIZARDFS_STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;  //can't change file version
		}
		status = hdd_chunk_overwrite_version(oc, chunkNewVersion);
		if (status != LIZARDFS_STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - write error", c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return LIZARDFS_ERROR_IO;
		}
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=LIZARDFS_STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_report_damaged_chunk(chunkId, chunkType);
			hdd_chunk_release(oc);
			return status;
		}
	}
	status = hdd_io_begin(c,1);
	if (status!=LIZARDFS_STATUS_OK) {
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
		uint8_t *mc_crc_data = gOpenChunks.getResource(mc->fd).crc_data();
		uint8_t *moc_crc_data = gOpenChunks.getResource(moc->fd).crc_data();
		memcpy(mc_crc_data, moc_crc_data, mc->getCrcBlockSize());
		memcpy(hdd_get_header_buffer() + mc->getCrcOffset(), moc_crc_data, mc->getCrcBlockSize());
		{
			FolderWriteStatsUpdater updater(mc->owner, mc->getHeaderSize());
			if (write(mc->fd, hdd_get_header_buffer(), mc->getHeaderSize()) != static_cast<ssize_t>(mc->getHeaderSize())) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"duplicate_chunk: file:%s - hdr write error", c->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				updater.markWriteAsFailed();
				return LIZARDFS_ERROR_IO;
			}
		}
		hdd_stats_overheadwrite(mc->getHeaderSize());
	}
	lseek(oc->fd, c->getBlockOffset(0), SEEK_SET);
	for (block=0 ; block<oc->blocks ; block++) {
		{
			FolderReadStatsUpdater updater(oc->owner, blockSize);
			retsize = read(oc->fd, blockbuffer, blockSize);
			if (retsize!=blockSize) {
				hdd_error_occured(oc);  // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"duplicate_chunk: file:%s - data read error", c->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_report_damaged_chunk(chunkId, chunkType);
				hdd_chunk_release(oc);
				updater.markReadAsFailed();
				return LIZARDFS_ERROR_IO;
			}
		}
		hdd_stats_overheadread(blockSize);
		{
			FolderWriteStatsUpdater updater(c->owner, blockSize);
			retsize = write(c->fd, blockbuffer, blockSize);
			if (retsize!=blockSize) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"duplicate_chunk: file:%s - data write error", c->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				updater.markWriteAsFailed();
				return LIZARDFS_ERROR_IO;        //write error
			}
		}
		hdd_stats_overheadwrite(blockSize);
	}
	status = hdd_io_end(oc);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(oc);  // uses and preserves errno !!!
		hdd_io_end(c);
		unlink(c->filename().c_str());
		hdd_chunk_delete(c);
		hdd_report_damaged_chunk(chunkId, chunkType);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename().c_str());
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = oc->blocks;
	folderlock.lock();
	c->owner->needrefresh = 1;
	folderlock.unlock();
	hdd_chunk_release(c);
	hdd_chunk_release(oc);
	return LIZARDFS_STATUS_OK;
}

int hdd_int_version(Chunk *chunk, uint32_t version, uint32_t newversion) {
	TRACETHIS();
	int status;
	assert(chunk);
	if (chunk->version != version && version > 0) {
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	if (chunk->renameChunkFile(newversion) < 0) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING, "set_chunk_version: file:%s - rename error",
		                   chunk->filename().c_str());
		return LIZARDFS_ERROR_IO;
	}
	status = hdd_io_begin(chunk, 0, version);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING, "set_chunk_version: file:%s - open error",
		                   chunk->filename().c_str());
		return status;
	}
	status = hdd_chunk_overwrite_version(chunk, newversion);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING, "set_chunk_version: file:%s - write error",
		                   chunk->filename().c_str());
		hdd_io_end(chunk);
		return LIZARDFS_ERROR_IO;
	}
	status = hdd_io_end(chunk);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(chunk);  // uses and preserves errno !!!
	}
	return status;
}

int hdd_int_version(uint64_t chunkid, uint32_t version, uint32_t newversion,
		ChunkPartType chunkType) {
	TRACETHIS();

	stats_version++;

	Chunk *c = hdd_chunk_find(chunkid, chunkType);
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	int status = hdd_int_version(c, version, newversion);
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_truncate(uint64_t chunkId, ChunkPartType chunkType, uint32_t oldVersion,
		uint32_t newVersion, uint32_t length) {
	TRACETHIS4(chunkId, oldVersion, newVersion, length);
	int status;
	Chunk *c;
	uint32_t blocks;
	uint32_t i;
	uint8_t *blockbuffer;

	stats_truncate++;

	blockbuffer = hdd_get_block_buffer();
	if (length>MFSCHUNKSIZE) {
		return LIZARDFS_ERROR_WRONGSIZE;
	}
	c = hdd_chunk_find(chunkId, chunkType);

	// step 1 - change version
	if (c==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (c->version!=oldVersion && oldVersion>0) {
		hdd_chunk_release(c);
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	if (c->renameChunkFile(newVersion) < 0) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"truncate_chunk: file:%s - rename error", c->filename().c_str());
		hdd_chunk_release(c);
		return LIZARDFS_ERROR_IO;
	}
	status = hdd_io_begin(c, 0, oldVersion);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;  //can't change file version
	}
	status = hdd_chunk_overwrite_version(c, newVersion);
	if (status != LIZARDFS_STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING,
				"truncate_chunk: file:%s - write error", c->filename().c_str());
		hdd_io_end(c);
		hdd_chunk_release(c);
		return LIZARDFS_ERROR_IO;
	}
	c->wasChanged = true;

	// step 2. truncate
	blocks = ((length + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE);
	if (blocks>c->blocks) {
		IF_MOOSEFS_CHUNK(mc, c) {
			uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data();
			for (auto block = c->blocks; block < blocks; block++) {
				memcpy(crc_data + block * sizeof(uint32_t), &emptyblockcrc, sizeof(uint32_t));
			}
		}
		if (ftruncate(c->fd, c->getFileSizeFromBlockCount(blocks)) < 0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"truncate_chunk: file:%s - ftruncate error", c->filename().c_str());
			hdd_io_end(c);
			hdd_chunk_release(c);
			return LIZARDFS_ERROR_IO;
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
				return LIZARDFS_ERROR_IO;
			}
		}
		if (ftruncate(c->fd, c->getFileSizeFromBlockCount(blocks)) < 0) {
			hdd_error_occured(c);   // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"truncate_chunk: file:%s - ftruncate error", c->filename().c_str());
			hdd_io_end(c);
			hdd_chunk_release(c);
			return LIZARDFS_ERROR_IO;
		}
		if (lastPartialBlockSize>0) {
			auto offset = c->getBlockOffset(fullBlocks);
			if (c->chunkFormat() == ChunkFormat::INTERLEAVED) {
				offset += 4;
			}
			{
				FolderReadStatsUpdater updater(c->owner, lastPartialBlockSize);
				if (pread(c->fd, blockbuffer, lastPartialBlockSize, offset)
						!= static_cast<ssize_t>(lastPartialBlockSize)) {
					hdd_error_occured(c);   // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
							"truncate_chunk: file:%s - read error", c->filename().c_str());
					hdd_io_end(c);
					hdd_chunk_release(c);
					updater.markReadAsFailed();
					return LIZARDFS_ERROR_IO;
				}
			}
			hdd_stats_overheadread(lastPartialBlockSize);
			i = mycrc32_zeroexpanded(0,blockbuffer,lastPartialBlockSize,MFSBLOCKSIZE-lastPartialBlockSize);
			uint8_t crcBuff[sizeof(uint32_t)];
			uint8_t* crcBuffPointer = crcBuff;
			put32bit(&crcBuffPointer, i);
			IF_MOOSEFS_CHUNK(mc, c) {
				sassert(c->chunkFormat() == ChunkFormat::MOOSEFS);
				uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data();
				memcpy(crc_data + fullBlocks * sizeof(uint32_t), crcBuff, sizeof(uint32_t));
				for (auto block = fullBlocks + 1; block < c->blocks; block++) {
					memcpy(crc_data + block * sizeof(uint32_t), &emptyblockcrc, sizeof(uint32_t));
				}
			} else {
				sassert(c->chunkFormat() == ChunkFormat::INTERLEAVED);
				{
					FolderWriteStatsUpdater updater(c->owner, 4);
					if (pwrite(c->fd, crcBuff, 4, c->getBlockOffset(fullBlocks)) != 4) {
						hdd_error_occured(c);   // uses and preserves errno !!!
						lzfs_silent_errlog(LOG_WARNING,
								"truncate_chunk: file:%s - write crc error", c->filename().c_str());
						hdd_report_damaged_chunk(chunkId, chunkType);
						hdd_chunk_release(c);
						updater.markWriteAsFailed();
						return LIZARDFS_ERROR_IO;
					}
				}
			}
		}
	}
	if (c->blocks != blocks) {
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		c->owner->needrefresh = 1;
	}
	c->blocks = blocks;
	status = hdd_io_end(c);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
	}
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_duptrunc(uint64_t chunkId, uint32_t chunkVersion, uint32_t chunkNewVersion,
		ChunkPartType chunkType, uint64_t copyChunkId, uint32_t copyChunkVersion,
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

	stats_duptrunc++;

	blockbuffer = hdd_get_block_buffer();
	hdrbuffer = hdd_get_header_buffer();

	if (copyChunkLength>MFSCHUNKSIZE) {
		return LIZARDFS_ERROR_WRONGSIZE;
	}
	oc = hdd_chunk_find(chunkId, chunkType);
	if (oc==NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (oc->version!=chunkVersion && chunkVersion>0) {
		hdd_chunk_release(oc);
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	if (copyChunkVersion==0) {
		copyChunkVersion = chunkNewVersion;
	}
	{
		std::unique_lock<std::mutex> folderlock_guard(folderlock);
		f = hdd_getfolder();
		if (f==NULL) {
			folderlock_guard.unlock();
			hdd_chunk_release(oc);
			return LIZARDFS_ERROR_NOSPACE;
		}
		c = hdd_chunk_create(f, copyChunkId, chunkType, copyChunkVersion, oc->chunkFormat());
	}
	if (c==NULL) {
		hdd_chunk_release(oc);
		return LIZARDFS_ERROR_CHUNKEXIST;
	}

	if (chunkNewVersion!=chunkVersion) {
		if (oc->renameChunkFile(chunkNewVersion) < 0) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duplicate_chunk: file:%s - rename error", oc->filename().c_str());
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return LIZARDFS_ERROR_IO;
		}
		status = hdd_io_begin(oc, 0, chunkVersion);
		if (status!=LIZARDFS_STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;  //can't change file version
		}
		status = hdd_chunk_overwrite_version(oc, chunkNewVersion);
		if (status != LIZARDFS_STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			lzfs_silent_errlog(LOG_WARNING,
					"duptrunc_chunk: file:%s - write error", c->filename().c_str());
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return LIZARDFS_ERROR_IO;
		}
	} else {
		status = hdd_io_begin(oc,0);
		if (status!=LIZARDFS_STATUS_OK) {
			hdd_error_occured(oc);  // uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_report_damaged_chunk(chunkId, chunkType);
			hdd_chunk_release(oc);
			return status;
		}
	}
	status = hdd_io_begin(c,1);
	if (status!=LIZARDFS_STATUS_OK) {
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
		uint8_t *crc_data = gOpenChunks.getResource(moc->fd).crc_data();
		memcpy(hdrbuffer + mc->getCrcOffset(), crc_data, mc->getCrcBlockSize());
	}
	lseek(c->fd, c->getBlockOffset(0), SEEK_SET);
	lseek(oc->fd, c->getBlockOffset(0), SEEK_SET);
	if (blocks>oc->blocks) { // expanding
		for (block=0 ; block<oc->blocks ; block++) {
			{
				FolderReadStatsUpdater updater(oc->owner, blockSize);
				retsize = read(oc->fd, blockbuffer, blockSize);
				if (retsize!=blockSize) {
					hdd_error_occured(oc);  // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
							"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
					hdd_io_end(c);
					unlink(c->filename().c_str());
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_report_damaged_chunk(chunkId, chunkType);
					hdd_chunk_release(oc);
					updater.markReadAsFailed();
					return LIZARDFS_ERROR_IO;
				}
			}
			hdd_stats_overheadread(blockSize);
			{
				FolderWriteStatsUpdater updater(c->owner, blockSize);
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
					updater.markWriteAsFailed();
					return LIZARDFS_ERROR_IO;
				}
			}
			hdd_stats_overheadwrite(blockSize);
		}
		if (mc) {
			for (block = oc->blocks; block < blocks; block++) {
				memcpy(hdrbuffer + mc->getCrcOffset() + sizeof(uint32_t) * block, &emptyblockcrc, sizeof(uint32_t));
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
			return LIZARDFS_ERROR_IO;        //write error
		}
	} else { // shrinking
		uint32_t lastBlockSize = copyChunkLength - (copyChunkLength / MFSBLOCKSIZE) * MFSBLOCKSIZE;
		if (lastBlockSize==0) { // aligned shrink
			for (block=0 ; block<blocks ; block++) {
				{
					FolderReadStatsUpdater updater(oc->owner, blockSize);
					retsize = read(oc->fd, blockbuffer, blockSize);
					if (retsize!=blockSize) {
						hdd_error_occured(oc);  // uses and preserves errno !!!
						lzfs_silent_errlog(LOG_WARNING,
								"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
						hdd_io_end(c);
						unlink(c->filename().c_str());
						hdd_chunk_delete(c);
						hdd_io_end(oc);
						hdd_report_damaged_chunk(chunkId, chunkType);
						hdd_chunk_release(oc);
						updater.markReadAsFailed();
						return LIZARDFS_ERROR_IO;
					}
				}
				hdd_stats_overheadread(blockSize);
				{
					FolderWriteStatsUpdater updater(c->owner, blockSize);
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
						updater.markWriteAsFailed();
						return LIZARDFS_ERROR_IO;
					}
				}
				hdd_stats_overheadwrite(blockSize);
			}
		} else { // misaligned shrink
			for (block=0 ; block<blocks-1 ; block++) {
				{
					FolderReadStatsUpdater updater(oc->owner, blockSize);
					retsize = read(oc->fd, blockbuffer, blockSize);
					if (retsize!=blockSize) {
						hdd_error_occured(oc);  // uses and preserves errno !!!
						lzfs_silent_errlog(LOG_WARNING,
								"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
						hdd_io_end(c);
						unlink(c->filename().c_str());
						hdd_chunk_delete(c);
						hdd_io_end(oc);
						hdd_report_damaged_chunk(chunkId, chunkType);
						hdd_chunk_release(oc);
						updater.markReadAsFailed();
						return LIZARDFS_ERROR_IO;
					}
				}
				hdd_stats_overheadread(blockSize);
				{
					FolderWriteStatsUpdater updater(c->owner, blockSize);
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
						updater.markWriteAsFailed();
						return LIZARDFS_ERROR_IO;        //write error
					}
				}
				hdd_stats_overheadwrite(blockSize);
			}
			block = blocks-1;
			auto toBeRead = c->chunkFormat() == ChunkFormat::MOOSEFS ? lastBlockSize : lastBlockSize + 4;
			{
				FolderReadStatsUpdater updater(oc->owner, toBeRead);
				retsize = read(oc->fd, blockbuffer, toBeRead);
				if (retsize!=(signed)toBeRead) {
					hdd_error_occured(oc);  // uses and preserves errno !!!
					lzfs_silent_errlog(LOG_WARNING,
						"duptrunc_chunk: file:%s - data read error", oc->filename().c_str());
					hdd_io_end(c);
					unlink(c->filename().c_str());
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_report_damaged_chunk(chunkId, chunkType);
					hdd_chunk_release(oc);
					updater.markReadAsFailed();
					return LIZARDFS_ERROR_IO;
				}
			}
			hdd_stats_overheadread(toBeRead);
			if (c->chunkFormat() == ChunkFormat::INTERLEAVED) {
				crc = mycrc32_zeroexpanded(0, blockbuffer + sizeof(uint32_t), lastBlockSize, MFSBLOCKSIZE - lastBlockSize);
				uint8_t* crcBuffPointer = blockbuffer;
				put32bit(&crcBuffPointer, crc);
			} else {
				auto* ptr = hdrbuffer + mc->getCrcOffset() + sizeof(uint32_t) * block;
				auto crc = mycrc32_zeroexpanded(0, blockbuffer, lastBlockSize, MFSBLOCKSIZE - lastBlockSize);
				put32bit(&ptr,crc);
			}
			memset(blockbuffer + toBeRead, 0, MFSBLOCKSIZE - lastBlockSize);
			{
				FolderWriteStatsUpdater updater(c->owner, blockSize);
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
					updater.markWriteAsFailed();
					return LIZARDFS_ERROR_IO;
				}
			}
			hdd_stats_overheadwrite(blockSize);
		}
	}
	if (mc) {
		uint8_t *crc_data = gOpenChunks.getResource(mc->fd).crc_data();
		memcpy(crc_data, hdrbuffer + mc->getCrcOffset(), mc->getCrcBlockSize());
		lseek(mc->fd,0,SEEK_SET);
		{
			FolderWriteStatsUpdater updater(mc->owner, mc->getHeaderSize());
			if (write(mc->fd, hdrbuffer, mc->getHeaderSize()) != static_cast<ssize_t>(mc->getHeaderSize())) {
				hdd_error_occured(c);   // uses and preserves errno !!!
				lzfs_silent_errlog(LOG_WARNING,
						"duptrunc_chunk: file:%s - hdr write error", c->filename().c_str());
				hdd_io_end(c);
				unlink(c->filename().c_str());
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				updater.markWriteAsFailed();
				return LIZARDFS_ERROR_IO;
			}
		}
		hdd_stats_overheadwrite(mc->getHeaderSize());
	}
	status = hdd_io_end(oc);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(oc);  // uses and preserves errno !!!
		hdd_io_end(c);
		unlink(c->filename().c_str());
		hdd_chunk_delete(c);
		hdd_report_damaged_chunk(chunkId, chunkType);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=LIZARDFS_STATUS_OK) {
		hdd_error_occured(c);   // uses and preserves errno !!!
		unlink(c->filename().c_str());
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = blocks;
	folderlock.lock();
	c->owner->needrefresh = 1;
	folderlock.unlock();
	hdd_chunk_release(c);
	hdd_chunk_release(oc);
	return LIZARDFS_STATUS_OK;
}

int hdd_int_delete(Chunk* chunk, uint32_t version) {
	TRACETHIS();
	assert(chunk);
	if (chunk->version != version && version > 0) {
		hdd_chunk_release(chunk);
		return LIZARDFS_ERROR_WRONGVERSION;
	}
	if (unlink(chunk->filename().c_str()) < 0) {
		uint8_t err = errno;
		hdd_error_occured(chunk);  // uses and preserves errno !!!
		lzfs_silent_errlog(LOG_WARNING, "delete_chunk: file:%s - unlink error",
		                   chunk->filename().c_str());
		if (err == ENOENT) {
			hdd_chunk_delete(chunk);
		} else {
			hdd_chunk_release(chunk);
		}
		return LIZARDFS_ERROR_IO;
	}
	hdd_chunk_delete(chunk);
	return LIZARDFS_STATUS_OK;
}

int hdd_int_delete(uint64_t chunkid, uint32_t version, ChunkPartType chunkType) {
	TRACETHIS();

	stats_delete++;

	Chunk *chunk = hdd_chunk_find(chunkid, chunkType);
	if (chunk == NULL) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	return hdd_int_delete(chunk, version);
}

/* all chunk operations in one call */
// newversion>0 && length==0xFFFFFFFF && copychunkid==0   -> change version
// newversion>0 && length==0xFFFFFFFF && copycnunkid>0    -> duplicate
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid==0    -> truncate
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid>0     -> duplicate and truncate
// newversion==0 && length==0                             -> delete
// newversion==0 && length==1                             -> create
// newversion==0 && length==2                             -> check chunk contents
int hdd_chunkop(uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType,
		uint32_t chunkNewVersion, uint64_t copyChunkId, uint32_t copyChunkVersion,
		uint32_t length) {
	TRACETHIS();
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
			return LIZARDFS_ERROR_EINVAL;
		}
	} else {
		if (length==0) {
			return hdd_int_delete(chunkId, chunkVersion, chunkType);
		} else if (length==1) {
			return hdd_int_create(chunkId, chunkVersion, chunkType);
		} else if (length==2) {
			return hdd_int_test(chunkId, chunkVersion, chunkType);
		} else {
			return LIZARDFS_ERROR_EINVAL;
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
			if (hdd_int_test(chunk.id, chunk.version, chunk.type) !=LIZARDFS_STATUS_OK) {
				lzfs_pretty_syslog(LOG_NOTICE, "Chunk %s corrupted (detected by a client)",
						name.c_str());
				hdd_report_damaged_chunk(chunk.id, chunk.type);
			} else {
				lzfs_pretty_syslog(LOG_NOTICE, "Chunk %s spuriously reported as corrupted",
						name.c_str());
			}
		} catch (UniqueQueueEmptyException&) {
			// hooray, nothing to do
		}
		// rate-limit to 1/sec
		usleep(time.remaining_us());
		terminate = term;
	};
}

void hdd_test_chunk(ChunkWithVersionAndType chunk) {
	test_chunk_queue.put(chunk);
}

void hdd_tester_thread() {
	TRACETHIS();
	folder *f, *of;
	Chunk *c;
	uint64_t chunkid;
	uint32_t version;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();
	uint32_t cnt;
	uint64_t start_us, end_us;

	f = folderhead;
	cnt = 0;
	while (!term) {
		start_us = get_usectime();
		chunkid = 0;
		version = 0;
		{
			std::lock_guard<std::mutex> folderlock_guard(folderlock);
			std::lock_guard<std::mutex> registryLockGuard(gChunkRegistryLock);
			std::lock_guard<std::mutex> testlock_guard(testlock);
			uint8_t testerresetExpected = 1;
			if (testerreset.compare_exchange_strong(testerresetExpected, 0)) {
				f = folderhead;
				cnt = 0;
			}
			cnt += std::min(HDDTestFreq_ms.load(), 1000U);
			if (cnt < HDDTestFreq_ms || folderactions == 0 || folderhead == nullptr) {
				chunkid = 0;
			} else {
				cnt = 0;
				of = f;
				do {
					f = f->next;
					if (f == nullptr) {
						f = folderhead;
					}
				} while ((f->damaged || f->todel || f->toremove || f->scanstate != SCST_WORKING) && of != f);
				if (of == f && (f->damaged || f->todel || f->toremove || f->scanstate != SCST_WORKING)) {
					chunkid = 0;
				} else {
					c = f->testhead;
					if (c && c->state==CH_AVAIL) {
						chunkid = c->chunkid;
						version = c->version;
						chunkType = c->type();
					}
				}
			}
		}
		if (chunkid > 0) {
			if (hdd_int_test(chunkid, version, chunkType) != LIZARDFS_STATUS_OK) {
				hdd_report_damaged_chunk(chunkid, chunkType);
			}
			chunkid = 0;
		}
		end_us = get_usectime();
		if (end_us > start_us) {
			unsigned time_to_sleep_us = 1000 * std::min(HDDTestFreq_ms.load(), 1000U);
			end_us -= start_us;
			if (end_us < time_to_sleep_us) {
				usleep(time_to_sleep_us - end_us);
			}
		}
	}
}

void hdd_scrub_handle(int signal) {
    // SIGUSR2 could be signal number 31, 12 or 17 based on platform
    if ( signal == 31 || signal == 12 || signal == 17 ) {
        if ( ScrubIsRunning ) {
            ScrubIsRunning = 0;
            lzfs_pretty_syslog(LOG_NOTICE, "hdd space manager: scrub aborted");
            return;
        } else {
            ScrubIsRunning = 1;
        }
    }
}

void hdd_scrubber_thread() {
	TRACETHIS();
	folder *f;
	Chunk *c;
	uint64_t chunkid;
	uint32_t version;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();
	uint32_t cnt,damagedCnt,chunksNumber,progress,lastProgress;
	std::string path;
    uint32_t i,chunksno;
    Chunk **csorttab;
	uint32_t starttime, currenttime, elapsedtime, remainingtime, totaltime;

	while ( !term ) {

		if ( !ScrubIsRunning ) {
			usleep(500000);
			continue;
		}

    	cnt = 0;
    	damagedCnt = 0;
    	chunksNumber = folderhead->chunkcount;
    	progress = 0;
    	lastProgress = 0;

        csorttab = (Chunk**) malloc(sizeof(Chunk*)*chunksNumber);
        passert(csorttab);
    	chunksno=0;

    	starttime = time(NULL);

    	lzfs_pretty_syslog(LOG_NOTICE, "hdd space manager: scrub started. Scrubbing %d chunks.", chunksNumber);

    	for (f=folderhead ; f ; f=f->next) {
            	for (c=f->testhead ; c ; c=c->testnext) {
                    	csorttab[chunksno++] = c;
            	}
    	}

		//for (c=f->testhead ; c ; c=c->testnext) {
		for (i=0 ; i<chunksno ; i++) {
            c = csorttab[i];
        	if ( !ScrubIsRunning ) {
                	break;
        	}

			cnt++;
			progress = (cnt * 100.0) / chunksNumber;
			currenttime = time(NULL);
			elapsedtime = currenttime - starttime;
			remainingtime = elapsedtime * (100-progress);

			if ( progress != lastProgress ) {
				lzfs_pretty_syslog(LOG_NOTICE, "hdd space manager: scrub is running (%d%%, %d mins, %d remaining). scrubbed chunks=%d/%d, damaged chunks=%d", progress, elapsedtime/60, remainingtime/60, cnt, chunksNumber, damagedCnt);
			}

			if ( c && c->state==CH_AVAIL && !c->filename().empty() ) {
                chunkid = c->chunkid;
                version = c->version;
                chunkType = c->type();
                path = c->filename();

				if ( hdd_int_test(chunkid, version, chunkType) != LIZARDFS_STATUS_OK ) {
                	hdd_report_damaged_chunk(chunkid, chunkType);
                	damagedCnt++;

                	lzfs_pretty_syslog(LOG_WARNING, "hdd space manager: scrub FAILED on chunk %s, damaged chunks=%d", path.c_str(), damagedCnt);
				}
			}

			lastProgress = progress;
		}

    	totaltime = (time(NULL)-starttime)/60.0;
    	lzfs_pretty_syslog(LOG_NOTICE, "hdd space manager: scrub ended in %d minutes. scrubbed chunks=%d, failed chunks=%d", totaltime, cnt, damagedCnt);
    	ScrubIsRunning = 0;
	}
}


void hdd_testshuffle(folder *f) {
	TRACETHIS();
	uint32_t i,j,chunksno;
	Chunk **csorttab,*c;
	std::lock_guard<std::mutex> testlock_guard(testlock);
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
}

/* initialization */

static inline void hdd_add_chunk(folder *f,
		const std::string& fullname,
		uint64_t chunkId,
		ChunkFormat chunkFormat,
		uint32_t version,
		ChunkPartType chunkType,
		uint8_t todel,
		int layout_version) {
	TRACETHIS();
	Chunk *c;

	c = hdd_chunk_get(chunkId, chunkType, CH_NEW_AUTO, chunkFormat);
	if (!c) {
		lzfs_pretty_syslog(LOG_ERR, "Can't use file %s as chunk", fullname.c_str());
		return;
	}

	bool new_chunk = c->filename().empty();

	if (!new_chunk) {
		// already have this chunk
		if (version <= c->version) {
			// current chunk is older
			if (todel < 2) { // this is R/W fs?
				unlink(fullname.c_str()); // if yes then remove file
			}
			hdd_chunk_release(c);
			return;
		}

		if (c->todel < 2) { // current chunk is on R/W fs?
			unlink(c->filename().c_str()); // if yes then remove file
		}
	}

	if (c->chunkFormat() != chunkFormat || !new_chunk) {
		std::lock_guard<std::mutex> registryLockGuard(gChunkRegistryLock);
		c = hdd_chunk_recreate(c, chunkId, chunkType, chunkFormat);
	}

	c->version = version;
	c->blocks = 0;
	c->owner = f;
	c->todel = todel;
	c->setFilenameLayout(layout_version);
	sassert(c->filename() == fullname);
	{
		std::lock_guard<std::mutex> testlock_guard(testlock);
		c->testprev = f->testtail;
		*(c->testprev) = c;
		f->testtail = &(c->testnext);
	}
	if (new_chunk) {
		hdd_report_new_chunk(c->chunkid, c->version, c->todel, c->type());
	}

	hdd_chunk_release(c);
	std::lock_guard<std::mutex> folderlock_guard(folderlock);
	f->chunkcount++;
}

void hdd_convert_chunk_to_ec2(const std::string &subfolder_path, const std::string &name,
		std::string &new_name) {
	std::string::size_type ec_pos;

	ec_pos = name.find("_ec_");
	if (ec_pos == std::string::npos) {
		new_name = name;
		return;
	}

	ChunkFilenameParser parser(name);

	if (parser.parse() != ChunkFilenameParser::OK || !slice_traits::isEC(parser.chunkType())) {
		new_name = name;
		return;
	}

	// drop old parity chunks for parity count greater than 4
	if (slice_traits::ec::isEC2Part(parser.chunkType())) {
		new_name.clear();
		int r = remove((subfolder_path + name).c_str());
		if (r < 0) {
			lzfs_pretty_syslog(LOG_ERR,
			                   "Failed to remove invalid chunk file %s placed in chunk directory %s.",
			                   name.c_str(), subfolder_path.c_str());
		}
		return;
	}

	new_name = name;
	new_name.replace(ec_pos, 4, "_ec2_");

	int r = rename((subfolder_path + name).c_str(), (subfolder_path + new_name).c_str());
	if (r < 0) {
		lzfs_pretty_syslog(LOG_ERR,
		                   "Failed to rename old chunk %s placed in chunk directory %s.",
		                   name.c_str(), subfolder_path.c_str());
		new_name.clear();
		return;
	}
}

/*! \brief Scan folder for new chunks in specific directory layout
 *
 * \param f folder
 * \param begin_time time from start of scan
 * \param layout_version directory and chunk name format identificator
 *                       value 0 corresponds to current layout version
 *                       other values are for older version
 */
void hdd_folder_scan_layout(folder *f, uint32_t begin_time, int layout_version) {
	DIR *dd;
	struct dirent *de;
	uint32_t tcheckcnt;
	uint8_t lastperc, currentperc;
	uint32_t lasttime, currenttime;

	folderlock.lock();
	unsigned scan_state = f->scanstate;
	folderlock.unlock();
	if (scan_state == SCST_SCANTERMINATE) {
		return;
	}

	bool scan_term = false;
	tcheckcnt = 0;
	lastperc = 0;
	lasttime = time(NULL);
	for (unsigned subfolder_number = 0; subfolder_number < Chunk::kNumberOfSubfolders && !scan_term;
	     ++subfolder_number) {
		std::string subfolder_path =
		    f->path + Chunk::getSubfolderNameGivenNumber(subfolder_number, layout_version) + "/";
		dd = opendir(subfolder_path.c_str());
		if (!dd) {
			continue;
		}

		while (!scan_term) {
			de = readdir(dd);
			if (!de) {
				break;
			}

			ChunkFilenameParser filenameParser(de->d_name);
			if (filenameParser.parse() != ChunkFilenameParser::Status::OK) {
				if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0) {
					lzfs_pretty_syslog(LOG_WARNING,
					                   "Invalid file %s placed in chunk directory %s; skipping it.",
					                   de->d_name, subfolder_path.c_str());
				}
				continue;
			}
			if (Chunk::getSubfolderNumber(filenameParser.chunkId(), layout_version) !=
			    subfolder_number) {
				lzfs_pretty_syslog(LOG_WARNING,
				                   "Chunk %s%s placed in a wrong directory; skipping it.",
				                   subfolder_path.c_str(), de->d_name);
				continue;
			}

			std::string chunk_name = de->d_name;
			hdd_convert_chunk_to_ec2(subfolder_path, de->d_name, chunk_name);

			if(chunk_name.empty()) {
				continue;
			}

			hdd_add_chunk(f, subfolder_path + chunk_name, filenameParser.chunkId(),
			              filenameParser.chunkFormat(), filenameParser.chunkVersion(),
			              filenameParser.chunkType(), f->todel, layout_version);
			tcheckcnt++;
			if (tcheckcnt >= 1000) {
				std::lock_guard<std::mutex> folderlock_guard(folderlock);
				if (f->scanstate == SCST_SCANTERMINATE) {
					scan_term = true;
				}
				tcheckcnt = 0;
			}
		}
		closedir(dd);

		currenttime = time(NULL);
		currentperc = (subfolder_number * 100.0) / 256.0;
		if (currentperc > lastperc && currenttime > lasttime) {
			lastperc = currentperc;
			lasttime = currenttime;
			folderlock.lock();
			f->scanprogress = currentperc;
			folderlock.unlock();
			hddspacechanged = 1;  // report chunk count to master
			lzfs_pretty_syslog(LOG_NOTICE, "scanning folder %s: %" PRIu8 "%% (%" PRIu32 "s)",
			                   f->path, lastperc, currenttime - begin_time);
		}
	}
}

/*! \brief Moves/renames chunks from old layout to current
 *
 * \param f folder
 * \param layout_version layout version that is going to be converted to current layout
 *
 * \return number of chunks moved/renamed
 */
int64_t hdd_folder_migrate_directories(folder *f, int layout_version) {
	DIR *dd;
	struct dirent *de;
	int64_t count = 0;

	assert(layout_version > 0);

	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		if (f->migratestate == MGST_MIGRATETERMINATE) {
			return count;
		}
	}

	bool scan_term = false;
	int check_cnt = 0;
	for (unsigned subfolder_number = 0; subfolder_number < Chunk::kNumberOfSubfolders && !scan_term;
	     ++subfolder_number) {
		std::string subfolder_path =
		    f->path + Chunk::getSubfolderNameGivenNumber(subfolder_number, layout_version) + "/";
		dd = opendir(subfolder_path.c_str());
		if (!dd) {
			continue;
		}

		while (!scan_term) {
			de = readdir(dd);
			if (!de) {
				break;
			}

			ChunkFilenameParser filenameParser(de->d_name);
			if (filenameParser.parse() != ChunkFilenameParser::Status::OK) {
				continue;
			}

			if (Chunk::getSubfolderNumber(filenameParser.chunkId(), layout_version) !=
			    subfolder_number) {
				continue;
			}

			Chunk *chunk = hdd_chunk_find(filenameParser.chunkId(), filenameParser.chunkType());
			if (!chunk) {
				continue;
			}

			if (chunk->filename() != (subfolder_path + de->d_name)) {
				hdd_chunk_release(chunk);
				continue;
			}

			if (chunk->renameChunkFile(chunk->version) < 0) {
				std::string old_path = subfolder_path + de->d_name;
				std::string new_path = chunk->generateFilenameForVersion(chunk->version);
				lzfs_pretty_syslog(LOG_WARNING, "Can't migrate %s to %s: %s", old_path.c_str(),
				                   new_path.c_str(), strerr(errno));
				// Probably something is really wrong (ro fs, wrong permissions,
				// new dirs on a different mountpoint) -- don't try to move any chunks more.
				scan_term = true;
			}
			hdd_chunk_release(chunk);
			count++;

			check_cnt++;
			if (check_cnt >= 100) {
				std::lock_guard<std::mutex> folderlock_guard(folderlock);
				if (f->migratestate == MGST_MIGRATETERMINATE) {
					scan_term = true;
				}
				check_cnt = 0;
			}

			// micro sleep to reduce load on disk as migrate doesn't have to finish fast
			if (!scan_term) {
				usleep(1000);
			}
		}
		closedir(dd);

		if (!scan_term && rmdir(subfolder_path.c_str()) != 0) {
			lzfs_pretty_syslog(LOG_WARNING, "Can't remove old directory %s: %s",
			                   subfolder_path.c_str(), strerr(errno));
		}
	}

	return count;
}

void *hdd_folder_migrate(void *arg) {
	TRACETHIS();
	folder *f = (folder *)arg;

	uint32_t begin_time = time(NULL);

	int64_t count = hdd_folder_migrate_directories(f, 1);

	std::lock_guard<std::mutex> folderlock_guard(folderlock);
	if (f->migratestate != MGST_MIGRATETERMINATE) {
		if (count > 0) {
			lzfs_pretty_syslog(LOG_NOTICE,
			                   "converting directories in folder %s: complete (%" PRIu32 "s)",
			                   f->path, (uint32_t)(time(NULL)) - begin_time);
		}
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "converting directories in folder %s: interrupted", f->path);
	}
	f->migratestate = MGST_MIGRATEFINISHED;

	return NULL;
}

void *hdd_folder_scan(void *arg) {
	TRACETHIS();
	folder *f = (folder *)arg;

	uint32_t begin_time = time(NULL);

	gScansInProgress++;

	unsigned todel = 0;
	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		todel = f->todel;
		hdd_refresh_usage(f);
	}

	if (todel == 0) {
		mkdir(f->path, 0755);
	}

	hddspacechanged = 1;

	if (todel == 0) {
		for (unsigned subfolderNumber = 0; subfolderNumber < Chunk::kNumberOfSubfolders;
		     ++subfolderNumber) {
			std::string subfolderPath =
			    f->path + Chunk::getSubfolderNameGivenNumber(subfolderNumber, 0);
			mkdir(subfolderPath.c_str(), 0755);
		}
	}

	hdd_folder_scan_layout(f, begin_time, 1);
	hdd_folder_scan_layout(f, begin_time, 0);
	hdd_testshuffle(f);
	gScansInProgress--;

	std::lock_guard<std::mutex> folderlock_guard(folderlock);
	if (f->scanstate == SCST_SCANTERMINATE) {
		lzfs_pretty_syslog(LOG_NOTICE, "scanning folder %s: interrupted", f->path);
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "scanning folder %s: complete (%" PRIu32 "s)", f->path,
		                   (uint32_t)(time(NULL)) - begin_time);
	}

	if (f->scanstate != SCST_SCANTERMINATE && f->migratestate == MGST_MIGRATEDONE) {
		f->migratestate = MGST_MIGRATEINPROGRESS;
		f->migratethread = std::thread(hdd_folder_migrate, f);
	}

	f->scanstate = SCST_SCANFINISHED;
	f->scanprogress = 100;

	return NULL;
}

bool hdd_scans_in_progress() {
	return gScansInProgress != 0;
}

void hdd_folders_thread() {
	TRACETHIS();
	while (!term) {
		hdd_check_folders();
		sleep(1);
	}
}

void hdd_free_resources_thread() {
	static const int kDelayedStep = 2;
	static const int kMaxFreeUnused = 1024;
	TRACETHIS();

	while (!term) {
		gOpenChunks.freeUnused(eventloop_time(), gChunkRegistryLock, kMaxFreeUnused);
		sleep(kDelayedStep);
	}
}

void hdd_term(void) {
	TRACETHIS();
	uint32_t i;
	folder *f,*fn;
	cntcond *cc,*ccn;

	i = term.exchange(1); // if term is non zero here then it means that threads have not been started, so do not join with them
	if (i==0) {
        scrubberthread.join();
		testerthread.join();
		foldersthread.join();
		delayedthread.join();
		try {
			test_chunk_thread.join();
		} catch (std::system_error &e) {
			lzfs_pretty_syslog(LOG_NOTICE, "Failed to join test chunk thread: %s", e.what());
		}
	}
	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		i = 0;
		for (f = folderhead; f; f = f->next) {
			if (f->scanstate == SCST_SCANINPROGRESS) {
				f->scanstate = SCST_SCANTERMINATE;
			}
			if (f->scanstate == SCST_SCANTERMINATE || f->scanstate == SCST_SCANFINISHED) {
				i++;
			}
			if (f->migratestate == MGST_MIGRATEINPROGRESS) {
				f->migratestate = MGST_MIGRATETERMINATE;
			}
			if (f->migratestate == MGST_MIGRATETERMINATE || f->migratestate == MGST_MIGRATEFINISHED) {
				i++;
			}
		}
	}
//      syslog(LOG_NOTICE,"waiting for scanning threads (%" PRIu32 ")",i);
	while (i>0) {
		usleep(10000); // not very elegant solution.
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		for (f=folderhead ; f ; f=f->next) {
			if (f->scanstate == SCST_SCANFINISHED) {
				f->scanthread.join();
				f->scanstate = SCST_WORKING;  // any state - to prevent calling join again
				i--;
			}
			if (f->migratestate == MGST_MIGRATEFINISHED) {
				f->migratethread.join();
				f->migratestate = MGST_MIGRATEDONE;
				i--;
			}
		}
	}

	for (auto &chunkEntry : gChunkRegistry) {
		Chunk *c = chunkEntry.second.get();
		if (c->state==CH_AVAIL) {
			MooseFSChunk* mc = dynamic_cast<MooseFSChunk*>(c);
			if (c->wasChanged && mc) {
				lzfs_pretty_syslog(LOG_WARNING,"hdd_term: CRC not flushed - writing now");
				if (chunk_writecrc(mc) != LIZARDFS_STATUS_OK) {
					lzfs_silent_errlog(LOG_WARNING,
							"hdd_term: file: %s - write error", c->filename().c_str());
				}
			}
			gOpenChunks.purge(c->fd);
		} else {
			lzfs::log_warn("hdd_term: locked chunk !!! (chunkid: {:#04x}, chunktype: {})", c->chunkid, c->type().toString());
		}
	}
	// Delete chunks even not in AVAILABLE state here, as all threads using chunk objects should already be joined
	// (by this function and other cleanup functions of other chunkserver modules that are registered on eventloop termination)
	// This function should always be executed after all other chunkserver modules' (that use chunk objects) cleanup functions
	// were executed.
	gChunkRegistry.clear();
	gOpenChunks.freeUnused(eventloop_time(), gChunkRegistryLock);

	for (f = folderhead ; f ; f = fn) {
		fn = f->next;
		if (f->lfd >= 0) {
			close(f->lfd);
		}
		free(f->path);
		delete f;
	}
	for (cc = cclist; cc; cc = ccn) {
		ccn = cc->next;
		if (cc->wcnt) {
			lzfs_pretty_syslog(LOG_WARNING, "hddspacemgr (atexit): used cond !!!");
		}
		delete cc;
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
	uint32_t l;
	int damaged,lfd,td;
	char *pptr;
	struct stat sb;
	folder *f;
	uint8_t lockneeded;

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
	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		lockneeded = 1;
		for (f=folderhead ; f && lockneeded ; f=f->next) {
			if (strcmp(f->path,pptr)==0) {
				lockneeded = 0;
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
		std::unique_lock<std::mutex> folderlock_guard(folderlock);
		for (f=folderhead ; f ; f=f->next) {
			if (f->lfd>=0 && f->devid==sb.st_dev) {
				if (f->lockinode==sb.st_ino) {
					std::string fPath = f->path;
					folderlock_guard.unlock();
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
	}
	std::unique_lock<std::mutex> folderlock_guard(folderlock);
	for (f=folderhead ; f ; f=f->next) {
		if (strcmp(f->path,pptr)==0) {
			f->toremove = 0;
			if (f->damaged) {
				f->scanstate = SCST_SCANNEEDED;
				f->scanprogress = 0;
				f->damaged = damaged;
				f->avail = 0ULL;
				f->total = 0ULL;
				f->leavefree = gLeaveFree;
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
			folderlock_guard.unlock();
			if (lfd>=0) {
				close(lfd);
			}
			return 1;
		}
	}
	f = new folder();
	passert(f);
	f->todel = td;
	f->damaged = damaged;
	f->scanstate = SCST_SCANNEEDED;
	f->scanprogress = 0;
	f->migratestate = MGST_MIGRATEDONE;
	f->path = strdup(pptr);
	passert(f->path);
	f->toremove = 0;
	f->leavefree = gLeaveFree;
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
				APP_EXAMPLES_SUBDIR "/mfshdd.cfg");
	}
	lzfs_pretty_syslog(LOG_INFO, "hdd configuration file %s opened", hddfname.c_str());

	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		folderactions = 0; // stop folder actions
		for (f=folderhead ; f ; f=f->next) {
			f->toremove = 1;
		}
	}

	char buff[1000];
	while (fgets(buff,999,fd.get())) {
		buff[999] = 0;
		hdd_parseline(buff);
	}
	fd.reset();

	bool anyDiskAvailable = false;
	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		for (f=folderhead ; f ; f=f->next) {
			if (f->toremove==0) {
				anyDiskAvailable = true;
				if (f->scanstate==SCST_SCANNEEDED) {
					lzfs_pretty_syslog(LOG_NOTICE,"hdd space manager: folder %s will be scanned",f->path);
				} else if (f->scanstate==SCST_SENDNEEDED) {
					lzfs_pretty_syslog(LOG_NOTICE,"hdd space manager: folder %s will be resend",f->path);
				} else {
					lzfs_pretty_syslog(LOG_NOTICE,"hdd space manager: folder %s didn't change",f->path);
				}
			} else {
				lzfs_pretty_syslog(LOG_NOTICE,"hdd space manager: folder %s will be removed",f->path);
			}
		}
		folderactions = 1; // continue folder actions
	}

	std::unique_lock<std::mutex> folderlock_lock(folderlock);
	std::vector<std::string> paths;
	for (f = folderhead; f; f = f->next) {
		paths.emplace_back(f->path);
	}
	folderlock_lock.unlock();

	gIoStat.resetPaths(paths);

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
			lzfs_pretty_syslog(LOG_INFO,"new chunks format set to 'MOOSEFS' format");
		}
	} else {
		if (defaultChunkFormat != ChunkFormat::INTERLEAVED) {
			MooseFSChunkFormat = false;
			lzfs_pretty_syslog(LOG_INFO,"new chunks format set to 'INTERLEAVED' format");
		}
	}
}

void hdd_reload(void) {
	TRACETHIS();
	gAdviseNoCache = cfg_getuint32("HDD_ADVISE_NO_CACHE", 0);

	PerformFsync = cfg_getuint32("PERFORM_FSYNC", 1);

	HDDTestFreq_ms = cfg_ranged_get("HDD_TEST_FREQ", 10., 0.001, 1000000.) * 1000;

	gPunchHolesInFiles = cfg_getuint32("HDD_PUNCH_HOLES", 0);

	hdd_int_set_chunk_format();
	char *LeaveFreeStr = cfg_getstr("HDD_LEAVE_SPACE_DEFAULT", gLeaveSpaceDefaultDefaultStrValue);
	if (hdd_size_parse(LeaveFreeStr,&gLeaveFree)<0) {
		lzfs_pretty_syslog(LOG_NOTICE,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT parse error - left unchanged");
	}
	free(LeaveFreeStr);
	if (gLeaveFree<0x4000000) {
		lzfs_pretty_syslog(LOG_NOTICE,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT < chunk size - leaving so small space on hdd is not recommended");
	}

	lzfs_pretty_syslog(LOG_NOTICE,"reloading hdd data ...");
	try {
		hdd_folders_reinit();
	} catch (const Exception& ex) {
		lzfs_pretty_syslog(LOG_ERR, "%s", ex.what());
	}
}

int hdd_late_init(void) {
	TRACETHIS();
	term = 0;
	testerthread = std::thread(hdd_tester_thread);
	foldersthread = std::thread(hdd_folders_thread);
	delayedthread = std::thread(hdd_free_resources_thread);
    scrubberthread = std::thread(hdd_scrubber_thread);
	try {
		test_chunk_thread = std::thread(hdd_test_chunk_thread);
	} catch (std::system_error &e) {
		lzfs_pretty_syslog(LOG_ERR, "Failed to create test chunk thread: %s", e.what());
		abort();
	}
	return 0;
}

int hdd_init(void) {
	TRACETHIS();
	folder *f;
	char *LeaveFreeStr;

#ifndef LIZARDFS_HAVE_THREAD_LOCAL
	zassert(pthread_key_create(&hdrbufferkey, free));
	zassert(pthread_key_create(&blockbufferkey, free));
#endif // LIZARDFS_HAVE_THREAD_LOCAL

    // SIGUSR2 will start or abort a scrub
    signal(SIGUSR2, hdd_scrub_handle);

	uint8_t *emptyblockcrc_buf = (uint8_t*)&emptyblockcrc;
	put32bit(&emptyblockcrc_buf, mycrc32_zeroblock(0,MFSBLOCKSIZE));

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

	{
		std::lock_guard<std::mutex> folderlock_guard(folderlock);
		for (f=folderhead ; f ; f=f->next) {
			lzfs_pretty_syslog(LOG_INFO, "hdd space manager: path to scan: %s",f->path);
		}
	}
	lzfs_pretty_syslog(LOG_INFO, "hdd space manager: start background hdd scanning "
				"(searching for available chunks)");

	gAdviseNoCache = cfg_getuint32("HDD_ADVISE_NO_CACHE", 0);
	HDDTestFreq_ms = cfg_ranged_get("HDD_TEST_FREQ", 10., 0.001, 1000000.) * 1000;

	gPunchHolesInFiles = cfg_getuint32("HDD_PUNCH_HOLES", 0);

	MooseFSChunkFormat = true;
	hdd_int_set_chunk_format();
	eventloop_reloadregister(hdd_reload);
	eventloop_timeregister(TIMEMODE_RUN_LATE,60,0,hdd_diskinfo_movestats);
	eventloop_destructregister(hdd_term);

	term = 1;

	return 0;
}
