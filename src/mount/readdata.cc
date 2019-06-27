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
#include "mount/readdata.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <condition_variable>
#include <map>
#include <mutex>

#include "common/connection_pool.h"
#include "common/datapack.h"
#include "common/exceptions.h"
#include "common/mfserr.h"
#include "common/read_plan_executor.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "mount/chunk_locator.h"
#include "mount/chunk_reader.h"
#include "mount/mastercomm.h"
#include "mount/readahead_adviser.h"
#include "mount/readdata_cache.h"
#include "mount/tweaks.h"
#include "protocol/MFSCommunication.h"

#define USECTICK 333333
#define REFRESHTICKS 15

#define MAPBITS 10
#define MAPSIZE (1<<(MAPBITS))
#define MAPMASK (MAPSIZE-1)
#define MAPINDX(inode) (inode&MAPMASK)

static std::atomic<uint32_t> gReadaheadMaxWindowSize;
static std::atomic<uint32_t> gCacheExpirationTime_ms;

struct readrec {
	ChunkReader reader;
	ReadCache cache;
	ReadaheadAdviser readahead_adviser;
	std::vector<uint8_t> read_buffer;
	uint32_t inode;
	uint8_t refreshCounter;         // gMutex
	bool expired;                   // gMutex
	struct readrec *next;           // gMutex
	struct readrec *mapnext;        // gMutex

	readrec(uint32_t inode, ChunkConnector& connector, double bandwidth_overuse)
			: reader(connector, bandwidth_overuse),
			  cache(gCacheExpirationTime_ms),
			  readahead_adviser(gCacheExpirationTime_ms, gReadaheadMaxWindowSize),
			  inode(inode),
			  refreshCounter(0),
			  expired(false),
			  next(nullptr),
			  mapnext(nullptr) {
	}
};

static ConnectionPool gReadConnectionPool;
static ChunkConnectorUsingPool gChunkConnector(gReadConnectionPool);
static std::mutex gMutex;
static readrec *rdinodemap[MAPSIZE];
static readrec *rdhead=NULL;
static pthread_t delayedOpsThread;
static std::atomic<uint32_t> gChunkserverConnectTimeout_ms;
static std::atomic<uint32_t> gChunkserverWaveReadTimeout_ms;
static std::atomic<uint32_t> gChunkserverTotalReadTimeout_ms;
static std::atomic<bool> gPrefetchXorStripes;
static bool readDataTerminate;
static std::atomic<uint32_t> maxRetries;
static double gBandwidthOveruse;

const unsigned ReadaheadAdviser::kInitWindowSize;
const unsigned ReadaheadAdviser::kDefaultWindowSizeLimit;
const int ReadaheadAdviser::kRandomThreshold;
const int ReadaheadAdviser::kHistoryEntryLifespan_ns;
const int ReadaheadAdviser::kHistoryCapacity;
const unsigned ReadaheadAdviser::kHistoryValidityThreshold;

uint32_t read_data_get_wave_read_timeout_ms() {
	return gChunkserverWaveReadTimeout_ms;
}

uint32_t read_data_get_connect_timeout_ms() {
	return gChunkserverConnectTimeout_ms;
}

uint32_t read_data_get_total_read_timeout_ms() {
	return gChunkserverTotalReadTimeout_ms;
}

bool read_data_get_prefetchxorstripes() {
	return gPrefetchXorStripes;
}

void* read_data_delayed_ops(void *arg) {
	readrec *rrec,**rrecp;
	readrec **rrecmap;
	(void)arg;
	for (;;) {
		gReadConnectionPool.cleanup();
		std::unique_lock<std::mutex> lock(gMutex);
		if (readDataTerminate) {
			return NULL;
		}
		rrecp = &rdhead;
		while ((rrec = *rrecp) != NULL) {
			if (rrec->refreshCounter < REFRESHTICKS) {
				rrec->refreshCounter++;
			}
			if (rrec->expired) {
				*rrecp = rrec->next;
				rrecmap = &(rdinodemap[MAPINDX(rrec->inode)]);
				while (*rrecmap) {
					if ((*rrecmap)==rrec) {
						*rrecmap = rrec->mapnext;
					} else {
						rrecmap = &((*rrecmap)->mapnext);
					}
				}
				delete rrec;
			} else {
				rrecp = &(rrec->next);
			}
		}
		lock.unlock();
		usleep(USECTICK);
	}
}

void* read_data_new(uint32_t inode) {
	readrec *rrec = new readrec(inode, gChunkConnector, gBandwidthOveruse);
	std::unique_lock<std::mutex> lock(gMutex);
	rrec->next = rdhead;
	rdhead = rrec;
	rrec->mapnext = rdinodemap[MAPINDX(inode)];
	rdinodemap[MAPINDX(inode)] = rrec;
	return rrec;
}

void read_data_end(void* rr) {
	readrec *rrec = (readrec*)rr;

	std::unique_lock<std::mutex> lock(gMutex);
	rrec->expired = true;
}

void read_data_init(uint32_t retries,
		uint32_t chunkserverRoundTripTime_ms,
		uint32_t chunkserverConnectTimeout_ms,
		uint32_t chunkServerWaveReadTimeout_ms,
		uint32_t chunkserverTotalReadTimeout_ms,
		uint32_t cache_expiration_time_ms,
		uint32_t readahead_max_window_size_kB,
		bool prefetchXorStripes,
		double bandwidth_overuse) {
	uint32_t i;
	pthread_attr_t thattr;

	readDataTerminate = false;
	for (i=0 ; i<MAPSIZE ; i++) {
		rdinodemap[i]=NULL;
	}
	maxRetries=retries;
	gChunkserverConnectTimeout_ms = chunkserverConnectTimeout_ms;
	gChunkserverWaveReadTimeout_ms = chunkServerWaveReadTimeout_ms;
	gChunkserverTotalReadTimeout_ms = chunkserverTotalReadTimeout_ms;
	gCacheExpirationTime_ms = cache_expiration_time_ms;
	gReadaheadMaxWindowSize = readahead_max_window_size_kB * 1024;
	gPrefetchXorStripes = prefetchXorStripes;
	gBandwidthOveruse = bandwidth_overuse;
	gTweaks.registerVariable("PrefetchXorStripes", gPrefetchXorStripes);
	gChunkConnector.setRoundTripTime(chunkserverRoundTripTime_ms);
	gChunkConnector.setSourceIp(fs_getsrcip());
	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);
	pthread_create(&delayedOpsThread,&thattr,read_data_delayed_ops,NULL);
	pthread_attr_destroy(&thattr);

	gTweaks.registerVariable("ReadMaxRetries", maxRetries);
	gTweaks.registerVariable("ReadConnectTimeout", gChunkserverConnectTimeout_ms);
	gTweaks.registerVariable("ReadWaveTimeout", gChunkserverWaveReadTimeout_ms);
	gTweaks.registerVariable("ReadTotalTimeout", gChunkserverTotalReadTimeout_ms);
	gTweaks.registerVariable("CacheExpirationTime", gCacheExpirationTime_ms);
	gTweaks.registerVariable("ReadaheadMaxWindowSize", gReadaheadMaxWindowSize);
	gTweaks.registerVariable("ReadChunkPrepare", ChunkReader::preparations);
	gTweaks.registerVariable("ReqExecutedTotal", ReadPlanExecutor::executions_total_);
	gTweaks.registerVariable("ReqExecutedUsingAll", ReadPlanExecutor::executions_with_additional_operations_);
	gTweaks.registerVariable("ReqFinishedUsingAll", ReadPlanExecutor::executions_finished_by_additional_operations_);
}

void read_data_term(void) {
	readrec *rr,*rrn;

	{
		std::unique_lock<std::mutex> lock(gMutex);
		readDataTerminate = true;
	}

	pthread_join(delayedOpsThread,NULL);
	for (rr = rdhead ; rr ; rr = rrn) {
		rrn = rr->next;
		delete rr;
	}
	for (auto& rr : rdinodemap) {
		rr = NULL;
	}
	rdhead = NULL;
}

void read_inode_ops(uint32_t inode) { // attributes of inode have been changed - force reconnect and clear cache
	readrec *rrec;
	std::unique_lock<std::mutex> lock(gMutex);
	for (rrec = rdinodemap[MAPINDX(inode)] ; rrec ; rrec=rrec->mapnext) {
		if (rrec->inode == inode) {
			rrec->refreshCounter = REFRESHTICKS; // force reconnect on forthcoming access
		}
	}
}

int read_data_sleep_time_ms(int tryCounter) {
	if (tryCounter <= 13) {            // 2^13 = 8192
		return (1 << tryCounter);  // 2^tryCounter milliseconds
	} else {
		return 1000 * 10;          // 10 seconds
	}
}

static void print_error_msg(const readrec *rrec, uint32_t try_counter, const Exception &ex) {
	if (rrec->reader.isChunkLocated()) {
		lzfs_pretty_syslog(LOG_WARNING,
		                   "read file error, inode: %u, index: %u, chunk: %" PRIu64 ", version: %u - %s "
		                   "(try counter: %u)", rrec->reader.inode(), rrec->reader.index(),
		                   rrec->reader.chunkId(), rrec->reader.version(), ex.what(), try_counter);
	} else {
		lzfs_pretty_syslog(LOG_WARNING,
		                   "read file error, inode: %u, index: %u, chunk: failed to locate - %s "
		                   "(try counter: %u)", rrec->reader.inode(), rrec->reader.index(),
		                   ex.what(), try_counter);
	}
}

static int read_to_buffer(readrec *rrec, uint64_t current_offset, uint64_t bytes_to_read,
		std::vector<uint8_t> &read_buffer, uint64_t *bytes_read) {
	uint32_t try_counter = 0;
	uint32_t prepared_inode = 0; // this is always different than any real inode
	uint32_t prepared_chunk_id = 0;
	assert(*bytes_read == 0);

	// forced sleep between retries caused by recoverable failures
	uint32_t sleep_time_ms = 0;

	std::unique_lock<std::mutex> lock(gMutex);
	bool force_prepare = (rrec->refreshCounter == REFRESHTICKS);
	lock.unlock();

	while (bytes_to_read > 0) {
		Timeout sleep_timeout = Timeout(std::chrono::milliseconds(sleep_time_ms));
		// Increase communicationTimeout to sleepTime; longer poll() can't be worse
		// than short poll() followed by nonproductive usleep().
		uint32_t timeout_ms = std::max(gChunkserverTotalReadTimeout_ms.load(), sleep_time_ms);
		Timeout communication_timeout = Timeout(std::chrono::milliseconds(timeout_ms));
		sleep_time_ms = 0;
		try {
			uint32_t chunk_id = current_offset / MFSCHUNKSIZE;
			if (force_prepare || prepared_inode != rrec->inode || prepared_chunk_id != chunk_id) {
				rrec->reader.prepareReadingChunk(rrec->inode, chunk_id, force_prepare);
				prepared_chunk_id = chunk_id;
				prepared_inode = rrec->inode;
				force_prepare = false;
				lock.lock();
				rrec->refreshCounter = 0;
				lock.unlock();
			}

			uint64_t offset_of_chunk = static_cast<uint64_t>(chunk_id) * MFSCHUNKSIZE;
			uint32_t offset_in_chunk = current_offset - offset_of_chunk;
			uint32_t size_in_chunk = MFSCHUNKSIZE - offset_in_chunk;
			if (size_in_chunk > bytes_to_read) {
				size_in_chunk = bytes_to_read;
			}
			uint32_t bytes_read_from_chunk = rrec->reader.readData(
					read_buffer, offset_in_chunk, size_in_chunk,
					gChunkserverConnectTimeout_ms, gChunkserverWaveReadTimeout_ms,
					communication_timeout, gPrefetchXorStripes);
			// No exceptions thrown. We can increase the counters and go to the next chunk
			*bytes_read += bytes_read_from_chunk;
			current_offset += bytes_read_from_chunk;
			bytes_to_read -= bytes_read_from_chunk;
			if (bytes_read_from_chunk < size_in_chunk) {
				// end of file
				break;
			}
			try_counter = 0;
		} catch (UnrecoverableReadException &ex) {
			print_error_msg(rrec, try_counter, ex);
			if (ex.status() == LIZARDFS_ERROR_ENOENT) {
				return LIZARDFS_ERROR_EBADF; // stale handle
			} else {
				return LIZARDFS_ERROR_IO;
			}
		} catch (Exception &ex) {
			if (try_counter > 0) {
				print_error_msg(rrec, try_counter, ex);
			}
			force_prepare = true;
			if (try_counter > maxRetries) {
				return LIZARDFS_ERROR_IO;
			} else {
				usleep(sleep_timeout.remaining_us());
				sleep_time_ms = read_data_sleep_time_ms(try_counter);
			}
			try_counter++;
		}
	}
	return LIZARDFS_STATUS_OK;
}

int read_data(void *rr, uint64_t offset, uint32_t size, ReadCache::Result &ret) {
	readrec *rrec = (readrec*)rr;
	assert(size % MFSBLOCKSIZE == 0);
	assert(offset % MFSBLOCKSIZE == 0);

	if (size == 0) {
		return LIZARDFS_STATUS_OK;
	}

	rrec->readahead_adviser.feed(offset, size);

	ReadCache::Result result = rrec->cache.query(offset, size);

	if (result.frontOffset() <= offset && offset + size <= result.endOffset()) {
		ret = std::move(result);
		return LIZARDFS_STATUS_OK;
	}
	uint64_t request_offset = result.remainingOffset();
	uint64_t bytes_to_read_left = std::max<uint64_t>(size, rrec->readahead_adviser.window()) - (request_offset - offset);
	bytes_to_read_left = (bytes_to_read_left + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE * MFSBLOCKSIZE;

	uint64_t bytes_read = 0;
	int err = read_to_buffer(rrec, request_offset, bytes_to_read_left, result.inputBuffer(), &bytes_read);
	if (err) {
		// paranoia check - discard any leftover bytes from incorrect read
		result.inputBuffer().clear();
		return err;
	}

	ret = std::move(result);
	return LIZARDFS_STATUS_OK;
}
