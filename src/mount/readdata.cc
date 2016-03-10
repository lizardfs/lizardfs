/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2016 Skytechnology sp. z o.o..

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
#include <mutex>

#include "common/connection_pool.h"
#include "common/datapack.h"
#include "protocol/MFSCommunication.h"
#include "common/mfserr.h"
#include "common/read_plan_executor.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "mount/chunk_locator.h"
#include "mount/chunk_reader.h"
#include "mount/exceptions.h"
#include "mount/mastercomm.h"
#include "mount/tweaks.h"

#define USECTICK 333333
#define REFRESHTICKS 15

#define MAPBITS 10
#define MAPSIZE (1<<(MAPBITS))
#define MAPMASK (MAPSIZE-1)
#define MAPINDX(inode) (inode&MAPMASK)

struct readrec {
	ChunkReader reader;
	std::vector<uint8_t> readBufer;
	uint32_t inode;
	uint8_t refreshCounter;         // gMutex
	bool expired;                   // gMutex
	struct readrec *next;           // gMutex
	struct readrec *mapnext;        // gMutex

	readrec(uint32_t inode, ChunkConnector& connector, double bandwidth_overuse)
			: reader(connector, bandwidth_overuse),
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

void read_inode_ops(uint32_t inode) { // attributes of inode have been changed - force reconnect
	readrec *rrec;
	std::unique_lock<std::mutex> lock(gMutex);
	for (rrec = rdinodemap[MAPINDX(inode)] ; rrec ; rrec=rrec->mapnext) {
		if (rrec->inode==inode) {
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

int read_data(void *rr, uint64_t offset, uint32_t *size, uint8_t **buff) {
	readrec *rrec = (readrec*)rr;
	sassert(size != NULL);
	sassert(buff != NULL);
	sassert(*buff == NULL);
	sassert(*size % MFSBLOCKSIZE == 0);
	sassert(offset % MFSBLOCKSIZE == 0);

	std::unique_lock<std::mutex> lock(gMutex);
	bool forcePrepare = (rrec->refreshCounter == REFRESHTICKS);
	lock.unlock();

	if (*size == 0) {
		return 0;
	}

	uint32_t tryCounter = 0;
	uint64_t currentOffset = offset;
	uint32_t bytesToReadLeft = *size;
	uint32_t bytesRead = 0;

	// We will reserve some more space. This might be helpful when reading
	// xored chunks with missing parts
	rrec->readBufer.resize(0);
	rrec->readBufer.reserve(bytesToReadLeft + 2 * MFSBLOCKSIZE);

	uint32_t preparedInode = 0; // this is always different than any real inode
	uint32_t preparedChunkIndex = 0;

	// forced sleep between retries caused by recoverable failures
	uint32_t sleepTime_ms = 0;

	auto printErrorMessage = [&rrec, &tryCounter] (const Exception& ex) {
		if (rrec->reader.isChunkLocated()) {
			lzfs_pretty_syslog(LOG_WARNING,
					"read file error, inode: %" PRIu32
					", index: %" PRIu32 ", chunk: %" PRIu64 ", version: %" PRIu32 " - %s "
					"(try counter: %" PRIu32 ")",
					rrec->reader.inode(),
					rrec->reader.index(),
					rrec->reader.chunkId(),
					rrec->reader.version(),
					ex.what(),
					tryCounter);
		} else {
			lzfs_pretty_syslog(LOG_WARNING,
					"read file error, inode: %" PRIu32
					", index: %" PRIu32 ", chunk: failed to locate - %s "
					"(try counter: %" PRIu32 ")",
					rrec->reader.inode(),
					rrec->reader.index(),
					ex.what(),
					tryCounter);
		}
	};

	while (bytesToReadLeft > 0) {
		Timeout sleepTimeout = Timeout(std::chrono::milliseconds(sleepTime_ms));
		// Increase communicationTimeout to sleepTime; longer poll() can't be worse
		// than short poll() followed by nonproductive usleep().
		uint32_t timeout_ms = std::max(gChunkserverTotalReadTimeout_ms.load(), sleepTime_ms);
		Timeout communicationTimeout = Timeout(std::chrono::milliseconds(timeout_ms));
		sleepTime_ms = 0;
		try {
			uint32_t chunkIndex = currentOffset / MFSCHUNKSIZE;
			if (forcePrepare || preparedInode != rrec->inode || preparedChunkIndex != chunkIndex) {
				rrec->reader.prepareReadingChunk(rrec->inode, chunkIndex, forcePrepare);
				preparedChunkIndex = chunkIndex;
				preparedInode = rrec->inode;
				forcePrepare = false;
				lock.lock();
				rrec->refreshCounter = 0;
				lock.unlock();
			}

			uint64_t offsetOfChunk = static_cast<uint64_t>(chunkIndex) * MFSCHUNKSIZE;
			uint32_t offsetInChunk = currentOffset - offsetOfChunk;
			uint32_t sizeInChunk = MFSCHUNKSIZE - offsetInChunk;
			if (sizeInChunk > bytesToReadLeft) {
				sizeInChunk = bytesToReadLeft;
			}
			uint32_t bytesReadFromChunk = rrec->reader.readData(
					rrec->readBufer, offsetInChunk, sizeInChunk,
					gChunkserverConnectTimeout_ms, gChunkserverWaveReadTimeout_ms,
					communicationTimeout, gPrefetchXorStripes);
			// No exceptions thrown. We can increase the counters and go to the next chunk
			bytesRead += bytesReadFromChunk;
			currentOffset += bytesReadFromChunk;
			bytesToReadLeft -= bytesReadFromChunk;
			if (bytesReadFromChunk < sizeInChunk) {
				// end of file
				break;
			}
			tryCounter = 0;
		} catch (UnrecoverableReadException &ex) {
			printErrorMessage(ex);
			if (ex.status() == LIZARDFS_ERROR_ENOENT) {
				return EBADF; // stale handle
			} else {
				return EIO;
			}
		} catch (Exception &ex) {
			if (tryCounter > 0) {
				printErrorMessage(ex);
			}
			forcePrepare = true;
			if (tryCounter > maxRetries) {
				return EIO;
			} else {
				usleep(sleepTimeout.remaining_us());
				sleepTime_ms = read_data_sleep_time_ms(tryCounter);
			}
			tryCounter++;
		}
	}

	*size = bytesRead;
	*buff = rrec->readBufer.data();
	return 0;
}
