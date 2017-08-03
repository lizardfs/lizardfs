/*
 Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2017 Skytechnology sp. z o.o..

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
#include "mount/writedata.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <vector>

#include "common/chunk_connector.h"

#include "common/crc.h"
#include "common/datapack.h"
#include "common/exceptions.h"
#include "common/goal.h"
#include "common/massert.h"
#include "common/message_receive_buffer.h"
#include "common/mfserr.h"
#include "common/multi_buffer_writer.h"
#include "common/pcqueue.h"
#include "common/slice_traits.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"
#include "mount/chunk_writer.h"
#include "mount/global_chunkserver_stats.h"
#include "mount/mastercomm.h"
#include "mount/readdata.h"
#include "mount/tweaks.h"
#include "mount/write_cache_block.h"
#include "protocol/cltocs.h"
#include "protocol/MFSCommunication.h"

#define IDLE_CONNECTION_TIMEOUT 6
#define IDHASHSIZE 256
#define IDHASH(inode) (((inode)*0xB239FB71)%IDHASHSIZE)

namespace {

struct inodedata {
	uint32_t inode;
	uint64_t maxfleng;
	int status;
	uint16_t flushwaiting;
	uint16_t writewaiting;
	uint16_t lcnt;
	uint32_t trycnt;
	bool inqueue; // true it this inode is waiting in one of the queues or is being processed
	uint32_t minimumBlocksToWrite;
	std::list<WriteCacheBlock> dataChain;
	int alterations_in_chain; // number of adherent blocks with different chunk ids in chain
	std::condition_variable flushcond; // wait for !inqueue (flush)
	std::condition_variable writecond; // wait for flushwaiting==0 (write)
	inodedata *next;
	std::unique_ptr<WriteChunkLocator> locator;
	int newDataInChainPipe[2];
	bool workerWaitingForData;
	Timer lastWriteToDataChain;
	Timer lastWriteToChunkservers;

	inodedata(uint32_t inode)
			: inode(inode),
			  maxfleng(0),
			  status(LIZARDFS_STATUS_OK),
			  flushwaiting(0),
			  writewaiting(0),
			  lcnt(0),
			  trycnt(0),
			  inqueue(false),
			  minimumBlocksToWrite(1),
			  alterations_in_chain(),
			  next(nullptr),
			  workerWaitingForData(false) {
#ifdef _WIN32
		// We don't use inodeData->waitingworker and inodeData->pipe on Cygwin because
		// Cygwin's implementation of mixed socket & pipe polling is very inefficient.
		// On mingw platform pipes are unavailable.
		newDataInChainPipe[0] = newDataInChainPipe[1] = -1;
#else
		if (pipe(newDataInChainPipe) < 0) {
			lzfs_pretty_syslog(LOG_WARNING, "creating pipe error: %s", strerr(errno));
			newDataInChainPipe[0] = -1;
		}
#endif
	}

	~inodedata() {
		if (isDataChainPipeValid()) {
			close(newDataInChainPipe[0]);
			close(newDataInChainPipe[1]);
		}
	}

	/* glock: LOCKED */
	void wakeUpWorkerIfNecessary() {
		/*
		 * Write worker always looks for the first block in chain and we modify or add always the
		 * last block in chain so it is necessary to wake up write worker only if the first block
		 * is the last one, ie. dataChain.size() == 1.
		 */
		if (workerWaitingForData && dataChain.size() == 1 && isDataChainPipeValid()) {
			if (write(newDataInChainPipe[1], " ", 1) != 1) {
				lzfs_pretty_syslog(LOG_ERR, "write pipe error: %s", strerr(errno));
			}
			workerWaitingForData = false;
		}
	}

	/* glock: UNUSED */
	bool isDataChainPipeValid() const {
		return newDataInChainPipe[0] >= 0;
	}

	/*! Check if inode requires flushing all its data chain to chunkservers.
	 *
	 * Returns true if anyone requested to flush the data by calling write_data_flush
	 * or write_data_flush_inode or the data in data chain is too old to keep it longer in
	 * our buffers. If this function returns false, we write only full stripes from data
	 * chain to chunkservers.
	 * glock: LOCKED
	 */
	bool requiresFlushing() const {
		return (flushwaiting > 0
				|| lastWriteToDataChain.elapsed_ms() >= kMaximumTimeInDataChainSinceLastWrite_ms
				|| lastWriteToChunkservers.elapsed_ms() >= kMaximumTimeInDataChainSinceLastFlush_ms);
	}

	void pushToChain(WriteCacheBlock &&block) {
		dataChain.push_back(std::move(block));
		if (dataChain.size() > 1 && dataChain.back().chunkIndex != std::next(dataChain.rbegin())->chunkIndex) {
			alterations_in_chain++;
		}
	}

	void popFromChain() {
		assert(dataChain.size() > 0);
		if (dataChain.size() > 1 && dataChain.front().chunkIndex != std::next(dataChain.begin())->chunkIndex) {
			alterations_in_chain--;
		}
		dataChain.pop_front();
	}

	void registerAlterationsInChain(int delta) {
		alterations_in_chain += delta;
	}

	bool hasMultipleChunkIdsInChain() const {
		return alterations_in_chain > 0;
	}

private:
	/*! Limit for \p lastWriteToChunkservers after which we force a flush.
	 *
	 * Maximum time for data to be kept in data chain waiting for collecting a full stripe.
	 */
	static const uint32_t kMaximumTimeInDataChainSinceLastFlush_ms = 15000;

	/*! Limit for \p lastWriteToDataChain after which we force a flush.
	 *
	 * Maximum time for data to be kept in data chain waiting for collecting a full stripe
	 * if no new data is written into the data chain
	 */
	static const uint32_t kMaximumTimeInDataChainSinceLastWrite_ms = 5000;
};

struct DelayedQueueEntry {
	inodedata *inodeData;
	int32_t ticksLeft;
	static constexpr int kTicksPerSecond = 10;

	DelayedQueueEntry(inodedata *inodeData, int32_t ticksLeft)
			: inodeData(inodeData),
			  ticksLeft(ticksLeft) {
	}
};

} // anonymous namespace

static std::atomic<uint32_t> maxretries;
static std::mutex gMutex;
typedef std::unique_lock<std::mutex> Glock;

static std::condition_variable fcbcond;
static uint32_t fcbwaiting = 0;
static int64_t freecacheblocks;
static inodedata **idhash;

static uint32_t gWriteWindowSize;
static uint32_t gChunkserverTimeout_ms;

// percentage of the free cache (1% - 100%) which can be used by one inode
static uint32_t gCachePerInodePercentage;

static pthread_t delayed_queue_worker_th;
static std::vector<pthread_t> write_worker_th;

static void* jqueue;
static std::list<DelayedQueueEntry> delayedQueue;

static ConnectionPool gChunkserverConnectionPool;
static ChunkConnectorUsingPool gChunkConnector(gChunkserverConnectionPool);

void write_cb_release_blocks(uint32_t count, Glock&) {
	freecacheblocks += count;
	if (fcbwaiting > 0 && freecacheblocks > 0) {
		fcbcond.notify_all();
	}
}

void write_cb_acquire_blocks(uint32_t count, Glock&) {
	freecacheblocks -= count;
}

void write_cb_wait_for_block(inodedata* id, Glock& glock) {
	LOG_AVG_TILL_END_OF_SCOPE0("write_cb_wait_for_block");
	fcbwaiting++;
	uint64_t dataChainSize = id->dataChain.size();
	while (freecacheblocks <= 0
			// dataChainSize / (dataChainSize + freecacheblocks) > gCachePerInodePercentage / 100
			// really means "0 > 0"
			|| dataChainSize * 100 > (dataChainSize + freecacheblocks) * gCachePerInodePercentage)
	{
		fcbcond.wait(glock);
	}
	fcbwaiting--;
}

/* inode */

inodedata* write_find_inodedata(uint32_t inode, Glock&) {
	uint32_t idh = IDHASH(inode);
	for (inodedata* id = idhash[idh]; id; id = id->next) {
		if (id->inode == inode) {
			return id;
		}
	}
	return NULL;
}

inodedata* write_get_inodedata(uint32_t inode, Glock&) {
	uint32_t idh = IDHASH(inode);
	inodedata* id;
	for (inodedata* id = idhash[idh]; id; id = id->next) {
		if (id->inode == inode) {
			return id;
		}
	}
	id = new inodedata(inode);
	id->next = idhash[idh];
	idhash[idh] = id;
	return id;
}

void write_free_inodedata(inodedata* fid, Glock&) {
	uint32_t idh = IDHASH(fid->inode);
	inodedata *id, **idp;
	idp = &(idhash[idh]);
	while ((id = *idp)) {
		if (id == fid) {
			*idp = id->next;
			delete id;
			return;
		}
		idp = &(id->next);
	}
}

/* delayed queue */

static void delayed_queue_put(inodedata* id, uint32_t seconds, Glock&) {
	delayedQueue.push_back(DelayedQueueEntry(id, seconds * DelayedQueueEntry::kTicksPerSecond));
}

static bool delayed_queue_remove(inodedata* id, Glock&) {
	for (auto it = delayedQueue.begin(); it != delayedQueue.end(); ++it) {
		if (it->inodeData == id) {
			delayedQueue.erase(it);
			return true;
		}
	}
	return false;
}

void* delayed_queue_worker(void*) {
	for (;;) {
		Timeout timeout(std::chrono::microseconds(1000000 / DelayedQueueEntry::kTicksPerSecond));
		Glock lock(gMutex);
		auto it = delayedQueue.begin();
		while (it != delayedQueue.end()) {
			if (it->inodeData == NULL) {
				return NULL;
			}
			if (--it->ticksLeft <= 0) {
				queue_put(jqueue, 0, 0, reinterpret_cast<uint8_t*>(it->inodeData), 0);
				it = delayedQueue.erase(it);
			} else {
				++it;
			}
		}
		lock.unlock();
		usleep(timeout.remaining_us());
	}
	return NULL;
}

/* queues */

void write_delayed_enqueue(inodedata* id, uint32_t seconds, Glock& lock) {
	if (seconds > 0) {
		delayed_queue_put(id, seconds, lock);
	} else {
		queue_put(jqueue, 0, 0, (uint8_t*) id, 0);
	}
}

void write_enqueue(inodedata* id, Glock&) {
	queue_put(jqueue, 0, 0, (uint8_t*) id, 0);
}

void write_job_delayed_end(inodedata* id, int status, int seconds, Glock &lock) {
	LOG_AVG_TILL_END_OF_SCOPE0("write_job_delayed_end");
	LOG_AVG_TILL_END_OF_SCOPE1("write_job_delayed_end#sec", seconds);
	id->locator.reset();
	if (status != LIZARDFS_STATUS_OK) {
		lzfs_pretty_syslog(LOG_WARNING, "error writing file number %" PRIu32 ": %s", id->inode, lizardfs_error_string(status));
		id->status = status;
	}
	status = id->status;
	if (id->requiresFlushing() > 0) {
		// Don't sleep if we have to write all the data immediately
		seconds = 0;
	}
	if (!id->dataChain.empty() && status == LIZARDFS_STATUS_OK) { // still have some work to do
		id->trycnt = 0; // on good write reset try counter
		write_delayed_enqueue(id, seconds, lock);
	} else {        // no more work or error occurred
		// if this is an error then release all data blocks
		write_cb_release_blocks(id->dataChain.size(), lock);
		id->dataChain.clear();
		id->inqueue = false;
		id->maxfleng = 0; // proper file length is now on the master server, remove our length cache
		if (id->flushwaiting > 0) {
			id->flushcond.notify_all();
		}
	}
}

void write_job_end(inodedata *id, int status, Glock &lock) {
	write_job_delayed_end(id, status, 0, lock);
}

class InodeChunkWriter {
public:
	InodeChunkWriter() : inodeData_(nullptr), chunkIndex_(0) {}
	void processJob(inodedata* data);

private:
	void processDataChain(ChunkWriter& writer);
	void returnJournalToDataChain(std::list<WriteCacheBlock>&& journal, Glock&);
	bool haveAnyBlockInCurrentChunk(Glock&);
	bool haveBlockWorthWriting(uint32_t unfinishedOperationCount, Glock&);
	inodedata* inodeData_;
	uint32_t chunkIndex_;
	Timer wholeOperationTimer;

	// Maximum time of writing one chunk
	static const uint32_t kMaximumTime = 30;
	static const uint32_t kMaximumTimeWhenJobsWaiting = 10;
	// For the last 'kTimeToFinishOperations' seconds of maximumTime we won't start new operations
	static const uint32_t kTimeToFinishOperations = 5;
};

void InodeChunkWriter::processJob(inodedata* inodeData) {
	LOG_AVG_TILL_END_OF_SCOPE0("InodeChunkWriter::processJob");
	inodeData_ = inodeData;

	// First, choose index of some chunk to write
	Glock lock(gMutex);
	int status = inodeData_->status;
	bool haveDataToWrite;
	if (inodeData_->locator) {
		// There is a chunk lock left by a previous unfinished job -- let's finish it!
		chunkIndex_ = inodeData_->locator->chunkIndex();
		haveDataToWrite = haveAnyBlockInCurrentChunk(lock);
	} else if (!inodeData_->dataChain.empty()) {
		// There is no unfinished job, but there is some data to write -- let's start a new job
		chunkIndex_ = inodeData_->dataChain.front().chunkIndex;
		haveDataToWrite = true;
	} else {
		// No data, no unfinished jobs -- something wrong!
		// This should never happen, so the status doesn't really matter
		lzfs_pretty_syslog(LOG_WARNING, "got inode with no data to write!!!");
		haveDataToWrite = false;
		status = LIZARDFS_ERROR_EINVAL;
	}
	if (status != LIZARDFS_STATUS_OK) {
		write_job_end(inodeData_, status, lock);
		return;
	}
	lock.unlock();

	/*  Process the job */
	ChunkWriter writer(globalChunkserverStats, gChunkConnector, inodeData_->newDataInChainPipe[0]);
	wholeOperationTimer.reset();
	std::unique_ptr<WriteChunkLocator> locator = std::move(inodeData_->locator);
	if (!locator) {
		locator.reset(new WriteChunkLocator());
	}

	try {
		try {
			locator->locateAndLockChunk(inodeData_->inode, chunkIndex_);

			// Optimization -- talk with chunkservers only if we have to write any data.
			// Don't do this if we just have to release some previously unlocked lock.
			if (haveDataToWrite) {
				writer.init(locator.get(), gChunkserverTimeout_ms);
				processDataChain(writer);
				writer.finish(kTimeToFinishOperations * 1000);

				Glock lock(gMutex);
				returnJournalToDataChain(writer.releaseJournal(), lock);
			}
			locator->unlockChunk();
			read_inode_ops(inodeData_->inode);

			Glock lock(gMutex);
			inodeData_->minimumBlocksToWrite = writer.getMinimumBlockCountWorthWriting();
			bool canWait = !inodeData_->requiresFlushing();
			if (!haveAnyBlockInCurrentChunk(lock)) {
				// There is no need to wait if we have just finished writing some chunk.
				// Let's immediately start writing the next chunk (if there is any).
				canWait = false;
			}
			if (inodeData_->hasMultipleChunkIdsInChain()) {
				// Don't wait if there is more than one chunk in the data chain -- the first chunk
				// has to be flushed, because no more data will be added to it
				canWait = false;
			}
			write_job_delayed_end(inodeData_, LIZARDFS_STATUS_OK, (canWait ? 1 : 0), lock);
		} catch (Exception& e) {
			std::string errorString = e.what();
			Glock lock(gMutex);
			if (e.status() != LIZARDFS_ERROR_LOCKED) {
				inodeData_->trycnt++;
				errorString += " (try counter: " + std::to_string(inodeData->trycnt) + ")";
			} else if (inodeData_->trycnt == 0) {
				// Set to nonzero to inform writers, that this task needs to wait a bit
				// Don't increase -- LIZARDFS_ERROR_LOCKED means that chunk is locked by a different client
				// and we have to wait until it is unlocked
				inodeData_->trycnt = 1;
			}
			// Keep the lock
			inodeData_->locator = std::move(locator);
			// Move data left in the journal into front of the write cache
			returnJournalToDataChain(writer.releaseJournal(), lock);
			lock.unlock();

			lzfs_pretty_syslog(LOG_WARNING, "write file error, inode: %" PRIu32 ", index: %" PRIu32 " - %s",
					inodeData_->inode, chunkIndex_, errorString.c_str());
			if (inodeData_->trycnt >= maxretries) {
				// Convert error to an unrecoverable error
				throw UnrecoverableWriteException(e.message(), e.status());
			} else {
				// This may be recoverable or unrecoverable error
				throw;
			}
		}
	} catch (UnrecoverableWriteException& e) {
		Glock lock(gMutex);
		if (e.status() == LIZARDFS_ERROR_ENOENT) {
			write_job_end(inodeData_, LIZARDFS_ERROR_EBADF, lock);
		} else if (e.status() == LIZARDFS_ERROR_QUOTA) {
			write_job_end(inodeData_, LIZARDFS_ERROR_QUOTA, lock);
		} else if (e.status() == LIZARDFS_ERROR_NOSPACE || e.status() == LIZARDFS_ERROR_NOCHUNKSERVERS) {
			write_job_end(inodeData_, LIZARDFS_ERROR_NOSPACE, lock);
		} else {
			write_job_end(inodeData_, LIZARDFS_ERROR_IO, lock);
		}
	} catch (Exception& e) {
		Glock lock(gMutex);
		int waitTime = 1;
		if (inodeData_->trycnt > 10) {
			waitTime = std::min<int>(10, inodeData_->trycnt - 9);
		}
		write_delayed_enqueue(inodeData_, waitTime, lock);
	}
}

void InodeChunkWriter::processDataChain(ChunkWriter& writer) {
	LOG_AVG_TILL_END_OF_SCOPE0("InodeChunkWriter::processDataChain");
	uint32_t maximumTime = kMaximumTime;
	bool otherJobsAreWaiting = false;
	while (true) {
		bool newOtherJobsAreWaiting = !queue_isempty(jqueue);
		if (!otherJobsAreWaiting && newOtherJobsAreWaiting) {
			// Some new jobs have just arrived in the queue -- we should finish faster.
			maximumTime = kMaximumTimeWhenJobsWaiting;
			// But we need at least 5 seconds to finish the operations that are in progress
			uint32_t elapsedSeconds = wholeOperationTimer.elapsed_s();
			if (elapsedSeconds + kTimeToFinishOperations >= maximumTime) {
				maximumTime = elapsedSeconds + kTimeToFinishOperations;
			}
		}
		otherJobsAreWaiting = newOtherJobsAreWaiting;

		// If we have sent the previous message and have some time left, we can take
		// another block from current chunk to process it simultaneously. We won't take anything
		// new if we've already sent 'gWriteWindowSize' blocks and didn't receive status from
		// the chunkserver.
		bool can_expect_next_block = true;
		if (wholeOperationTimer.elapsed_s() + kTimeToFinishOperations < maximumTime
				&& writer.acceptsNewOperations()) {
			Glock lock(gMutex);
			// While there is any block worth sending, we add new write operation
			while (haveBlockWorthWriting(writer.getUnfinishedOperationsCount(), lock)) {
				// Remove block from cache and pass it to the writer
				writer.addOperation(std::move(inodeData_->dataChain.front()));
				inodeData_->popFromChain();
				write_cb_release_blocks(1, lock);
			}
			if (inodeData_->requiresFlushing() && !haveAnyBlockInCurrentChunk(lock)) {
				// No more data and some flushing is needed or required, so flush everything
				writer.startFlushMode();
			}
			if (writer.getUnfinishedOperationsCount() < gWriteWindowSize) {
				inodeData_->workerWaitingForData = true;
			}
			can_expect_next_block = haveAnyBlockInCurrentChunk(lock);
		} else if (writer.acceptsNewOperations()) {
			// We are running out of time...
			Glock lock(gMutex);
			if (!inodeData_->requiresFlushing()) {
				// Nobody is waiting for the data to be flushed and the data in write chain
				// isn't too old. Let's postpone any operations
				// that didn't start yet and finish them in the next time slice for this chunk
				writer.dropNewOperations();
			} else {
				// Somebody if waiting for a flush, so we have to finish writing everything.
				writer.startFlushMode();
			}
			can_expect_next_block = haveAnyBlockInCurrentChunk(lock);
		}

		if (writer.startNewOperations(can_expect_next_block) > 0) {
			Glock lock(gMutex);
			inodeData_->lastWriteToChunkservers.reset();
		}
		if (writer.getPendingOperationsCount() == 0) {
			return;
		} else if (wholeOperationTimer.elapsed_s() >= maximumTime) {
			throw RecoverableWriteException(
					"Timeout after " + std::to_string(wholeOperationTimer.elapsed_ms()) + " ms",
					LIZARDFS_ERROR_TIMEOUT);
		}

		// Let's sleep a bit shorter if we can't be woken up by the pipe to reduce the latency
		writer.processOperations(inodeData_->isDataChainPipeValid() ? 50 : 10);
	}
}

void InodeChunkWriter::returnJournalToDataChain(std::list<WriteCacheBlock> &&journal, Glock &lock) {
	if (!journal.empty()) {
		write_cb_acquire_blocks(journal.size(), lock);
		uint64_t prev_id = journal.front().chunkIndex;
		int alterations = (!inodeData_->dataChain.empty()
				&& journal.back().chunkIndex != inodeData_->dataChain.front().chunkIndex) ? 1 : 0;
		for (auto it = std::next(journal.begin()); it != journal.end(); ++it) {
			if (it->chunkIndex != prev_id) {
				alterations++;
				prev_id = it->chunkIndex;
			}
		}
		inodeData_->dataChain.splice(inodeData_->dataChain.begin(), std::move(journal));
		inodeData_->registerAlterationsInChain(alterations);
	}
}

/*
 * Check if there is any data in the same chunk waiting to be written.
 */
bool InodeChunkWriter::haveAnyBlockInCurrentChunk(Glock&) {
	if (inodeData_->dataChain.empty()) {
		return false;
	} else {
		return inodeData_->dataChain.front().chunkIndex == chunkIndex_;
	}
}

/*
 * Check if there is any data worth sending to the chunkserver.
 * We will avoid sending blocks of size different than MFSBLOCKSIZE.
 * These can be taken only if we are close to run out of tasks to do.
 * glock: LOCKED
 */
bool InodeChunkWriter::haveBlockWorthWriting(uint32_t unfinishedOperationCount, Glock& lock) {
	if (!haveAnyBlockInCurrentChunk(lock)) {
		return false;
	}
	const auto& block = inodeData_->dataChain.front();
	if (block.type != WriteCacheBlock::kWritableBlock) {
		// Always write data, that was previously written
		return true;
	} else if (unfinishedOperationCount >= gWriteWindowSize) {
		// Don't start new operations if there is already a lot of pending writes
		return false;
	} else {
		// Always start full blocks; start partial blocks only if we have to flush the data
		// or the block won't be expanded (only the last one can be) to a full block
		return (block.size() == MFSBLOCKSIZE
				|| inodeData_->requiresFlushing()
				|| inodeData_->dataChain.size() > 1);
	}
}

/* main working thread | glock:UNLOCKED */
void* write_worker(void*) {
	InodeChunkWriter inodeDataWriter;
	for (;;) {
		// get next job
		uint32_t z1, z2, z3;
		uint8_t *data;
		{
			LOG_AVG_TILL_END_OF_SCOPE0("write_worker#idle");
			queue_get(jqueue, &z1, &z2, &data, &z3);
		}
		if (data == NULL) {
			return NULL;
		}

		// process the job
		LOG_AVG_TILL_END_OF_SCOPE0("write_worker#working");
		inodeDataWriter.processJob((inodedata*) data);
	}
	return NULL;
}

/* API | glock: INITIALIZED,UNLOCKED */
void write_data_init(uint32_t cachesize, uint32_t retries, uint32_t workers,
		uint32_t writewindowsize, uint32_t chunkserverTimeout_ms, uint32_t cachePerInodePercentage) {
	uint64_t cachebytecount = uint64_t(cachesize) * 1024 * 1024;
	uint64_t cacheblockcount = (cachebytecount / MFSBLOCKSIZE);
	uint32_t i;
	pthread_attr_t thattr;

	gChunkConnector.setSourceIp(fs_getsrcip());
	gWriteWindowSize = writewindowsize;
	gChunkserverTimeout_ms = chunkserverTimeout_ms;
	maxretries = retries;
	if (cacheblockcount < 10) {
		cacheblockcount = 10;
	}

	freecacheblocks = cacheblockcount;
	gCachePerInodePercentage = cachePerInodePercentage;

	idhash = (inodedata**) malloc(sizeof(inodedata*) * IDHASHSIZE);
	for (i = 0; i < IDHASHSIZE; i++) {
		idhash[i] = NULL;
	}

	jqueue = queue_new(0);

	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr, 0x100000);
	pthread_create(&delayed_queue_worker_th, &thattr, delayed_queue_worker, NULL);
	write_worker_th.resize(workers);
	for (auto& th : write_worker_th) {
		pthread_create(&th, &thattr, write_worker, NULL);
	}
	pthread_attr_destroy(&thattr);

	gTweaks.registerVariable("WriteMaxRetries", maxretries);
}

void write_data_term(void) {
	uint32_t i;
	inodedata *id, *idn;

	{
		Glock lock(gMutex);
		delayed_queue_put(nullptr, 0, lock);
	}
	for (i = 0; i < write_worker_th.size(); i++) {
		queue_put(jqueue, 0, 0, NULL, 0);
	}
	for (i = 0; i < write_worker_th.size(); i++) {
		pthread_join(write_worker_th[i], NULL);
	}
	pthread_join(delayed_queue_worker_th, NULL);
	queue_delete(jqueue, queue_deleter_delete<inodedata>);
	for (i = 0; i < IDHASHSIZE; i++) {
		for (id = idhash[i]; id; id = idn) {
			idn = id->next;
			delete id;
		}
	}
	free(idhash);
}

/* glock: UNLOCKED */
int write_block(inodedata *id, uint32_t chindx, uint16_t pos, uint32_t from, uint32_t to, const uint8_t *data) {
	Glock lock(gMutex);
	id->lastWriteToDataChain.reset();

	// Try to expand the last block
	if (!id->dataChain.empty()) {
		auto& lastBlock = id->dataChain.back();
		if (lastBlock.chunkIndex == chindx
				&& lastBlock.blockIndex == pos
				&& lastBlock.type == WriteCacheBlock::kWritableBlock
				&& lastBlock.expand(from, to, data)) {
			id->wakeUpWorkerIfNecessary();
			return 0;
		}
	}

	// Didn't manage to expand an existing block, so allocate a new one
	write_cb_wait_for_block(id, lock);
	write_cb_acquire_blocks(1, lock);
	id->pushToChain(WriteCacheBlock(chindx, pos, WriteCacheBlock::kWritableBlock));
	sassert(id->dataChain.back().expand(from, to, data));
	if (id->inqueue) {
		// Consider some speedup if there are no errors and:
		// - there is a lot of blocks in the write chain
		// - there are at least two chunks in the write chain
		if (id->trycnt == 0 && (id->dataChain.size() > id->minimumBlocksToWrite
			|| id->dataChain.front().chunkIndex != id->dataChain.back().chunkIndex)) {
			if (delayed_queue_remove(id, lock)) {
				write_enqueue(id, lock);
			}
		}
		id->wakeUpWorkerIfNecessary();
	} else {
		id->inqueue = true;
		write_enqueue(id, lock);
	}
	return 0;
}

/* glock: UNLOCKED */
int write_blocks(inodedata *id, uint64_t offset, uint32_t size, const uint8_t* data) {
	LOG_AVG_TILL_END_OF_SCOPE0("write_blocks");
	uint32_t chindx = offset >> MFSCHUNKBITS;
	uint16_t pos = (offset & MFSCHUNKMASK) >> MFSBLOCKBITS;
	uint32_t from = offset & MFSBLOCKMASK;
	while (size > 0) {
		if (size > MFSBLOCKSIZE - from) {
			if (write_block(id, chindx, pos, from, MFSBLOCKSIZE, data) < 0) {
				return LIZARDFS_ERROR_IO;
			}
			size -= (MFSBLOCKSIZE - from);
			data += (MFSBLOCKSIZE - from);
			from = 0;
			pos++;
			if (pos == MFSBLOCKSINCHUNK) {
				pos = 0;
				chindx++;
			}
		} else {
			if (write_block(id, chindx, pos, from, from + size, data) < 0) {
				return LIZARDFS_ERROR_IO;
			}
			size = 0;
		}
	}
	return 0;
}

int write_data(void *vid, uint64_t offset, uint32_t size, const uint8_t* data) {
	LOG_AVG_TILL_END_OF_SCOPE0("write_data");
	int status;
	inodedata *id = (inodedata*) vid;
	if (id == NULL) {
		return LIZARDFS_ERROR_IO;
	}

	Glock lock(gMutex);
	status = id->status;
	if (status == LIZARDFS_STATUS_OK) {
		if (offset + size > id->maxfleng) {     // move fleng
			id->maxfleng = offset + size;
		}
		id->writewaiting++;
		while (id->flushwaiting > 0) {
			id->writecond.wait(lock);
		}
		id->writewaiting--;
	}
	lock.unlock();

	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}

	return write_blocks(id, offset, size, data);
}

static void write_data_flushwaiting_increase(inodedata *id, Glock&) {
	id->flushwaiting++;
}

static void write_data_flushwaiting_decrease(inodedata *id, Glock&) {
	id->flushwaiting--;
	if (id->flushwaiting == 0 && id->writewaiting > 0) {
		id->writecond.notify_all();
	}
}

static void write_data_lcnt_increase(inodedata *id, Glock&) {
	id->lcnt++;
}

static void write_data_lcnt_decrease(inodedata *id, Glock& lock) {
	id->lcnt--;
	if (id->lcnt == 0 && !id->inqueue && id->flushwaiting == 0 && id->writewaiting == 0) {
		write_free_inodedata(id, lock);
	}
}

void* write_data_new(uint32_t inode) {
	inodedata* id;
	Glock lock(gMutex);
	id = write_get_inodedata(inode, lock);
	if (id == NULL) {
		return NULL;
	}
	write_data_lcnt_increase(id, lock);
	return id;
}

static int write_data_flush(void* vid, Glock& lock) {
	inodedata* id = (inodedata*) vid;
	if (id == NULL) {
		return LIZARDFS_ERROR_IO;
	}

	write_data_flushwaiting_increase(id, lock);
	// If there are no errors (trycnt==0) and inode is waiting in the delayed queue, speed it up
	if (id->trycnt == 0 && delayed_queue_remove(id, lock)) {
		write_enqueue(id, lock);
	}
	// Wait for the data to be flushed
	while (id->inqueue) {
		id->flushcond.wait(lock);
	}
	write_data_flushwaiting_decrease(id, lock);
	return id->status;
}

int write_data_flush(void* vid) {
	Glock lock(gMutex);
	return write_data_flush(vid, lock);
}

uint64_t write_data_getmaxfleng(uint32_t inode) {
	uint64_t maxfleng;
	inodedata* id;
	Glock lock(gMutex);
	id = write_find_inodedata(inode, lock);
	if (id) {
		maxfleng = id->maxfleng;
	} else {
		maxfleng = 0;
	}
	return maxfleng;
}

int write_data_flush_inode(uint32_t inode) {
	Glock lock(gMutex);
	inodedata* id = write_find_inodedata(inode, lock);
	if (id == NULL) {
		return 0;
	}
	return write_data_flush(id, lock);
}

int write_data_truncate(uint32_t inode, bool opened, uint32_t uid, uint32_t gid, uint64_t length,
		Attributes& attr) {
	Glock lock(gMutex);

	// 1. Flush writes but don't finish it completely - it'll be done at the end of truncate
	inodedata* id = write_get_inodedata(inode, lock);
	if (id == NULL) {
		return LIZARDFS_ERROR_IO;
	}
	write_data_lcnt_increase(id, lock);
	write_data_flushwaiting_increase(id, lock); // this will block any writing to this inode

	int err = write_data_flush(id, lock);
	if (err != 0) {
		write_data_flushwaiting_decrease(id, lock);
		write_data_lcnt_decrease(id, lock);
		return err;
	}

	// 2. Send the request to master
	uint8_t status;
	bool writeNeeded;
	uint64_t oldLength;
	uint32_t lockId;
	lock.unlock();
	int retrySleepTime_us = 200000;
	uint32_t retries = 0;
	do {
		status = fs_truncate(inode, opened, uid, gid, length, writeNeeded, attr, oldLength, lockId);
		if (status != LIZARDFS_STATUS_OK) {
			lzfs_pretty_syslog(LOG_INFO, "truncate file %" PRIu32 " to length %" PRIu64 ": %s (try %d/%d)",
					inode, length, lizardfs_error_string(status), int(retries + 1), int(maxretries));
		}
		if (retries >= maxretries) {
			break;
		}
		if (status == LIZARDFS_ERROR_LOCKED) {
			sleep(1);
		} else if (status == LIZARDFS_ERROR_CHUNKLOST || status == LIZARDFS_ERROR_NOTDONE) {
			usleep(retrySleepTime_us);
			retrySleepTime_us = std::min(2 * retrySleepTime_us, 60 * 1000000);
			++retries;
		}
	} while (status == LIZARDFS_ERROR_LOCKED || status == LIZARDFS_ERROR_CHUNKLOST || status == LIZARDFS_ERROR_NOTDONE);
	lock.lock();
	if (status != 0 || !writeNeeded) {
		// Something failed or we have nothing to do more (master server managed to do the truncate)
		write_data_flushwaiting_decrease(id, lock);
		write_data_lcnt_decrease(id, lock);
		if (status == LIZARDFS_STATUS_OK) {
			return 0;
		} else {
			// status is now MFS status, so we cannot return any errno
			throw UnrecoverableWriteException("fs_truncate failed", status);
		}
	}

	// We have to write zeros in suitable region to update xor/ec parity parts.
	// Let's calculate size of the region to be zeroed
	uint64_t endOffset = std::min({
		oldLength,                            // no further than to the end of the file
		length + slice_traits::ec::kMaxDataCount * MFSBLOCKSIZE, // no more than the maximal stripe
		(length + MFSCHUNKSIZE - 1) / MFSCHUNKSIZE * MFSCHUNKSIZE // no beyond the end of chunk
	});

	if (endOffset > length) {
		// Something has to be written, so pass our lock to writing threads
		sassert(id->dataChain.empty());
		id->locator.reset(new TruncateWriteChunkLocator(inode, length / MFSCHUNKSIZE, lockId));

		// And now pass block of zeros to writing threads
		std::vector<uint8_t> zeros(endOffset - length, 0);
		lock.unlock();
		err = write_blocks(id, length, zeros.size(), zeros.data());
		lock.lock();
		if (err != 0) {
			write_data_flushwaiting_decrease(id, lock);
			write_data_lcnt_decrease(id, lock);
			return err;
		}

		// Wait for writing threads to finish
		err = write_data_flush(id, lock);
		id->locator.reset();
		if (err != 0) {
			// unlock the chunk here?
			write_data_flushwaiting_decrease(id, lock);
			write_data_lcnt_decrease(id, lock);
			return err;
		}
	}

	// Now we can tell the master server to finish the truncate operation and then unblock the inode
	lock.unlock();
	status = fs_truncateend(inode, uid, gid, length, lockId, attr);
	write_data_flushwaiting_decrease(id, lock);
	write_data_lcnt_decrease(id, lock);

	if (status != LIZARDFS_STATUS_OK) {
		// status is now MFS status, so we cannot return any errno
		throw UnrecoverableWriteException("fs_truncateend failed", status);
	}
	return 0;
}

int write_data_end(void* vid) {
	Glock lock(gMutex);
	inodedata* id = (inodedata*) vid;
	if (id == NULL) {
		return LIZARDFS_ERROR_IO;
	}
	int status = write_data_flush(id, lock);
	write_data_lcnt_decrease(id, lock);
	return status;
}
