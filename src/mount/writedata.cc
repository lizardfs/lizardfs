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

#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <inttypes.h>
#include <vector>

#include "common/chunk_connector.h"
#include "common/cltocs_communication.h"
#include "common/crc.h"
#include "common/datapack.h"
#include "common/massert.h"
#include "common/message_receive_buffer.h"
#include "common/MFSCommunication.h"
#include "common/mfsstrerr.h"
#include "common/multi_buffer_writer.h"
#include "common/pcqueue.h"
#include "common/sockets.h"
#include "common/strerr.h"
#include "common/time_utils.h"
#include "mount/chunk_writer.h"
#include "mount/chunkserver_write_chain.h"
#include "mount/exceptions.h"
#include "mount/global_chunkserver_stats.h"
#include "mount/mastercomm.h"
#include "mount/readdata.h"
#include "mount/write_cache_block.h"

#ifndef EDQUOT
#define EDQUOT ENOSPC
#endif

#define IDLE_CONNECTION_TIMEOUT 6
#define IDHASHSIZE 256
#define IDHASH(inode) (((inode)*0xB239FB71)%IDHASHSIZE)

struct inodedata {
	uint32_t inode;
	uint64_t maxfleng;
	int status;
	uint16_t flushwaiting;
	uint16_t writewaiting;
	uint16_t lcnt;
	uint32_t trycnt;
	bool inqueue;
	std::list<WriteCacheBlock> dataChain;
	pthread_cond_t flushcond; // wait for !inqueue (flush)
	pthread_cond_t writecond; // wait for flushwaiting==0 (write)
	inodedata *next;
	std::unique_ptr<WriteChunkLocator> locator;

	inodedata(uint32_t inode)
			: inode(inode),
			  maxfleng(0),
			  status(STATUS_OK),
			  flushwaiting(0),
			  writewaiting(0),
			  lcnt(0),
			  trycnt(0),
			  inqueue(false),
			  next(nullptr) {
		pthread_cond_init(&flushcond, nullptr);
		pthread_cond_init(&writecond, nullptr);
	}

	~inodedata() {
		pthread_cond_destroy(&flushcond);
		pthread_cond_destroy(&writecond);
	}
};

// static pthread_mutex_t fcblock;

static pthread_cond_t fcbcond;
static uint8_t fcbwaiting;
static int32_t freecacheblocks;

static uint32_t maxretries;

static inodedata **idhash;

static pthread_mutex_t glock;

static pthread_t dqueue_worker_th;
static std::vector<pthread_t> write_worker_th;

static void *jqueue, *dqueue;

static ConnectionPool gChunkserverConnectionPool;

#define TIMEDIFF(tv1,tv2) (((int64_t)((tv1).tv_sec-(tv2).tv_sec))*1000000LL+(int64_t)((tv1).tv_usec-(tv2).tv_usec))

/* glock: LOCKED */
void write_cb_release_blocks(uint32_t count) {
	freecacheblocks += count;
	if (fcbwaiting) {
		pthread_cond_signal(&fcbcond);
	}
}

/* glock: LOCKED */
void write_cb_acquire_blocks(uint32_t count) {
	freecacheblocks -= count;
}

/* glock: LOCKED */
void write_cb_wait_for_block(inodedata *id) {
	fcbwaiting++;
	while (freecacheblocks <= 0
			|| static_cast<int32_t>(id->dataChain.size()) > (freecacheblocks / 3)) {
		pthread_cond_wait(&fcbcond, &glock);
	}
	fcbwaiting--;
}

/* inode */

/* glock: LOCKED */
inodedata* write_find_inodedata(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	for (id = idhash[idh]; id; id = id->next) {
		if (id->inode == inode) {
			return id;
		}
	}
	return NULL;
}

/* glock: LOCKED */
inodedata* write_get_inodedata(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;

	for (id = idhash[idh]; id; id = id->next) {
		if (id->inode == inode) {
			return id;
		}
	}
	id = new inodedata(inode);
	id->next = idhash[idh];
	idhash[idh] = id;
	return id;
}

/* glock: LOCKED */
void write_free_inodedata(inodedata *fid) {
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

/* queues */

/* glock: UNUSED */
void write_delayed_enqueue(inodedata *id, uint32_t cnt) {
	struct timeval tv;
	if (cnt > 0) {
		gettimeofday(&tv, NULL);
		queue_put(dqueue, tv.tv_sec, tv.tv_usec, (uint8_t*) id, cnt);
	} else {
		queue_put(jqueue, 0, 0, (uint8_t*) id, 0);
	}
}

/* glock: UNUSED */
void write_enqueue(inodedata *id) {
	queue_put(jqueue, 0, 0, (uint8_t*) id, 0);
}

/* worker thread | glock: UNUSED */
void* write_dqueue_worker(void *arg) {
	struct timeval tv;
	uint32_t sec, usec, cnt;
	uint8_t *id;
	(void) arg;
	for (;;) {
		queue_get(dqueue, &sec, &usec, &id, &cnt);
		if (id == NULL) {
			return NULL;
		}
		gettimeofday(&tv, NULL);
		if ((uint32_t) (tv.tv_usec) < usec) {
			tv.tv_sec--;
			tv.tv_usec += 1000000;
		}
		if ((uint32_t) (tv.tv_sec) < sec) {
			// time went backward !!!
			sleep(1);
		} else if ((uint32_t) (tv.tv_sec) == sec) {
			usleep(1000000 - (tv.tv_usec - usec));
		}
		cnt--;
		if (cnt > 0) {
			gettimeofday(&tv, NULL);
			queue_put(dqueue, tv.tv_sec, tv.tv_usec, (uint8_t*) id, cnt);
		} else {
			queue_put(jqueue, 0, 0, id, 0);
		}
	}
	return NULL;
}

/* glock: UNLOCKED */
void write_job_end(inodedata *id, int status) {
	id->locator.reset();
	pthread_mutex_lock(&glock);
	if (status) {
		errno = status;
		syslog(LOG_WARNING, "error writing file number %" PRIu32 ": %s", id->inode, strerr(errno));
		id->status = status;
	}
	status = id->status;

	if (!id->dataChain.empty() && status == 0) { // still have some work to do
		id->trycnt = 0;	// on good write reset try counter
		write_delayed_enqueue(id, 0);
	} else {	// no more work or error occured
		// if this is an error then release all data blocks
		write_cb_release_blocks(id->dataChain.size());
		id->dataChain.clear();
		id->inqueue = false;
		if (id->flushwaiting > 0) {
			pthread_cond_broadcast(&(id->flushcond));
		}
	}
	pthread_mutex_unlock(&glock);
}

class InodeChunkWriter {
public:
	InodeChunkWriter() : inodeData_(nullptr), chunkIndex_(0) {}
	void processJob(inodedata* data);

private:
	void processDataChain(ChunkWriter& writer);
	bool haveBlockToWrite(int pendingOperationCount);
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
	inodeData_ = inodeData;

	// First, choose index of some chunk to write
	pthread_mutex_lock(&glock);
	int status = inodeData_->status;
	bool haveDataToWrite;
	if (inodeData_->locator) {
		// There is a chunk lock left by a previous unfinished job -- let's finish it!
		chunkIndex_ = inodeData_->locator->chunkIndex();
		haveDataToWrite = (!inodeData_->dataChain.empty())
				&& inodeData_->dataChain.front().chunkIndex == chunkIndex_;
	} else if (!inodeData_->dataChain.empty()) {
		// There is no unfinished job, but there is some data to write -- let's start a new job
		chunkIndex_ = inodeData_->dataChain.front().chunkIndex;
		haveDataToWrite = true;
	} else {
		// No data, no unfinished jobs -- something wrong!
		// This should never happen, so the status doesn't really matter
		syslog(LOG_WARNING, "got inode with no data to write!!!");
		haveDataToWrite = false;
		status = EINVAL;
	}
	pthread_mutex_unlock(&glock);
	if (status != STATUS_OK) {
		write_job_end(inodeData_, status);
		return;
	}

	/*  Process the job */
	ChunkConnector connector(fs_getsrcip(), gChunkserverConnectionPool);
	ChunkWriter writer(globalChunkserverStats, connector);
	wholeOperationTimer.reset();
	std::unique_ptr<WriteChunkLocator> locator = std::move(inodeData_->locator);
	if (!locator) {
		locator.reset(new WriteChunkLocator());
	}

	try {
		try {
			locator->locateAndLockChunk(inodeData_->inode, chunkIndex_);
			if (haveDataToWrite) {
				// Optimization -- don't talk with chunkservers if we have just to release
				// some previously unlocked lock
				writer.init(locator.get(), 5000);
				processDataChain(writer);
				writer.finish(5000);
			}
			locator->unlockChunk();
			read_inode_ops(inodeData_->inode);
			write_job_end(inodeData_, STATUS_OK);
		} catch (Exception& e) {
			std::string errorString = e.what();
			if (e.status() != ERROR_LOCKED) {
				inodeData_->trycnt++;
				errorString += " (try counter: " + std::to_string(inodeData->trycnt) + ")";
			}
			syslog(LOG_WARNING, "write file error, inode: %" PRIu32 ", index: %" PRIu32 " - %s",
					inodeData_->inode, chunkIndex_, errorString.c_str());
			// Keep the lock
			inodeData_->locator = std::move(locator);
			// Move data left in the journal into front of the write cache
			std::list<WriteCacheBlock> journal = writer.releaseJournal();
			if (!journal.empty()) {
				pthread_mutex_lock(&glock);
				write_cb_acquire_blocks(journal.size());
				inodeData_->dataChain.splice(inodeData_->dataChain.begin(), journal);
				pthread_mutex_unlock(&glock);
			}
			if (inodeData_->trycnt >= maxretries) {
				// Convert error to an unrecoverable error
				throw UnrecoverableWriteException(e.message(), e.status());
			} else {
				// This may be recoverable or unrecoverable error
				throw;
			}
		}
	} catch (UnrecoverableWriteException& e) {
		if (e.status() == ERROR_ENOENT) {
			write_job_end(inodeData_, EBADF);
		} else if (e.status() == ERROR_QUOTA) {
			write_job_end(inodeData_, EDQUOT);
		} else if (e.status() == ERROR_NOSPACE || e.status() == ERROR_NOCHUNKSERVERS) {
			write_job_end(inodeData_, ENOSPC);
		} else {
			write_job_end(inodeData_, EIO);
		}
	} catch (Exception& e) {
		write_delayed_enqueue(inodeData_, 1 + std::min<int>(10, inodeData_->trycnt / 3));
	}
}

void InodeChunkWriter::processDataChain(ChunkWriter& writer) {
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
		// new if we've already sent 15 blocks and didn't receive status from the chunkserver.
		if (wholeOperationTimer.elapsed_s() + kTimeToFinishOperations < maximumTime) {
			pthread_mutex_lock(&glock);
			// While there is any block worth sending, we add new write operation
			try {
				uint32_t unfinishedOperations = writer.getUnfinishedOperationsCount();
				while (haveBlockToWrite(unfinishedOperations)) {
					// Remove block from cache and pass it to the writer
					writer.addOperation(std::move(inodeData_->dataChain.front()));
					inodeData_->dataChain.pop_front();
					write_cb_release_blocks(1);
				}
			} catch (...) {
				pthread_mutex_unlock(&glock);
				throw;
			}
			pthread_mutex_unlock(&glock);
		}

		if (writer.getUnfinishedOperationsCount() == 0) {
			return;
		}

		if (wholeOperationTimer.elapsed_s() >= maximumTime) {
			std::stringstream ss;
			throw RecoverableWriteException(
					"Timeout after " + std::to_string(wholeOperationTimer.elapsed_ms()) + " ms",
					ERROR_TIMEOUT);
		}

		writer.processOperations(50);
	}
}

/*
 * Check if there is any data worth sending to the chunkserver.
 * We will avoid sending blocks of size different than MFSBLOCKSIZE.
 * These can be taken only if we are close to run out of tasks to do.
 */
bool InodeChunkWriter::haveBlockToWrite(int pendingOperationCount) {
	if (inodeData_->dataChain.empty()) {
		return false;
	}
	const auto& block = inodeData_->dataChain.front();
	if (block.chunkIndex != chunkIndex_) {
		// Don't write other chunks
		return false;
	} else if (block.type != WriteCacheBlock::kWritableBlock) {
		// Always write data, that was previously written
		return true;
	} else if (pendingOperationCount >= 15) {
		// Don't start new operations if there is already a lot of pending writes
		return false;
	} else {
		// Always start full blocks; start partial blocks only if we are running out of tasks
		return (block.size() == MFSBLOCKSIZE || pendingOperationCount <= 1);
	}
}

/* main working thread | glock:UNLOCKED */
void* write_worker(void*) {
	InodeChunkWriter inodeDataWriter;
	for (;;) {
		// get next job
		uint32_t z1, z2, z3;
		uint8_t *data;
		queue_get(jqueue, &z1, &z2, &data, &z3);
		if (data == NULL) {
			return NULL;
		}

		// process the job
		inodeDataWriter.processJob((inodedata*) data);
	}
	return NULL;
}

/* API | glock: INITIALIZED,UNLOCKED */
void write_data_init(uint32_t cachesize, uint32_t retries, uint32_t workers) {
	uint32_t cacheblockcount = (cachesize / MFSBLOCKSIZE);
	uint32_t i;
	pthread_attr_t thattr;

	maxretries = retries;
	if (cacheblockcount < 10) {
		cacheblockcount = 10;
	}
	pthread_mutex_init(&glock, NULL);

	pthread_cond_init(&fcbcond, NULL);
	fcbwaiting = 0;
	freecacheblocks = cacheblockcount;

	idhash = (inodedata**) malloc(sizeof(inodedata*) * IDHASHSIZE);
	for (i = 0; i < IDHASHSIZE; i++) {
		idhash[i] = NULL;
	}

	dqueue = queue_new(0);
	jqueue = queue_new(0);

	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr, 0x100000);
	pthread_create(&dqueue_worker_th, &thattr, write_dqueue_worker, NULL);
	write_worker_th.resize(workers);
	for (auto& th : write_worker_th) {
		pthread_create(&th, &thattr, write_worker, (void*) (unsigned long) (i));
	}
	pthread_attr_destroy(&thattr);
}

void write_data_term(void) {
	uint32_t i;
	inodedata *id, *idn;

	queue_put(dqueue, 0, 0, NULL, 0);
	for (i = 0; i < write_worker_th.size(); i++) {
		queue_put(jqueue, 0, 0, NULL, 0);
	}
	for (i = 0; i < write_worker_th.size(); i++) {
		pthread_join(write_worker_th[i], NULL);
	}
	pthread_join(dqueue_worker_th, NULL);
	queue_delete(dqueue);
	queue_delete(jqueue);
	for (i = 0; i < IDHASHSIZE; i++) {
		for (id = idhash[i]; id; id = idn) {
			idn = id->next;
			delete id;
		}
	}
	free(idhash);
	pthread_cond_destroy(&fcbcond);
	pthread_mutex_destroy(&glock);
}

/* glock: UNLOCKED */
int write_block(inodedata *id, uint32_t chindx, uint16_t pos, uint32_t from, uint32_t to, const uint8_t *data) {
	pthread_mutex_lock(&glock);
	// Try to expand the last block
	if (!id->dataChain.empty()) {
		auto& lastBlock = id->dataChain.back();
		if (lastBlock.chunkIndex == chindx
				&& lastBlock.blockIndex == pos
				&& lastBlock.type == WriteCacheBlock::kWritableBlock
				&& lastBlock.expand(from, to, data)) {
			pthread_mutex_unlock(&glock);
			return 0;
		}
	}

	// Didn't manage to expand an existing block, so allocate a new one
	write_cb_wait_for_block(id);
	write_cb_acquire_blocks(1);
	id->dataChain.push_back(WriteCacheBlock(chindx, pos, WriteCacheBlock::kWritableBlock));
	sassert(id->dataChain.back().expand(from, to, data));
	if (!id->inqueue) {
		id->inqueue = true;
		write_enqueue(id);
	}
	pthread_mutex_unlock(&glock);
	return 0;
}

/* API | glock: UNLOCKED */
int write_data(void *vid, uint64_t offset, uint32_t size, const uint8_t *data) {
	uint32_t chindx;
	uint16_t pos;
	uint32_t from;
	int status;
	inodedata *id = (inodedata*) vid;
	if (id == NULL) {
		return EIO;
	}

//	gettimeofday(&s,NULL);
	pthread_mutex_lock(&glock);
//	syslog(LOG_NOTICE,"write_data: inode:%" PRIu32 " offset:%" PRIu32 " size:%" PRIu32,id->inode,offset,size);
	status = id->status;
	if (status == 0) {
		if (offset + size > id->maxfleng) {	// move fleng
			id->maxfleng = offset + size;
		}
		id->writewaiting++;
		while (id->flushwaiting > 0) {
			pthread_cond_wait(&(id->writecond), &glock);
		}
		id->writewaiting--;
	}
	pthread_mutex_unlock(&glock);
	if (status != 0) {
		return status;
	}

	chindx = offset >> MFSCHUNKBITS;
	pos = (offset & MFSCHUNKMASK) >> MFSBLOCKBITS;
	from = offset & MFSBLOCKMASK;
	while (size > 0) {
		if (size > MFSBLOCKSIZE - from) {
			if (write_block(id, chindx, pos, from, MFSBLOCKSIZE, data) < 0) {
				return EIO;
			}
			size -= (MFSBLOCKSIZE - from);
			data += (MFSBLOCKSIZE - from);
			from = 0;
			pos++;
			if (pos == 1024) {
				pos = 0;
				chindx++;
			}
		} else {
			if (write_block(id, chindx, pos, from, from + size, data) < 0) {
				return EIO;
			}
			size = 0;
		}
	}
//	gettimeofday(&e,NULL);
//	syslog(LOG_NOTICE,"write_data time: %" PRId64,TIMEDIFF(e,s));
	return 0;
}

/* API | glock: UNLOCKED */
void* write_data_new(uint32_t inode) {
	inodedata* id;
	pthread_mutex_lock(&glock);
	id = write_get_inodedata(inode);
	if (id == NULL) {
		pthread_mutex_unlock(&glock);
		return NULL;
	}
	id->lcnt++;
	pthread_mutex_unlock(&glock);
	return id;
}

int write_data_flush(void *vid) {
	inodedata* id = (inodedata*) vid;
	int ret;
	if (id == NULL) {
		return EIO;
	}

//	gettimeofday(&s,NULL);
	pthread_mutex_lock(&glock);
	id->flushwaiting++;
	while (id->inqueue) {
//		syslog(LOG_NOTICE,"flush: wait ...");
		pthread_cond_wait(&(id->flushcond), &glock);
//		syslog(LOG_NOTICE,"flush: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting == 0 && id->writewaiting > 0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	if (id->lcnt == 0 && !id->inqueue && id->flushwaiting == 0 && id->writewaiting == 0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
//	gettimeofday(&e,NULL);
//	syslog(LOG_NOTICE,"write_data_flush time: %" PRId64,TIMEDIFF(e,s));
	return ret;
}

uint64_t write_data_getmaxfleng(uint32_t inode) {
	uint64_t maxfleng;
	inodedata* id;
	pthread_mutex_lock(&glock);
	id = write_find_inodedata(inode);
	if (id) {
		maxfleng = id->maxfleng;
	} else {
		maxfleng = 0;
	}
	pthread_mutex_unlock(&glock);
	return maxfleng;
}

/* API | glock: UNLOCKED */
int write_data_flush_inode(uint32_t inode) {
	inodedata* id;
	int ret;
	pthread_mutex_lock(&glock);
	id = write_find_inodedata(inode);
	if (id == NULL) {
		pthread_mutex_unlock(&glock);
		return 0;
	}
	id->flushwaiting++;
	while (id->inqueue) {
//		syslog(LOG_NOTICE,"flush_inode: wait ...");
		pthread_cond_wait(&(id->flushcond), &glock);
//		syslog(LOG_NOTICE,"flush_inode: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting == 0 && id->writewaiting > 0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	if (id->lcnt == 0 && !id->inqueue && id->flushwaiting == 0 && id->writewaiting == 0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
	return ret;
}

/* API | glock: UNLOCKED */
int write_data_end(void *vid) {
	inodedata* id = (inodedata*) vid;
	int ret;
	if (id == NULL) {
		return EIO;
	}
	pthread_mutex_lock(&glock);
	id->flushwaiting++;
	while (id->inqueue) {
//		syslog(LOG_NOTICE,"write_end: wait ...");
		pthread_cond_wait(&(id->flushcond), &glock);
//		syslog(LOG_NOTICE,"write_end: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting == 0 && id->writewaiting > 0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	id->lcnt--;
	if (id->lcnt == 0 && !id->inqueue && id->flushwaiting == 0 && id->writewaiting == 0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
	return ret;
}
