/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
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
#include "mount/chunkserver_write_chain.h"
#include "mount/mastercomm.h"
#include "mount/readdata.h"

// TODO: wtf?!
// #define WORKER_DEBUG 1
// #define BUFFER_DEBUG 1

#ifndef EDQUOT
#define EDQUOT ENOSPC
#endif

#define WORKERS 10
#define IDLE_CONNECTION_TIMEOUT 6

#define WCHASHSIZE 256
#define WCHASH(inode,indx) (((inode)*0xB239FB71+(indx)*193)%WCHASHSIZE)

#define IDHASHSIZE 256
#define IDHASH(inode) (((inode)*0xB239FB71)%IDHASHSIZE)

typedef struct cblock_s {
	uint8_t data[MFSBLOCKSIZE];	// modified only when writeid==0
	uint32_t chindx;	// chunk number
	uint16_t pos;		// block in chunk (0...1023) - never modified
	uint32_t writeid;	// 0 = not sent, >0 = block was sent (modified and accessed only when wchunk is locked)
	uint32_t from;		// first filled byte in data (modified only when writeid==0)
	uint32_t to;		// first not used byte in data (modified only when writeid==0)
	struct cblock_s *next,*prev;
} cblock;

typedef struct inodedata_s {
	uint32_t inode;
	uint64_t maxfleng;
	uint32_t cacheblockcount;
	int status;
	uint16_t flushwaiting;
	uint16_t writewaiting;
	uint16_t lcnt;
	uint32_t trycnt;
	uint8_t waitingworker;
	uint8_t inqueue;
	int pipe[2];
	cblock *datachainhead,*datachaintail;
	pthread_cond_t flushcond;	// wait for inqueue==0 (flush)
	pthread_cond_t writecond;	// wait for flushwaiting==0 (write)
	struct inodedata_s *next;
} inodedata;

// static pthread_mutex_t fcblock;

static pthread_cond_t fcbcond;
static uint8_t fcbwaiting;
static cblock *cacheblocks,*freecblockshead;
static uint32_t freecacheblocks;

static uint32_t maxretries;

static inodedata **idhash;

static pthread_mutex_t glock;

#ifdef BUFFER_DEBUG
static pthread_t info_worker_th;
static uint32_t usedblocks;
#endif

static pthread_t dqueue_worker_th;
static pthread_t write_worker_th[WORKERS];

static void *jqueue,*dqueue;

#define TIMEDIFF(tv1,tv2) (((int64_t)((tv1).tv_sec-(tv2).tv_sec))*1000000LL+(int64_t)((tv1).tv_usec-(tv2).tv_usec))

#ifdef BUFFER_DEBUG
void* write_info_worker(void *arg) {
	(void)arg;
	for (;;) {
		pthread_mutex_lock(&glock);
		syslog(LOG_NOTICE,"used cache blocks: %" PRIu32,usedblocks);
		pthread_mutex_unlock(&glock);
		usleep(500000);
	}

}
#endif

/* glock: LOCKED */
void write_cb_release (inodedata *id,cblock *cb) {
	cb->next = freecblockshead;
	freecblockshead = cb;
	freecacheblocks++;
	id->cacheblockcount--;
	if (fcbwaiting) {
		pthread_cond_signal(&fcbcond);
	}
#ifdef BUFFER_DEBUG
	usedblocks--;
#endif
}

/* glock: LOCKED */
cblock* write_cb_acquire(inodedata *id) {
	cblock *ret;
	fcbwaiting++;
	while (freecblockshead==NULL || id->cacheblockcount>(freecacheblocks/3)) {
		pthread_cond_wait(&fcbcond,&glock);
	}
	fcbwaiting--;
	ret = freecblockshead;
	freecblockshead = ret->next;
	ret->chindx = 0;
	ret->pos = 0;
	ret->writeid = 0;
	ret->from = 0;
	ret->to = 0;
	ret->next = NULL;
	ret->prev = NULL;
	freecacheblocks--;
	id->cacheblockcount++;
#ifdef BUFFER_DEBUG
	usedblocks++;
#endif
	return ret;
}


/* inode */

/* glock: LOCKED */
inodedata* write_find_inodedata(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	for (id=idhash[idh] ; id ; id=id->next) {
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
	int pfd[2];

	for (id=idhash[idh] ; id ; id=id->next) {
		if (id->inode == inode) {
			return id;
		}
	}

	if (pipe(pfd)<0) {
		syslog(LOG_WARNING,"pipe error: %s",strerr(errno));
		return NULL;
	}
	id = (inodedata*) malloc(sizeof(inodedata));
	id->inode = inode;
	id->cacheblockcount = 0;
	id->maxfleng = 0;
	id->status = 0;
	id->trycnt = 0;
	id->pipe[0] = pfd[0];
	id->pipe[1] = pfd[1];
	id->datachainhead = NULL;
	id->datachaintail = NULL;
	id->waitingworker = 0;
	id->inqueue = 0;
	id->flushwaiting = 0;
	id->writewaiting = 0;
	id->lcnt = 0;
	pthread_cond_init(&(id->flushcond),NULL);
	pthread_cond_init(&(id->writecond),NULL);
	id->next = idhash[idh];
	idhash[idh] = id;
	return id;
}

/* glock: LOCKED */
void write_free_inodedata(inodedata *fid) {
	uint32_t idh = IDHASH(fid->inode);
	inodedata *id,**idp;
	idp = &(idhash[idh]);
	while ((id=*idp)) {
		if (id==fid) {
			*idp = id->next;
			pthread_cond_destroy(&(id->flushcond));
			pthread_cond_destroy(&(id->writecond));
			close(id->pipe[0]);
			close(id->pipe[1]);
			free(id);
			return;
		}
		idp = &(id->next);
	}
}


/* queues */

/* glock: UNUSED */
void write_delayed_enqueue(inodedata *id,uint32_t cnt) {
	struct timeval tv;
	if (cnt>0) {
		gettimeofday(&tv,NULL);
		queue_put(dqueue,tv.tv_sec,tv.tv_usec,(uint8_t*)id,cnt);
	} else {
		queue_put(jqueue,0,0,(uint8_t*)id,0);
	}
}

/* glock: UNUSED */
void write_enqueue(inodedata *id) {
	queue_put(jqueue,0,0,(uint8_t*)id,0);
}

/* worker thread | glock: UNUSED */
void* write_dqueue_worker(void *arg) {
	struct timeval tv;
	uint32_t sec,usec,cnt;
	uint8_t *id;
	(void)arg;
	for (;;) {
		queue_get(dqueue,&sec,&usec,&id,&cnt);
		if (id==NULL) {
			return NULL;
		}
		gettimeofday(&tv,NULL);
		if ((uint32_t)(tv.tv_usec) < usec) {
			tv.tv_sec--;
			tv.tv_usec += 1000000;
		}
		if ((uint32_t)(tv.tv_sec) < sec) {
			// time went backward !!!
			sleep(1);
		} else if ((uint32_t)(tv.tv_sec) == sec) {
			usleep(1000000-(tv.tv_usec-usec));
		}
		cnt--;
		if (cnt>0) {
			gettimeofday(&tv,NULL);
			queue_put(dqueue,tv.tv_sec,tv.tv_usec,(uint8_t*)id,cnt);
		} else {
			queue_put(jqueue,0,0,id,0);
		}
	}
	return NULL;
}

/* glock: UNLOCKED */
void write_job_end(inodedata *id,int status,uint32_t delay) {
	cblock *cb,*fcb;

	pthread_mutex_lock(&glock);
	if (status) {
		errno = status;
		syslog(LOG_WARNING,"error writing file number %" PRIu32 ": %s",id->inode,strerr(errno));
		id->status = status;
	}
	status = id->status;

	if (id->datachainhead && status==0) {	// still have some work to do
		// reset write id
		for (cb=id->datachainhead ; cb ; cb=cb->next) {
			cb->writeid = 0;
		}
		if (delay==0) {
			id->trycnt=0;	// on good write reset try counter
		}
		write_delayed_enqueue(id,delay);
	} else {	// no more work or error occured
		// if this is an error then release all data blocks
		cb = id->datachainhead;
		while (cb) {
			fcb = cb;
			cb = cb->next;
			write_cb_release(id,fcb);
		}
		id->datachainhead=NULL;
		id->inqueue=0;

		if (id->flushwaiting>0) {
			pthread_cond_broadcast(&(id->flushcond));
		}
	}
	pthread_mutex_unlock(&glock);
}

class InodeChunkWriter {
public:
	InodeChunkWriter();
	void processJob(inodedata* data);

private:
	void reset();
	bool tryGetNewBlockToWrite();
	int processReceivedMessage(const MessageReceiveBuffer& messageBuffer);
	int processReceivedStatusMessage(const uint8_t* statusMessageData);
	inodedata* inodeData_;
	uint32_t chunkIndex_;
	uint64_t fileLength_;
	uint64_t chunkId_;
	uint32_t chunkVersion_;
	cblock* currentBlock_;
	bool sendingCurrentBlockData_;
	int requestsWaitingForStatus_;
};

InodeChunkWriter::InodeChunkWriter()
		: inodeData_(NULL),
		  chunkIndex_(0),
		  fileLength_(0),
		  chunkId_(0),
		  chunkVersion_(0),
		  currentBlock_(NULL),
		  sendingCurrentBlockData_(false),
		  requestsWaitingForStatus_(0) {
}

void InodeChunkWriter::reset() {
	inodeData_ = NULL;
	currentBlock_ = NULL;
	sendingCurrentBlockData_ = false;
	requestsWaitingForStatus_ = 0;
}

void InodeChunkWriter::processJob(inodedata* inodeData) {
	uint8_t pipebuff[1024];
	int status;

	reset();
	inodeData_ = inodeData;

	pthread_mutex_lock(&glock);
	if (inodeData_->datachainhead) {
		chunkIndex_ = inodeData_->datachainhead->chindx;
		status = inodeData_->status;
	} else {
		syslog(LOG_WARNING,"writeworker got inode with no data to write !!!");
		chunkIndex_ = 0;
		status = EINVAL; // this should never happen, so status is not important - just anything
	}
	pthread_mutex_unlock(&glock);

	if (status != STATUS_OK) {
		write_job_end(inodeData_, status, 0);
		return;
	}

	// Get chunk data from master and acquire a lock on the chunk
	const uint8_t *chunkserverData;
	uint32_t chunkserverDataSize;
	int writeChunkStatus = fs_writechunk(inodeData_->inode, chunkIndex_,
			&fileLength_, &chunkId_, &chunkVersion_, &chunkserverData, &chunkserverDataSize);
	if (writeChunkStatus != STATUS_OK) {
		syslog(LOG_WARNING,
				"file: %" PRIu32 ", index: %" PRIu32
				" - fs_writechunk returns status: %s",
				inodeData_->inode, chunkIndex_, mfsstrerr(writeChunkStatus));
		if (writeChunkStatus != ERROR_LOCKED) {
			if (writeChunkStatus == ERROR_ENOENT) {
				write_job_end(inodeData_, EBADF, 0);
			} else if (writeChunkStatus == ERROR_QUOTA) {
				write_job_end(inodeData_, EDQUOT, 0);
			} else if (writeChunkStatus == ERROR_NOSPACE) {
				write_job_end(inodeData_, ENOSPC, 0);
			} else {
				inodeData_->trycnt++;
				if (inodeData_->trycnt >= maxretries) {
					if (writeChunkStatus == ERROR_NOCHUNKSERVERS) {
						write_job_end(inodeData_, ENOSPC, 0);
					} else {
						write_job_end(inodeData_, EIO, 0);
					}
				} else {
					write_delayed_enqueue(inodeData_, 1 + std::min<int>(10, inodeData_->trycnt / 3));
				}
			}
		} else {
			write_delayed_enqueue(inodeData_, 1 + std::min<int>(10, inodeData_->trycnt / 3));
		}
		return;
	}

	if (chunkserverData == NULL || chunkserverDataSize == 0) {
		syslog(LOG_WARNING,
				"file: %" PRIu32 ", index: %" PRIu32
				", chunk: %" PRIu64 ", version: %" PRIu32
				" - there are no valid copies",
				inodeData_->inode, chunkIndex_, chunkId_, chunkVersion_);
		inodeData_->trycnt += 6;
		if (inodeData_->trycnt >= maxretries) {
			write_job_end(inodeData_, ENXIO, 0);
		} else {
			write_delayed_enqueue(inodeData_, 60);
		}
		return;
	}

	// Create a chain of chunkservers that we will write to
	ChunkserverWriteChain chunkserverChain;
	const uint8_t* cp = chunkserverData;
	const uint8_t* cpe = chunkserverData + chunkserverDataSize;
	while (cp < cpe && chunkserverChain.size() < 10) {
		uint32_t ip = get32bit(&cp);
		uint16_t port = get16bit(&cp);
		chunkserverChain.add(NetworkAddress(ip, port));
	}

	Timer wholeOperationTimer;

	// Connect to the first chunkserver from a chain
	int fd = chunkserverChain.connect();
	if (fd < 0) {
		fs_writeend(chunkId_, inodeData_->inode, 0);
		inodeData_->trycnt++;
		if (inodeData_->trycnt >= maxretries) {
			write_job_end(inodeData_, EIO, 0);
		} else {
			write_delayed_enqueue(inodeData_, 1 + std::min<int>(10, inodeData_->trycnt / 3));
		}
		return;
	}
	if (tcpnodelay(fd) < 0) {
		syslog(LOG_WARNING,"can't set TCP_NODELAY: %s",strerr(errno));
	}

	// Prepare initial message
	std::vector<uint8_t> message;
	MultiBufferWriter sendBuffer;
	chunkserverChain.createInitialMessage(message, chunkId_, chunkVersion_);
	sendBuffer.addBufferToSend(message.data(), message.size());
	requestsWaitingForStatus_++;

	// Prepare buffer for CSTOCL_WRITE_STATUS messages
	MessageReceiveBuffer receiveBuffer(21); // 21 == size of CSTOCL_WRITE_STATUS

	// Identifier of the first WRITE_DATA message we will send.
	// It has to be different than 0, so that we can distinguish status of the initial message
	// from statuses of the WRITE_DATA messages.
	uint32_t nextWriteId = 1;
	status = STATUS_OK;
	Timer lastMessageReceiveTimer;

	bool otherJobsAreWaiting;
	do {
		otherJobsAreWaiting = !queue_isempty(jqueue);
		if (lastMessageReceiveTimer.elapsed_ms() >= 2000) {
			syslog(LOG_WARNING,
					"file: %" PRIu32 ", index: %" PRIu32
					", chunk: %" PRIu64 ", version: %" PRIu32
					" - writeworker: connection with (%08" PRIX32 ":%" PRIu16
					") was timed out (unfinished writes: %" PRIu8
					"; try counter: %" PRIu32 ")",
					inodeData_->inode, chunkIndex_,
					chunkId_, chunkVersion_,
					chunkserverChain.head().ip, chunkserverChain.head().port,
					requestsWaitingForStatus_,
					inodeData_->trycnt + 1);
			break;
		}

		// If we have sent the previous message and have some time left, we can take
		// another block from current chunk to process it simultaneously. We won't take anything
		// new if we've already sent 15 blocks and didn't receive status from the chunkserver.
		if (!sendBuffer.hasDataToSend()
				&& wholeOperationTimer.elapsed_s() < (otherJobsAreWaiting ? 5 : 25)
				&& requestsWaitingForStatus_ < 15) {
			pthread_mutex_lock(&glock);
			bool haveNewData = tryGetNewBlockToWrite();
			// If there was any block worth sending, we create a new CLTOCS_WRITE_DATA message
			// and put it in our send buffer
			if (haveNewData) {
				currentBlock_->writeid = nextWriteId++;
				message.resize(32);
				size_t blockSize = currentBlock_->to - currentBlock_->from;
				uint8_t *wptr = message.data();
				put32bit(&wptr, CLTOCS_WRITE_DATA);
				put32bit(&wptr, 24 + blockSize);
				put64bit(&wptr, chunkId_);
				put32bit(&wptr, currentBlock_->writeid);
				put16bit(&wptr, currentBlock_->pos);
				put16bit(&wptr, currentBlock_->from);
				put32bit(&wptr, blockSize);
				put32bit(&wptr,mycrc32(0, currentBlock_->data + currentBlock_->from, blockSize));

				sendBuffer.reset();
				sendBuffer.addBufferToSend(message.data(), message.size());
				sendBuffer.addBufferToSend(currentBlock_->data + currentBlock_->from, blockSize);
				requestsWaitingForStatus_++;
				sendingCurrentBlockData_ = true;
			}
			pthread_mutex_unlock(&glock);
		}

		struct pollfd pfd[2];
		pfd[0].fd = fd;
		pfd[0].events = POLLIN | (sendBuffer.hasDataToSend() ? POLLOUT : 0);
		pfd[0].revents = 0;
		pfd[1].fd = inodeData_->pipe[0];
		pfd[1].events = POLLIN;
		pfd[1].revents = 0;
		if (poll(pfd, 2, 100) < 0) { /* correct timeout - in msec */
			syslog(LOG_WARNING, "writeworker: poll error: %s", strerr(errno));
			status = EIO;
			break;
		}
		pthread_mutex_lock(&glock);	// make helgrind happy
		inodeData_->waitingworker = 0;
		pthread_mutex_unlock(&glock); // make helgrind happy

		if (pfd[1].revents & POLLIN) {
			// used just to break poll - so just read all data from pipe to empty it
			ssize_t ret = read(inodeData_->pipe[0], pipebuff, 1024);
			if (ret < 0) { // mainly to make happy static code analyzers
				syslog(LOG_NOTICE, "read pipe error: %s", strerr(errno));
			}
		}

		if (pfd[0].revents & POLLIN) {
			lastMessageReceiveTimer.reset();
			ssize_t ret = receiveBuffer.readFrom(fd);
			if (ret == 0) { 	// connection reset by peer
				syslog(LOG_WARNING,
						"file: %" PRIu32 ", index: %" PRIu32
						", chunk: %" PRIu64 ", version: %" PRIu32
						" - writeworker: connection with (%08" PRIX32 ":%" PRIu16
						") was reset by peer (unfinished writes: %" PRIu8
						"; try counter: %" PRIu32 ")",
						inodeData_->inode, chunkIndex_,
						chunkId_, chunkVersion_,
						chunkserverChain.head().ip, chunkserverChain.head().port,
						requestsWaitingForStatus_,
						inodeData_->trycnt + 1);
				status = EIO;
				break;
			}

			if (receiveBuffer.isMessageTooBig()) {
				PacketHeader header = receiveBuffer.getMessageHeader();
				syslog(LOG_WARNING,
						"writeworker: got unrecognized packet from chunkserver (cmd:%" PRIu32
						",leng:%" PRIu32 ")", header.type, header.length);
				status = EIO;
				break;
			}

			while (receiveBuffer.hasMessageData()) {
				int messageProcessingStatus = processReceivedMessage(receiveBuffer);
				receiveBuffer.removeMessage();
				if (messageProcessingStatus != STATUS_OK) {
					status = messageProcessingStatus;
					break;
				}
			}
		}

		if (sendBuffer.hasDataToSend() && (pfd[0].revents & POLLOUT)) {
			ssize_t ret = sendBuffer.writeTo(fd);
			if (ret < 0) {
				syslog(LOG_WARNING,
						"file: %" PRIu32 ", index: %" PRIu32
						", chunk: %" PRIu64 ", version: %" PRIu32
						" - writeworker: connection with (%08" PRIX32 ":%" PRIu16
						") was reset by peer (unfinished writes: %" PRIu8
						"; try counter: %" PRIu32 ")",
						inodeData_->inode, chunkIndex_,
						chunkId_, chunkVersion_,
						chunkserverChain.head().ip, chunkserverChain.head().port,
						requestsWaitingForStatus_,
						inodeData_->trycnt + 1);
				status = EIO;
				break;
			}
			if (!sendBuffer.hasDataToSend()) {
				sendingCurrentBlockData_ = false;
			}
		}
	} while (requestsWaitingForStatus_ > 0
			&& wholeOperationTimer.elapsed_s() < (otherJobsAreWaiting ? 10 : 30));

	tcpclose(fd);

	int writeEndStatus;
	for (int retryCount = 0 ; retryCount < 10 ; ++retryCount) {
		writeEndStatus = fs_writeend(chunkId_, inodeData_->inode, fileLength_);
		if (writeEndStatus != STATUS_OK) {
			usleep(100000 + (10000 << retryCount));
		} else {
			break;
		}
	}

	if (writeEndStatus != STATUS_OK) {
		write_job_end(inodeData_, ENXIO, 0);
	} else if (status != STATUS_OK) {
		inodeData_->trycnt++;
		if (inodeData_->trycnt >= maxretries) {
			write_job_end(inodeData_, status, 0);
		} else {
			write_job_end(inodeData_, 0, 1 + std::min<int>(10, inodeData_->trycnt / 3));
		}
	} else {
		read_inode_ops(inodeData_->inode);
		write_job_end(inodeData_, 0, 0);
	}
}

/*
 * Check if there is any data worth sending to the chunkserver.
 * We will avoid sending blocks of size different than MFSBLOCKSIZE.
 * These can be taken only if we are close to run out of tasks to do.
 */
bool InodeChunkWriter::tryGetNewBlockToWrite() {
	if (currentBlock_ == NULL) {
		if (inodeData_->datachainhead) {
			uint32_t writeSize = inodeData_->datachainhead->to -
					inodeData_->datachainhead->from;
			if (writeSize == MFSBLOCKSIZE || requestsWaitingForStatus_ <= 1) {
				currentBlock_ = inodeData_->datachainhead;
				return true;
			}
		}
	} else {
		if (currentBlock_->next) {
			if (currentBlock_->next->chindx == chunkIndex_) {
				if (currentBlock_->next->to - currentBlock_->next->from == MFSBLOCKSIZE
						|| requestsWaitingForStatus_ <= 1) {
					currentBlock_ = currentBlock_->next;
					return true;
				}
			}
		} else {
			inodeData_->waitingworker = 1;
		}
	}
	return false;
}

int InodeChunkWriter::processReceivedMessage(const MessageReceiveBuffer& messageBuffer) {
	PacketHeader header = messageBuffer.getMessageHeader();
	if (header.type == ANTOAN_NOP && header.length == 0) {
		return STATUS_OK;
	} else if (header.type == CSTOCL_WRITE_STATUS && header.length == 13) {
		return processReceivedStatusMessage(messageBuffer.getMessageData());
	} else {
		syslog(LOG_WARNING,
				"writeworker: got unrecognized packet from chunkserver (cmd:%" PRIu32
				",leng:%" PRIu32 ")", header.type, header.length);
		return EIO;
	}
}

int InodeChunkWriter::processReceivedStatusMessage(const uint8_t* statusMessageData) {
	const uint8_t* rptr = statusMessageData;
	uint64_t receivedChunkId = get64bit(&rptr);
	uint32_t receivedWriteId = get32bit(&rptr);
	uint8_t receivedStatus = get8bit(&rptr);

	if (receivedChunkId != chunkId_) {
		syslog(LOG_WARNING,
				"writeworker: got unexpected packet (expected chunkdid:%" PRIu64
				",packet chunkid:%" PRIu64 ")",
				chunkId_, receivedChunkId);
		return EIO;
	}

	if (receivedStatus != STATUS_OK) {
		syslog(LOG_WARNING, "writeworker: write error: %s", mfsstrerr(receivedStatus));
		// convert MFS status to OS errno
		if (receivedStatus == ERROR_NOSPACE) {
			return ENOSPC;
		} else {
			return EIO;
		}
	}

	if (receivedWriteId > 0) { // TODO(msulikowski) Isn't this condition always true?
		pthread_mutex_lock(&glock);
		// Find the block, for which we've just received status
		cblock* acknowledgedBlock = inodeData_->datachainhead;
		while (acknowledgedBlock != NULL
				&& acknowledgedBlock->writeid != receivedWriteId) {
			acknowledgedBlock = acknowledgedBlock->next;
		}
		if (acknowledgedBlock == NULL) {
			syslog(LOG_WARNING,
					"writeworker: got unexpected status (writeid:%" PRIu32 ")",
					receivedWriteId);
			pthread_mutex_unlock(&glock);
			return EIO;
		}
		if (acknowledgedBlock == currentBlock_) {
			if (sendingCurrentBlockData_) {
				syslog(LOG_WARNING,
					"writeworker: got status OK before all data has been sent");
				pthread_mutex_unlock(&glock);
				return EIO;
			} else {
				currentBlock_ = NULL;
			}
		}

		// Remove the block from the list of blocks to write
		if (acknowledgedBlock->prev) {
			acknowledgedBlock->prev->next = acknowledgedBlock->next;
		} else {
			inodeData_->datachainhead = acknowledgedBlock->next;
		}
		if (acknowledgedBlock->next) {
			acknowledgedBlock->next->prev = acknowledgedBlock->prev;
		} else {
			inodeData_->datachaintail = acknowledgedBlock->prev;
		}

		// Update file size if changed
		uint64_t writtenOffset = static_cast<uint64_t>(chunkIndex_) * MFSCHUNKSIZE;
		writtenOffset += static_cast<uint64_t>(acknowledgedBlock->pos) * MFSBLOCKSIZE;
		writtenOffset += acknowledgedBlock->to;
		if (writtenOffset > fileLength_) {
			fileLength_ = writtenOffset;
		}
		write_cb_release(inodeData_, acknowledgedBlock);
		pthread_mutex_unlock(&glock);
	}
	requestsWaitingForStatus_--;
	return STATUS_OK;
}

/* main working thread | glock:UNLOCKED */
void* write_worker(void*) {
	InodeChunkWriter inodeDataWriter;
	for (;;) {
		// get next job
		uint32_t z1,z2,z3;
		uint8_t *data;
		queue_get(jqueue, &z1, &z2, &data, &z3);
		if (data == NULL) {
			return NULL;
		}

		// process the job
		inodeDataWriter.processJob((inodedata*)data);
	}
	return NULL;
}

/* API | glock: INITIALIZED,UNLOCKED */
void write_data_init (uint32_t cachesize,uint32_t retries) {
	uint32_t cacheblockcount = (cachesize/MFSBLOCKSIZE);
	uint32_t i;
	pthread_attr_t thattr;

	maxretries = retries;
	if (cacheblockcount<10) {
		cacheblockcount=10;
	}
	pthread_mutex_init(&glock,NULL);

	pthread_cond_init(&fcbcond,NULL);
	fcbwaiting=0;
	cacheblocks = (cblock*) malloc(sizeof(cblock)*cacheblockcount);
	for (i=0 ; i<cacheblockcount-1 ; i++) {
		cacheblocks[i].next = cacheblocks+(i+1);
	}
	cacheblocks[cacheblockcount-1].next = NULL;
	freecblockshead = cacheblocks;
	freecacheblocks = cacheblockcount;

	idhash = (inodedata**) malloc(sizeof(inodedata*)*IDHASHSIZE);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		idhash[i]=NULL;
	}

	dqueue = queue_new(0);
	jqueue = queue_new(0);

	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);
	pthread_create(&dqueue_worker_th,&thattr,write_dqueue_worker,NULL);
#ifdef BUFFER_DEBUG
	pthread_create(&info_worker_th,&thattr,write_info_worker,NULL);
#endif
	for (i=0 ; i<WORKERS ; i++) {
		pthread_create(write_worker_th+i,&thattr,write_worker,(void*)(unsigned long)(i));
	}
	pthread_attr_destroy(&thattr);
}

void write_data_term(void) {
	uint32_t i;
	inodedata *id,*idn;

	queue_put(dqueue,0,0,NULL,0);
	for (i=0 ; i<WORKERS ; i++) {
		queue_put(jqueue,0,0,NULL,0);
	}
	for (i=0 ; i<WORKERS ; i++) {
		pthread_join(write_worker_th[i],NULL);
	}
	pthread_join(dqueue_worker_th,NULL);
	queue_delete(dqueue);
	queue_delete(jqueue);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		for (id = idhash[i] ; id ; id = idn) {
			idn = id->next;
			pthread_cond_destroy(&(id->flushcond));
			pthread_cond_destroy(&(id->writecond));
			close(id->pipe[0]);
			close(id->pipe[1]);
			free(id);
		}
	}
	free(idhash);
	free(cacheblocks);
	pthread_cond_destroy(&fcbcond);
	pthread_mutex_destroy(&glock);
}

/* glock: LOCKED */
int write_cb_expand(cblock *cb,uint32_t from,uint32_t to,const uint8_t *data) {
	if (cb->writeid>0 || from>cb->to || to<cb->from) {	// can't expand
		return -1;
	}
	memcpy(cb->data+from,data,to-from);
	if (from<cb->from) {
		cb->from = from;
	}
	if (to>cb->to) {
		cb->to = to;
	}
	return 0;
}

/* glock: UNLOCKED */
int write_block(inodedata *id,uint32_t chindx,uint16_t pos,uint32_t from,uint32_t to,const uint8_t *data) {
	cblock *cb;

	pthread_mutex_lock(&glock);
	for (cb=id->datachaintail ; cb ; cb=cb->prev) {
		if (cb->pos==pos && cb->chindx==chindx) {
			if (write_cb_expand(cb,from,to,data)==0) {
				pthread_mutex_unlock(&glock);
				return 0;
			} else {
				break;
			}
		}
	}

	cb = write_cb_acquire(id);
//	syslog(LOG_NOTICE,"write_block: acquired new cache block");
	cb->chindx = chindx;
	cb->pos = pos;
	cb->from = from;
	cb->to = to;
	memcpy(cb->data+from,data,to-from);
	cb->prev = id->datachaintail;
	cb->next = NULL;
	if (id->datachaintail!=NULL) {
		id->datachaintail->next = cb;
	} else {
		id->datachainhead = cb;
	}
	id->datachaintail = cb;
	if (id->inqueue) {
		if (id->waitingworker) {
			if (write(id->pipe[1]," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			id->waitingworker=0;
		}
	} else {
		id->inqueue=1;
		write_enqueue(id);
	}
	pthread_mutex_unlock(&glock);
	return 0;
}

/* API | glock: UNLOCKED */
int write_data(void *vid,uint64_t offset,uint32_t size,const uint8_t *data) {
	uint32_t chindx;
	uint16_t pos;
	uint32_t from;
	int status;
	inodedata *id = (inodedata*)vid;
	if (id==NULL) {
		return EIO;
	}

//	gettimeofday(&s,NULL);
	pthread_mutex_lock(&glock);
//	syslog(LOG_NOTICE,"write_data: inode:%" PRIu32 " offset:%" PRIu32 " size:%" PRIu32,id->inode,offset,size);
	status = id->status;
	if (status==0) {
		if (offset+size>id->maxfleng) {	// move fleng
			id->maxfleng = offset+size;
		}
		id->writewaiting++;
		while (id->flushwaiting>0) {
			pthread_cond_wait(&(id->writecond),&glock);
		}
		id->writewaiting--;
	}
	pthread_mutex_unlock(&glock);
	if (status!=0) {
		return status;
	}

	chindx = offset>>MFSCHUNKBITS;
	pos = (offset&MFSCHUNKMASK)>>MFSBLOCKBITS;
	from = offset&MFSBLOCKMASK;
	while (size>0) {
		if (size>MFSBLOCKSIZE-from) {
			if (write_block(id,chindx,pos,from,MFSBLOCKSIZE,data)<0) {
				return EIO;
			}
			size -= (MFSBLOCKSIZE-from);
			data += (MFSBLOCKSIZE-from);
			from = 0;
			pos++;
			if (pos==1024) {
				pos = 0;
				chindx++;
			}
		} else {
			if (write_block(id,chindx,pos,from,from+size,data)<0) {
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
	if (id==NULL) {
		pthread_mutex_unlock(&glock);
		return NULL;
	}
	id->lcnt++;
	pthread_mutex_unlock(&glock);
	return id;
}

int write_data_flush(void *vid) {
	inodedata* id = (inodedata*)vid;
	int ret;
	if (id==NULL) {
		return EIO;
	}

//	gettimeofday(&s,NULL);
	pthread_mutex_lock(&glock);
	id->flushwaiting++;
	while (id->inqueue) {
//		syslog(LOG_NOTICE,"flush: wait ...");
		pthread_cond_wait(&(id->flushcond),&glock);
//		syslog(LOG_NOTICE,"flush: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting==0 && id->writewaiting>0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	if (id->lcnt==0 && id->inqueue==0 && id->flushwaiting==0 && id->writewaiting==0) {
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
	if (id==NULL) {
		pthread_mutex_unlock(&glock);
		return 0;
	}
	id->flushwaiting++;
	while (id->inqueue) {
//		syslog(LOG_NOTICE,"flush_inode: wait ...");
		pthread_cond_wait(&(id->flushcond),&glock);
//		syslog(LOG_NOTICE,"flush_inode: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting==0 && id->writewaiting>0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	if (id->lcnt==0 && id->inqueue==0 && id->flushwaiting==0 && id->writewaiting==0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
	return ret;
}

/* API | glock: UNLOCKED */
int write_data_end(void *vid) {
	inodedata* id = (inodedata*)vid;
	int ret;
	if (id==NULL) {
		return EIO;
	}
	pthread_mutex_lock(&glock);
	id->flushwaiting++;
	while (id->inqueue) {
//		syslog(LOG_NOTICE,"write_end: wait ...");
		pthread_cond_wait(&(id->flushcond),&glock);
//		syslog(LOG_NOTICE,"write_end: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting==0 && id->writewaiting>0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	id->lcnt--;
	if (id->lcnt==0 && id->inqueue==0 && id->flushwaiting==0 && id->writewaiting==0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
	return ret;
}
