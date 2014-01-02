#include "mount/chunk_writer.h"

#include <poll.h>
#include <cstring>

#include "common/block_xor.h"
#include "common/massert.h"
#include "common/read_operation_executor.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "mount/exceptions.h"
#include "mount/mastercomm.h"
#include "mount/write_executor.h"

ChunkWriter::ChunkWriter(ChunkserverStats& chunkserverStats, ChunkConnector& connector)
	: chunkserverStats_(chunkserverStats),
	  connector_(connector),
	  inode_(0),
	  index_(0),
	  currentWriteId_(0) {
}

ChunkWriter::~ChunkWriter() {
	try {
		abortOperations();
	} catch (...) {
	}
}

void ChunkWriter::init(uint32_t inode, uint32_t index, uint32_t msTimeout) {
	sassert(currentWriteId_ == 0);
	sassert(pendingOperations_.empty());
	sassert(executors_.empty());

	Timeout connectTimeout{std::chrono::milliseconds(msTimeout)};
	locator_.locateAndLockChunk(inode, index);
	inode_ = inode;
	index_ = index;

	const ChunkLocationInfo& chunkLocationInfo = locator_.locationInfo();
	for (const ChunkTypeWithAddress& location : locator_.locationInfo().locations) {
		bool addedToChain = false;
		for (auto& fdAndExecutor : executors_) {
			if (fdAndExecutor.second->chunkType() == location.chunkType) {
				fdAndExecutor.second->addChunkserverToChain(location.address);
				addedToChain = true;
			}
		}
		if (addedToChain) {
			continue;
		}

		int fd = connector_.connect(location.address, connectTimeout);
		std::unique_ptr<WriteExecutor> executor(new WriteExecutor(
				chunkserverStats_, location.address, fd,
				chunkLocationInfo.chunkId, chunkLocationInfo.version, location.chunkType));
		executors_.insert(std::make_pair(fd, std::move(executor)));
	}
	for (const auto& fdAndExecutor : executors_) {
		fdAndExecutor.second->addInitPacket();
		pendingOperations_[currentWriteId_].unfinishedWrites++;
	}
}

void ChunkWriter::processOperations(uint32_t msTimeout) {
	if (!newOperations_.empty()) {
		for (auto& operation : newOperations_) {
			startOperation(std::move(operation));
		}
		newOperations_.clear();
	}

	std::vector<pollfd> pollFds;
	for (const auto& pair : executors_) {
		pollFds.push_back(pollfd());
		pollFds.back().fd = pair.first;
		pollFds.back().events = POLLIN;
		if (pair.second->getPendingPacketCount() > 0) {
			pollFds.back().events |= POLLOUT;
		}
		pollFds.back().revents = 0;
	}

	int status = poll(pollFds.data(), pollFds.size(), msTimeout);
	if (status < 0) {
		throw RecoverableWriteException("Poll error: " + std::string(strerr(errno)));
	} else if (status == 0) {
		return;
	}

	for (pollfd& pollFd : pollFds) {
		auto it = executors_.find(pollFd.fd);
		sassert(it != executors_.end());
		WriteExecutor& executor = *it->second;

		if (pollFd.revents & POLLOUT) {
			executor.sendData();
		}

		if (pollFd.revents & POLLIN) {
			std::vector<WriteExecutor::Status> statuses = executor.receiveData();
			for (const auto& status : statuses) {
				processStatus(executor, status);
			}
		}

		if (pollFd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
			throw ChunkserverConnectionException(
					"Write to chunkserver (poll) error", executor.server());
		}
	}
}

uint32_t ChunkWriter::getUnfinishedOperationsCount() {
	return pendingOperations_.size() + newOperations_.size();
}

void ChunkWriter::finish(uint32_t msTimeout) {
	sassert(getUnfinishedOperationsCount() == 0);
	for (auto& pair : executors_) {
		pair.second->addEndPacket();
	}
	Timeout timeout{std::chrono::milliseconds(msTimeout)};
	while (!timeout.expired() && !executors_.empty()) {
		processOperations(timeout.remaining_ms());
		std::vector<int> closedFds;
		for (auto& fdAndExecutor : executors_) {
			int fd = fdAndExecutor.first;
			const WriteExecutor& executor = *fdAndExecutor.second;
			if (executor.getPendingPacketCount() == 0) {
				connector_.returnToPool(fd, executor.server());
				closedFds.push_back(fd);
			}
		}
		for (int fd : closedFds) {
			executors_.erase(fd);
		}
	}
	releaseChunk();
}

void ChunkWriter::abortOperations() {
	for (const auto& pair : executors_) {
		if (pair.first < 0) {
			continue;
		}
		tcpclose(pair.first);
	}
	executors_.clear();
	releaseChunk();
}

std::list<WriteCacheBlock> ChunkWriter::releaseJournal() {
	std::list<WriteCacheBlock> tmp;
	std::swap(tmp, journal_);
	return tmp;
}

void ChunkWriter::releaseChunk() {
	int retryCount = 0;
	while (true) {
		try {
			locator_.unlockChunk();
			break;
		} catch (Exception& ex) {
			if (++retryCount == 10) {
				throw;
			}
			usleep(100000 + (10000 << retryCount));
		}
	}
}

void ChunkWriter::addOperation(WriteCacheBlock&& block) {
	sassert(block.chunkIndex == index_);
	Operation operation;
	operation.offsetOfEnd = block.offsetInFile() + block.size();
	journal_.push_back(std::move(block));
	operation.journalPosition = std::prev(journal_.end());
	newOperations_.push_back(std::move(operation));
}

void ChunkWriter::startOperation(Operation&& operation) {
	const ChunkLocationInfo& chunkLocationInfo = locator_.locationInfo();
	Timeout timeout{std::chrono::seconds(10)};
	WriteId id = ++currentWriteId_;
	const WriteCacheBlock& block = *operation.journalPosition;
	// TODO this method assumes, that each chunkType has ONLY ONE executor!
	for (auto& fdAndExecutor : executors_) {
		WriteExecutor& executor = *fdAndExecutor.second;
		ChunkType chunkType = fdAndExecutor.second->chunkType();
		if (chunkType.isStandardChunkType()) {
			executor.addDataPacket(id, block.blockIndex, block.from, block.size(), block.data());
			operation.unfinishedWrites++;
		} else {
			if (chunkType.isXorParity()) {
				continue;
			}
			ChunkType::XorLevel level = chunkType.getXorLevel();
			ChunkType::XorPart part = chunkType.getXorPart();
			ChunkType parityChunkType = ChunkType::getXorParityChunkType(level);
			if (block.blockIndex % level + 1 != part) {
				continue;
			}

			uint32_t stripe = block.blockIndex / level;

			// Read previous state of the data
			std::vector<uint8_t>& dataBuffer = buffers_[chunkType][stripe];
			if (dataBuffer.empty()) {
				if (block.offsetInFile() >= chunkLocationInfo.fileLength) {
					dataBuffer.resize(MFSBLOCKSIZE);
					memset(dataBuffer.data(), 0, MFSBLOCKSIZE);
				} else {
					ReadPlanner::ReadOperation readOperation;
					readOperation.requestOffset = stripe * MFSBLOCKSIZE;
					readOperation.requestSize = MFSBLOCKSIZE;
					readOperation.readDataOffsets.push_back(0);
					buffers_[chunkType][stripe].resize(MFSBLOCKSIZE);
					int fd = connector_.connect(executor.server(), timeout);
					try {
						ReadOperationExecutor readExecutor(readOperation,
								chunkLocationInfo.chunkId, chunkLocationInfo.version, chunkType,
								executor.server(), fd, buffers_[chunkType][stripe].data());
						readExecutor.sendReadRequest(timeout);
						readExecutor.readAll(timeout);
						connector_.returnToPool(fd, executor.server());
					} catch (...) {
						tcpclose(fd);
						throw;
					}
				}
			}

			// Find an executor writing parity part of the current level. Might be equal to nullptr
			WriteExecutor* parityWriteExecutor = nullptr;
			for (const auto& fdAndExecutor2 : executors_) {
				if (fdAndExecutor2.second->chunkType() == parityChunkType) {
					parityWriteExecutor = fdAndExecutor2.second.get();
				}
			}

			// Read previous state of the parity
			// TODO optimization possible if we know, that there are zeros!
			std::vector<uint8_t>& parityBuffer = buffers_[parityChunkType][stripe];
			if (parityWriteExecutor != nullptr && parityBuffer.empty()) {
				uint32_t chunkOffset = index_ * MFSCHUNKSIZE;
				uint32_t xorBlockSize = level * MFSBLOCKSIZE;
				uint32_t firstXorBlockOffset = (block.offsetInChunk() / xorBlockSize) * xorBlockSize;
				if (chunkOffset + firstXorBlockOffset >= chunkLocationInfo.fileLength) {
					parityBuffer.resize(MFSBLOCKSIZE);
					memset(parityBuffer.data(), 0, MFSBLOCKSIZE);
				} else {
					ReadPlanner::ReadOperation readOperation;
					readOperation.requestOffset = stripe * MFSBLOCKSIZE;
					readOperation.requestSize = MFSBLOCKSIZE;
					readOperation.readDataOffsets.push_back(0);
					buffers_[parityChunkType][stripe].resize(MFSBLOCKSIZE);
					int fd = connector_.connect(parityWriteExecutor->server(), timeout);
					try {
						ReadOperationExecutor readExecutor(readOperation,
								chunkLocationInfo.chunkId, chunkLocationInfo.version, parityChunkType,
								parityWriteExecutor->server(), fd,
								buffers_[parityChunkType][stripe].data());
						readExecutor.sendReadRequest(timeout);
						readExecutor.readAll(timeout);
						connector_.returnToPool(fd, parityWriteExecutor->server());
					} catch (...) {
						tcpclose(fd);
						throw;
					}
				}
			}

			// Update parity part in our cache
			if (parityWriteExecutor != nullptr) {
				blockXor(parityBuffer.data() + block.from,
						dataBuffer.data() + block.from,
						block.size());
				blockXor(parityBuffer.data() + block.from, block.data(), block.size());
			}

			// Update data in our cache
			memcpy(dataBuffer.data() + block.from, block.data(), block.size());

			// Send a parity update request
			if (parityWriteExecutor != nullptr) {
				operation.parityBuffers.push_back(std::move(std::vector<uint8_t>(
						parityBuffer.data() + block.from,
						parityBuffer.data() + block.from + block.size())));
				parityWriteExecutor->addDataPacket(id, stripe, block.from,
						block.size(), operation.parityBuffers.back().data());
				operation.unfinishedWrites += 1;
			}

			// Send data update request
			executor.addDataPacket(id, stripe, block.from, block.size(), block.data());
			operation.unfinishedWrites += 1;
		}
	}
	pendingOperations_[id] = std::move(operation);
}

void ChunkWriter::processStatus(const WriteExecutor& executor,
		const WriteExecutor::Status& status) {
	if (status.chunkId != locator_.locationInfo().chunkId) {
		throw ChunkserverConnectionException(
				"Received inconsistent write status message"
				", expected chunk " + std::to_string(locator_.locationInfo().chunkId) +
				", got chunk " + std::to_string(status.chunkId),
				executor.server());
	}
	if (status.status != STATUS_OK) {
		throw RecoverableWriteException("Chunk write error", status.status);
	}
	if (pendingOperations_.count(status.writeId) == 0) {
		throw RecoverableWriteException(
				"Chunk write error: unexpected status for operation #" +
				std::to_string(status.writeId));
	}
	auto& operation = pendingOperations_[status.writeId];
	if (--operation.unfinishedWrites == 0) {
		// Operation has just finished: update file size if changed and delete the operation
		if (status.writeId != 0) {
			// This was a WRITE_DATA operation, not WRITE_INIT
			if (operation.offsetOfEnd > locator_.locationInfo().fileLength) {
				locator_.updateFileLength(operation.offsetOfEnd);
			}
			journal_.erase(operation.journalPosition);
		}
		pendingOperations_.erase(status.writeId);
	}
}
