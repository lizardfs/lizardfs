#include "mount/chunk_writer.h"

#include <poll.h>
#include <cstring>

#include "common/massert.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "mount/block_xor.h"
#include "mount/chunk_connector.h"
#include "mount/chunkserver_stats.h"
#include "mount/exceptions.h"
#include "mount/mastercomm.h"
#include "mount/read_operation_executor.h"
#include "mount/write_executor.h"

ChunkWriter::Operation::Operation(WriteId id, const uint8_t* data, uint32_t offset, uint32_t size)
		: id(id),
		  data(data),
		  offset(offset),
		  size(size) {
	sassert(size != 0);
	sassert(data != nullptr);
	sassert(size > 0);
	sassert((offset + size) <= MFSCHUNKSIZE);
	sassert(offset % MFSBLOCKSIZE + size <= MFSBLOCKSIZE);
}


ChunkWriter::ChunkWriter(
		ChunkserverStats& chunkserverStats,
		ChunkConnector& connector)
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
	sassert(unfinishedOperationCounters_.empty());
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
		unfinishedOperationCounters_[currentWriteId_]++;
	}
}

std::vector<ChunkWriter::WriteId> ChunkWriter::processOperations(uint32_t msTimeout) {
	if (!newOperations.empty()) {
		for (const auto& operation : newOperations) {
			startOperation(operation);
		}
		newOperations.clear();
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
		return std::move(std::vector<ChunkWriter::WriteId>());
	}

	std::vector<ChunkWriter::WriteId> finishedOperations;
	for (pollfd& pollFd : pollFds) {
		auto it = executors_.find(pollFd.fd);
		sassert(it != executors_.end());
		WriteExecutor& executor = *it->second;
		const NetworkAddress& server = executor.server();

		if (pollFd.revents & POLLOUT) {
			executor.sendData();
		}

		if (pollFd.revents & POLLIN) {
			std::vector<WriteExecutor::Status> statuses = executor.receiveData();
			for (const auto& status : statuses) {
				if (status.chunkId != locator_.locationInfo().chunkId) {
					throw ChunkserverConnectionException(
							"Received inconsistent write status message"
							", expected chunk " + std::to_string(locator_.locationInfo().chunkId) +
							", got chunk " + std::to_string(status.chunkId),
							server);
				}
				if (status.status != STATUS_OK) {
					throw RecoverableWriteException("Chunk write error", status.status);
				}
				if (--unfinishedOperationCounters_[status.writeId] == 0) {
					if (status.writeId != 0) {
						finishedOperations.push_back(status.writeId);
					}
					unfinishedOperationCounters_.erase(status.writeId);
					// Update file size if changed
					uint64_t writtenOffset = offsetOfEnd_[status.writeId];
					offsetOfEnd_.erase(status.writeId);
					if (writtenOffset > locator_.locationInfo().fileLength) {
						locator_.updateFileLength(writtenOffset);
					}
				}
			}
		}
		if (pollFd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
			throw ChunkserverConnectionException(
					"Write to chunkserver (poll) error", executor.server());
		}
	}
	return std::move(finishedOperations);
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

ChunkWriter::WriteId ChunkWriter::addOperation(const uint8_t* data, uint32_t offset, uint32_t size) {
	newOperations.push_back(Operation(++currentWriteId_, data, offset, size));
	return currentWriteId_;
}

void ChunkWriter::startOperation(const Operation& operation) {
	unfinishedOperationCounters_[operation.id] = 0;
	const ChunkLocationInfo& chunkLocationInfo = locator_.locationInfo();
	Timeout timeout{std::chrono::seconds(10)};
	// TODO this method assumes, that each chunkType has ONLY ONE executor!
	for (auto& fdAndExecutor : executors_) {
		WriteExecutor& executor = *fdAndExecutor.second;
		ChunkType chunkType = fdAndExecutor.second->chunkType();
		offsetOfEnd_[operation.id] = static_cast<uint64_t>(index_) * MFSCHUNKSIZE +
				operation.offset + operation.size;
		if (chunkType.isStandardChunkType()) {
			uint32_t block = operation.offset / MFSBLOCKSIZE;
			uint32_t offsetInBlock = operation.offset - block * MFSBLOCKSIZE;
			executor.addDataPacket(operation.id, block, offsetInBlock,
					operation.size, operation.data);
			++unfinishedOperationCounters_[operation.id];
		} else {
			if (chunkType.isXorParity()) {
				continue;
			}
			ChunkType::XorLevel level = chunkType.getXorLevel();
			ChunkType::XorPart part = chunkType.getXorPart();
			ChunkType parityChunkType = ChunkType::getXorParityChunkType(level);
			uint32_t blockOfChunk = operation.offset / MFSBLOCKSIZE;
			if (blockOfChunk % level + 1 != part) {
				continue;
			}
			uint32_t block = blockOfChunk / level;

			// Read previous state of the data
			std::vector<uint8_t>& dataBlock = buffers_[chunkType][block];
			if (dataBlock.empty()) {
				uint64_t blockOffsetInFile = index_ * MFSCHUNKSIZE + blockOfChunk * MFSBLOCKSIZE;
				if (blockOffsetInFile >= chunkLocationInfo.fileLength) {
					dataBlock.resize(MFSBLOCKSIZE);
					memset(dataBlock.data(), 0, MFSBLOCKSIZE);
				} else {
					ReadPlanner::ReadOperation readOperation;
					readOperation.requestOffset = block * MFSBLOCKSIZE;
					readOperation.requestSize = MFSBLOCKSIZE;
					readOperation.readDataOffsets.push_back(0);
					buffers_[chunkType][block].resize(MFSBLOCKSIZE);
					int fd = connector_.connect(executor.server(), timeout);
					try {
						ReadOperationExecutor readExecutor(readOperation,
								chunkLocationInfo.chunkId, chunkLocationInfo.version, chunkType,
								executor.server(), fd, buffers_[chunkType][block].data());
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
			std::vector<uint8_t>& parityBlock = buffers_[parityChunkType][block];
			if (parityWriteExecutor != nullptr && parityBlock.empty()) {
				uint32_t chunkOffset = index_ * MFSCHUNKSIZE;
				uint32_t xorBlockSize = level * MFSBLOCKSIZE;
				uint32_t firstXorBlockOffset = (operation.offset / xorBlockSize) * xorBlockSize;
				if (chunkOffset + firstXorBlockOffset >= chunkLocationInfo.fileLength) {
					parityBlock.resize(MFSBLOCKSIZE);
					memset(parityBlock.data(), 0, MFSBLOCKSIZE);
				} else {
					ReadPlanner::ReadOperation readOperation;
					readOperation.requestOffset = block * MFSBLOCKSIZE;
					readOperation.requestSize = MFSBLOCKSIZE;
					readOperation.readDataOffsets.push_back(0);
					buffers_[parityChunkType][block].resize(MFSBLOCKSIZE);
					int fd = connector_.connect(parityWriteExecutor->server(), timeout);
					try {
						ReadOperationExecutor readExecutor(readOperation,
								chunkLocationInfo.chunkId, chunkLocationInfo.version, parityChunkType,
								parityWriteExecutor->server(), fd,
								buffers_[parityChunkType][block].data());
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
			uint32_t offsetInBlock = operation.offset -
					MFSBLOCKSIZE * (operation.offset / MFSBLOCKSIZE);
			if (parityWriteExecutor != nullptr) {
				blockXor(parityBlock.data() + offsetInBlock,
						dataBlock.data() + offsetInBlock,
						operation.size);
				blockXor(parityBlock.data() + offsetInBlock, operation.data, operation.size);
			}

			// Update data in our cache
			memcpy(dataBlock.data() + offsetInBlock, operation.data, operation.size);

			// Send a parity update request
			if (parityWriteExecutor != nullptr) {
				paritiesBeingSent_.push_back(std::move(std::vector<uint8_t>(
						parityBlock.data() + offsetInBlock,
						parityBlock.data() + offsetInBlock + operation.size)));
				parityWriteExecutor->addDataPacket(operation.id, block, offsetInBlock,
						operation.size, paritiesBeingSent_.back().data());
				unfinishedOperationCounters_[operation.id] += 1;
			}

			// Send data update request
			executor.addDataPacket(operation.id, block,
					offsetInBlock, operation.size, operation.data);
			unfinishedOperationCounters_[operation.id] += 1;
		}
	}
}

uint32_t ChunkWriter::getUnfinishedOperationsCount() {
	return unfinishedOperationCounters_.size() + newOperations.size();
}
