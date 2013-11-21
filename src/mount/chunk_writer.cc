#include "mount/chunk_writer.h"

#include <poll.h>
#include <cstring>

#include "common/exceptions.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "mount/block_xor.h"
#include "mount/chunk_connector.h"
#include "mount/chunkserver_stats.h"
#include "mount/mastercomm.h"
#include "mount/read_operation_executor.h"
#include "mount/write_executor.h"

ChunkWriter::ChunkWriter(ChunkserverStats& chunkserverStats, ChunkConnector& connector,
		uint64_t chunkId, uint32_t chunkVersion)
	: connector_(connector),
	  chunkserverStats_(chunkserverStats),
	  chunkId_(chunkId),
	  chunkVersion_(chunkVersion),
	  currentWriteId_(0) {
}

ChunkWriter::~ChunkWriter() {
	abort();
}

void ChunkWriter::init(const std::vector<ChunkTypeWithAddress>& chunkLocations,
		uint32_t msTimeout) {
	Timeout connectTimeout{std::chrono::milliseconds(msTimeout)};
	for (const ChunkTypeWithAddress& location : chunkLocations) {
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
				chunkId_, chunkVersion_, location.chunkType));
		executors_.insert(std::make_pair(fd, std::move(executor)));
	}
	for (const auto& fdAndExecutor : executors_) {
		fdAndExecutor.second->addInitPacket();
	}
}

std::vector<ChunkWriter::WriteId> ChunkWriter::processOperations(uint32_t msTimeout) {
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
				if (status.chunkId != chunkId_) {
					throw ChunkserverConnectionException(
							"Received inconsistent write status message, "
							"expected chunk " + std::to_string(chunkId_) + ", "
							"got chunk " + std::to_string(status.chunkId),
							server);
				}
				if (status.status != STATUS_OK) {
					throw RecoverableWriteException("Chunk write error", status.status);
				}
				if (status.writeId == 0) {
					// Status of the write init message
					continue;
				}
				if (--unfinishedOperationCounters_[status.writeId] == 0) {
					finishedOperations.push_back(status.writeId);
					unfinishedOperationCounters_.erase(status.writeId);
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
			if (fdAndExecutor.second->getPendingPacketCount() == 0) {
				connector_.returnToPool(fdAndExecutor.first, fdAndExecutor.second->server());
				closedFds.push_back(fdAndExecutor.first);
			}
		}
		for (int fd : closedFds) {
			executors_.erase(fd);
		}
	}

	if (!executors_.empty()) {
		abort();
	}
}

void ChunkWriter::abort() {
	for (const auto& pair : executors_) {
		if (pair.first < 0) {
			continue;
		}
		tcpclose(pair.first);
	}
	executors_.clear();
}

ChunkWriter::WriteId ChunkWriter::addOperation(const uint8_t* data, uint32_t offset, uint32_t size) {
	sassert(size != 0);
	sassert(data != nullptr);
	sassert(size > 0 && size < MFSCHUNKSIZE);
	sassert((offset + size) <= MFSCHUNKSIZE);
	unfinishedOperationCounters_[++currentWriteId_] = 0;
	Timeout timeout{std::chrono::seconds(3)};
	// TODO this method assumes, that each chunkType has ONLY ONE executor!
	for (auto& fdAndExecutor : executors_) {
		WriteExecutor& executor = *fdAndExecutor.second;
		ChunkType chunkType = fdAndExecutor.second->chunkType();
		if (chunkType.isStandardChunkType()) {
			uint32_t block = offset / MFSBLOCKSIZE;
			uint32_t offsetInBlock = offset - block * MFSBLOCKSIZE;
			executor.addDataPacket(currentWriteId_, block, offsetInBlock, size, data);
			++unfinishedOperationCounters_[currentWriteId_];
		} else {
			if (chunkType.isXorParity()) {
				continue;
			}
			ChunkType::XorLevel level = chunkType.getXorLevel();
			ChunkType::XorPart part = chunkType.getXorPart();
			ChunkType parityChunkType = ChunkType::getXorParityChunkType(level);
			uint32_t blockOfChunk = offset / MFSBLOCKSIZE;
			if (blockOfChunk % level + 1 != part) {
				continue;
			}
			uint32_t block = blockOfChunk / level;

			// Read previous state of the data
			// TODO optimization possible if we know, that there are zeros!
			if (buffers_[chunkType][block].empty()) {
				ReadOperationPlanner::ReadOperation readOperation;
				readOperation.requestOffset = block * MFSBLOCKSIZE;
				readOperation.requestSize = MFSBLOCKSIZE;
				readOperation.destinationOffsets.push_back(0);
				buffers_[chunkType][block].resize(MFSBLOCKSIZE);
				int fd = connector_.connect(executor.server(), timeout);
				try {
					ReadOperationExecutor readExecutor(readOperation,
							chunkId_, chunkVersion_, chunkType,
							executor.server(), fd, buffers_[chunkType][block].data());
					readExecutor.sendReadRequest(timeout);
					while (!readExecutor.isFinished()) {
						readExecutor.continueReading();
					}
					connector_.returnToPool(fd, executor.server());
				} catch (...) {
					tcpclose(fd);
					throw;
				}
			}

			// Find an executor writing parity part of the current level
			WriteExecutor* parityWriteExecutor = nullptr;
			for (const auto& fdAndExecutor2 : executors_) {
				if (fdAndExecutor2.second->chunkType() == parityChunkType) {
					parityWriteExecutor = fdAndExecutor2.second.get();
				}
			}
			if (parityWriteExecutor == nullptr) {
				throw UnrecoverableWriteException(
						"No parity part for level" + std::to_string(level));
			}

			// Read previous state of the parity
			// TODO optimization possible if we know, that there are zeros!
			if (buffers_[parityChunkType][block].empty()) {
				ReadOperationPlanner::ReadOperation readOperation;
				readOperation.requestOffset = block * MFSBLOCKSIZE;
				readOperation.requestSize = MFSBLOCKSIZE;
				readOperation.destinationOffsets.push_back(0);
				buffers_[parityChunkType][block].resize(MFSBLOCKSIZE);
				int fd = connector_.connect(parityWriteExecutor->server(), timeout);
				try {
					ReadOperationExecutor readExecutor(readOperation,
							chunkId_, chunkVersion_, parityChunkType,
							parityWriteExecutor->server(), fd,
							buffers_[parityChunkType][block].data());
					readExecutor.sendReadRequest(timeout);
					while (!readExecutor.isFinished()) {
						readExecutor.continueReading();
					}
					connector_.returnToPool(fd, parityWriteExecutor->server());
				} catch (...) {
					tcpclose(fd);
					throw;
				}
			}

			// Update parity part in our cache
			uint32_t offsetInBlock = offset - MFSBLOCKSIZE * (offset / MFSBLOCKSIZE);
			blockXor(buffers_[parityChunkType][block].data() + offsetInBlock,
					buffers_[chunkType][block].data() + offsetInBlock,
					size);
			blockXor(buffers_[parityChunkType][block].data() + offsetInBlock, data, size);

			// Update data in our cache
			memcpy(buffers_[chunkType][block].data() + offsetInBlock, data, size);

			// Send a parity update request
			paritiesBeingSent_.push_back(std::move(std::vector<uint8_t>(
					buffers_[parityChunkType][block].data() + offsetInBlock,
					buffers_[parityChunkType][block].data() + offsetInBlock + size)));
			parityWriteExecutor->addDataPacket(currentWriteId_, block, offsetInBlock, size,
					paritiesBeingSent_.back().data());

			// Send data update request
			executor.addDataPacket(currentWriteId_, block, offsetInBlock, size, data);
			unfinishedOperationCounters_[currentWriteId_] += 2;
		}
	}
	return currentWriteId_;
}

uint32_t ChunkWriter::getUnfinishedOperationsCount() {
	uint32_t counter = 0;
	for (const auto& pair : unfinishedOperationCounters_) {
		if (pair.second) {
			++counter;
		}
	}
	return counter;
}
