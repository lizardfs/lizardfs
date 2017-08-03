/*
   Copyright 2013-2017 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "mount/chunk_writer.h"

#include <algorithm>
#include <bitset>
#include <cstring>

#include "common/block_xor.h"
#include "common/chunk_connector.h"
#include "common/chunk_read_planner.h"
#include "common/chunk_type_with_address.h"
#include "common/exceptions.h"
#include "common/goal.h"
#include "common/massert.h"
#include "common/read_operation_executor.h"
#include "common/read_plan_executor.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"
#include "mount/mastercomm.h"
#include "mount/readdata.h"

static uint32_t gcd(uint32_t a, uint32_t b) {
	for (;;) {
		if (a == 0) {
			return b;
		}
		b %= a;
		if (b == 0) {
			return a;
		}
		a %= b;
	}
}

ChunkWriter::Operation::Operation() : unfinishedWrites(0), offsetOfEnd(0) {}

bool ChunkWriter::Operation::isExpandPossible(JournalPosition newPosition, uint32_t stripeSize) {
	// If the operation is not empty, the new JournalPosition has to be compatible with
	// the previous elements of the operation, ie. we can only expand by a new block
	// of the same stripe and the same (from, to) range
	for (const JournalPosition& position : journalPositions) {
		sassert(newPosition->chunkIndex == position->chunkIndex);
		if (newPosition->from != position->from
				|| newPosition->to != position->to
				|| (newPosition->blockIndex / stripeSize) != (position->blockIndex / stripeSize)
				|| newPosition->blockIndex == position->blockIndex) {
			return false;
		}
	}
	return true;
}

void ChunkWriter::Operation::expand(JournalPosition newPosition) {
	sassert(newPosition->type != WriteCacheBlock::kParityBlock);
	uint64_t newOffsetOfEnd = newPosition->offsetInFile() + newPosition->size();
	if (newPosition->type != WriteCacheBlock::kReadBlock && newOffsetOfEnd > offsetOfEnd) {
		offsetOfEnd = newOffsetOfEnd;
	}
	journalPositions.push_back(newPosition);
}

bool ChunkWriter::Operation::collidesWith(const Operation& operation) const {
	for (const auto& position1 : journalPositions) {
		for (const auto& position2 : operation.journalPositions) {
			sassert(position1->chunkIndex == position2->chunkIndex);
			if (position1->blockIndex != position2->blockIndex
					|| position1->from >= position2->to
					|| position1->to <= position2->from) {
				continue;
			}
			return true;
		}
	}
	return false;
}

bool ChunkWriter::Operation::isFullStripe(uint32_t stripeSize) const {
	if (journalPositions.empty()) {
		return false;
	}
	uint32_t elementsInStripe = stripeSize;
	// The last one is shorter when MFSBLOCKSINCHUNK % stripeSize != 0
	uint32_t stripe = journalPositions.front()->blockIndex / stripeSize;
	if (stripe == (MFSBLOCKSINCHUNK - 1) / stripeSize && MFSBLOCKSINCHUNK % stripeSize != 0) {
		elementsInStripe = MFSBLOCKSINCHUNK % stripeSize;
	}
	return (journalPositions.size() == elementsInStripe);
}

ChunkWriter::ChunkWriter(ChunkserverStats& chunkserverStats, ChunkConnector& connector,
		int dataChainFd)
	: chunkserverStats_(chunkserverStats),
	  connector_(connector),
	  locator_(nullptr),
	  idCounter_(0),
	  acceptsNewOperations_(true),
	  combinedStripeSize_(0),
	  dataChainFd_(dataChainFd) {
}

ChunkWriter::~ChunkWriter() {
	try {
		abortOperations();
	} catch (...) {
	}
}

void ChunkWriter::init(WriteChunkLocator* locator, uint32_t chunkserverTimeout_ms) {
	LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::init");
	sassert(pendingOperations_.empty());
	sassert(executors_.empty());

	Timeout connectTimeout{std::chrono::milliseconds(chunkserverTimeout_ms)};
	combinedStripeSize_ = 0;
	locator_ = locator;

	for (const ChunkTypeWithAddress& location : locator_->locationInfo().locations) {
		// If we have an executor writing the same chunkType, use it
		bool addedToChain = false;
		for (auto& fdAndExecutor : executors_) {
			if (fdAndExecutor.second->chunkType() == location.chunk_type) {
				fdAndExecutor.second->addChunkserverToChain(location);
				addedToChain = true;
			}
		}
		if (addedToChain) {
			continue;
		}

		// Update combinedStripeSize_
		uint32_t stripeSize = slice_traits::getStripeSize(location.chunk_type);
		if (combinedStripeSize_ == 0) {
			combinedStripeSize_ = stripeSize;
		} else {
			combinedStripeSize_ =
					stripeSize * combinedStripeSize_ / gcd(combinedStripeSize_, stripeSize);
		}

		// Create an executor
		int fd = connector_.startUsingConnection(location.address, connectTimeout);
		std::unique_ptr<WriteExecutor> executor(new WriteExecutor(
				chunkserverStats_, location.address, location.chunkserver_version, fd,
				chunkserverTimeout_ms, locator_->locationInfo().chunkId, locator_->locationInfo().version,
				location.chunk_type));
		executors_.insert(std::make_pair(fd, std::move(executor)));
	}

	// Initialize all the executors -- this is a special operation with id=0
	for (const auto& fdAndExecutor : executors_) {
		fdAndExecutor.second->addInitPacket();
		pendingOperations_[0].unfinishedWrites++;
	}
}

uint32_t ChunkWriter::getMinimumBlockCountWorthWriting() {
	return combinedStripeSize_;
}

uint32_t ChunkWriter::startNewOperations(bool can_expect_next_block) {
	LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::startNewOperations");
	uint32_t operationsStarted = 0;
	// Start all possible operations. Break at the first operation that can't be started, because
	// we have to preserve the order of operations in order to ensure the files contain proper data
	for (auto i = newOperations_.begin(); i != newOperations_.end(); i = newOperations_.erase(i)) {
		Operation& operation = *i;
		// Don't start partial-stripe writes if they can be extended in the future.
		// Only the last one can be expanded and only if we accept new data.
		if (i == std::prev(newOperations_.end())
				&& acceptsNewOperations_
				&& !operation.isFullStripe(combinedStripeSize_)
				&& can_expect_next_block) {
			break;
		}
		if (!canStartOperation(operation)) {
			break;
		}
		startOperation(std::move(operation));
		++operationsStarted;
	}
	return operationsStarted;
}

void ChunkWriter::processOperations(uint32_t msTimeout) {
	LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::processOperations");
	LOG_AVG_TILL_END_OF_SCOPE1("ChunkWriter::processOperations#op", getPendingOperationsCount());
	std::vector<pollfd> pollFds;
	if (dataChainFd_ >= 0) {
		pollFds.push_back(pollfd());
		pollFds.back().fd = dataChainFd_;
		pollFds.back().events = POLLIN;
		pollFds.back().revents = 0;
	}
	for (const auto& pair : executors_) {
		pollFds.push_back(pollfd());
		pollFds.back().fd = pair.first;
		pollFds.back().events = POLLIN;
		if (pair.second->getPendingPacketCount() > 0) {
			pollFds.back().events |= POLLOUT;
		}
		pollFds.back().revents = 0;
	}

	// NOTICE: On Linux there can be pipe descriptor in pollFds.
	// This function can handle it.
	int status = tcppoll(pollFds, msTimeout);
	if (status < 0) {
		throw RecoverableWriteException("Poll error: " + std::string(strerr(tcpgetlasterror())));
	}

	for (pollfd& pollFd : pollFds) {
		if (pollFd.fd == dataChainFd_) {
			if (pollFd.revents & POLLIN) {
				const uint32_t dataFdBufferSize = 1024;
				uint8_t dataFdBuffer[dataFdBufferSize];
				if (read(dataChainFd_, dataFdBuffer, dataFdBufferSize) < 0) {
					lzfs_pretty_syslog(LOG_NOTICE, "read pipe error: %s", strerr(errno));
				}
			}
		} else {
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

			if (executor.serverTimedOut()) {
				throw ChunkserverConnectionException("Chunkserver timed out", executor.server());
			}
		}
	}
}

uint32_t ChunkWriter::getUnfinishedOperationsCount() {
	return pendingOperations_.size() + newOperations_.size();
}

uint32_t ChunkWriter::getPendingOperationsCount() {
	return pendingOperations_.size();
}


void ChunkWriter::startFlushMode() {
	sassert(acceptsNewOperations_);
	acceptsNewOperations_ = false;
}

void ChunkWriter::dropNewOperations() {
	sassert(acceptsNewOperations_);
	newOperations_.clear();
	acceptsNewOperations_ = false;
}

void ChunkWriter::finish(uint32_t msTimeout) {
	LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::finish");
	sassert(getPendingOperationsCount() == 0);
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
				connector_.endUsingConnection(fd, executor.server());
				closedFds.push_back(fd);
			}
		}
		for (int fd : closedFds) {
			executors_.erase(fd);
		}
	}
}

void ChunkWriter::abortOperations() {
	LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::abortOperations");
	for (const auto& pair : executors_) {
		if (pair.first < 0) {
			continue;
		}
		tcpclose(pair.first);
	}
	executors_.clear();
}

std::list<WriteCacheBlock> ChunkWriter::releaseJournal() {
	return std::move(journal_);
}

void ChunkWriter::addOperation(WriteCacheBlock&& block) {
	sassert(block.type != WriteCacheBlock::kParityBlock);
	sassert(acceptsNewOperations_);
	sassert(block.chunkIndex == locator_->chunkIndex());
	if (block.type == WriteCacheBlock::kWritableBlock) {
		// Block is writeable until the first try of writing it to chunkservers, ie. until now
		block.type = WriteCacheBlock::kReadOnlyBlock;
	}
	journal_.push_back(std::move(block));
	JournalPosition journalPosition = std::prev(journal_.end());
	if (newOperations_.empty()
			|| !newOperations_.back().isExpandPossible(journalPosition, combinedStripeSize_)) {
		newOperations_.push_back(Operation());
		newOperations_.back().expand(journalPosition);
	} else {
		newOperations_.back().expand(journalPosition);
	}
}

bool ChunkWriter::canStartOperation(const Operation& operation) {
	// Don't start operations which intersect with some pending operation
	// Starting them may result in reading old version of data when calculating new parity.
	for (const auto& writeIdAndOperation : pendingOperations_) {
		const auto& pendingOperation = writeIdAndOperation.second;
		if (operation.collidesWith(pendingOperation)) {
			return false;
		}
	}
	return true;
}

/*!
 * Computes parity block value and writes it to memory pointed by parity_block.
 * \param chunk_type type of the chunk
 * \param parity_block address of output buffer
 * \param data_blocks array of pointers to data blocks
 * \param offset index of first block to be computed
 * \param size size of data to be computed
 */
void ChunkWriter::computeParityBlock(const ChunkPartType &chunk_type, uint8_t *parity_block,
		const std::vector<uint8_t *> &data_blocks, int offset, int size) {
	int data_part_count = slice_traits::getNumberOfDataParts(chunk_type);
	int parity_part_count = slice_traits::getNumberOfParityParts(chunk_type);

	assert(slice_traits::isParityPart(chunk_type));
	assert(parity_block);

	if (slice_traits::isXor(chunk_type)) {
		assert(data_blocks[offset]);
		std::memcpy(parity_block, data_blocks[offset], size);
		for (int i = 1; i < data_part_count; ++i) {
			if(data_blocks[offset + i]) {
				blockXor(parity_block, data_blocks[offset + i], size);
			}
		}
		return;
	}

	assert(slice_traits::isEC(chunk_type));

	typedef ReedSolomon<slice_traits::ec::kMaxDataCount, slice_traits::ec::kMaxParityCount> RS;
	RS rs(data_part_count, parity_part_count);
	RS::ErasedMap erased;
	RS::ConstFragmentMap data_parts{{0}};
	RS::FragmentMap result_parts{{0}};

	for (int i = 0; i < parity_part_count; ++i) {
		erased.set(data_part_count + i);
	}
	for (int i = 0; i < data_part_count; ++i) {
		data_parts[i] = data_blocks[offset + i];
	}
	result_parts[chunk_type.getSlicePart()] = parity_block;

	rs.recover(data_parts, erased, result_parts, size);
}

/*!
 * Fills given operation with a range of blocks required to be read.
 * \param operation operation to be filled
 * \param first_block first block of the operation
 * \param first_index index of the first block required to be read
 * \param size number of required blocks
 * \param stripe_element map of blocks in a stripe
 */
void ChunkWriter::fillOperation(Operation &operation, int first_block, int first_index, int size,
		std::vector<uint8_t *> &stripe_element) {
	assert(size >= 0);
	if (size == 0) {
		return;
	}
	int block_from = operation.journalPositions.front()->from;
	int block_to = operation.journalPositions.front()->to;

	std::vector<WriteCacheBlock> blocks;
	blocks.reserve(size);
	readBlocks(first_block + first_index, size, block_from, block_to, blocks);
	assert(blocks.size() == (size_t)size);

	for (int index = 0; index < size; ++index) {
		// Insert the new block into the journal just after the last block of the operation
		auto position = journal_.insert(operation.journalPositions.back(), std::move(blocks[index]));
		operation.journalPositions.push_back(position);

		stripe_element[first_index + index] = position->data();
	}
}

/*!
 * Fills given operation with blocks required to be read.
 * \param operation operation to be filled
 * \param first_block first block of the operation
 * \param stripe_element map of blocks in a stripe
 */
void ChunkWriter::fillStripe(Operation &operation, int first_block, std::vector<uint8_t *> &stripe_element) {
	for (const auto &position : operation.journalPositions) {
		assert((position->blockIndex % combinedStripeSize_) == (position->blockIndex - first_block));
		assert(((int)position->blockIndex - first_block) < combinedStripeSize_);
		assert(position->to == operation.journalPositions.front()->to);
		assert(position->from == operation.journalPositions.front()->from);
		stripe_element[position->blockIndex - first_block] = position->data();
	}

	int hole_start = 0;
	int hole_size = 0;
	int range_end = std::min(combinedStripeSize_, MFSBLOCKSINCHUNK - first_block);
	for (int i = 0; i < range_end; ++i) {
		if (stripe_element[i] == nullptr) {
			if (hole_size == 0) {
				hole_start = i;
			}
			hole_size++;
		} else if (hole_size > 0) {
			fillOperation(operation, first_block, hole_start, hole_size, stripe_element);
			hole_size = 0;
		}
	}
	if (hole_size > 0) {
		fillOperation(operation, first_block, hole_start, hole_size, stripe_element);
	}
}

/*!
 * Starts the write operation.
 * Firstly, function checks if any blocks need to be read (which may be the case with xor/ec goal).
 * If so, they are fetched from chunkservers and used for computing parity blocks.
 * Afterwards, data is sent to selected chunkservers.
 * \param operation operation to be started
 */
void ChunkWriter::startOperation(Operation operation) {
	LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::startOperation");
	// If the operation is a partial-stripe write, read all the missing blocks first
	int first_block = combinedStripeSize_ * (operation.journalPositions.front()->blockIndex / combinedStripeSize_);
	int block_size = operation.journalPositions.front()->size();
	int block_from = operation.journalPositions.front()->from;
	int block_to = operation.journalPositions.front()->to;

	std::vector<uint8_t *> stripe_element(combinedStripeSize_, nullptr);
	fillStripe(operation, first_block, stripe_element);

	// Now operation.journalElements is a complete stripe.
	assert(operation.isFullStripe(combinedStripeSize_));

	// Send all the data
	std::vector<WriteCacheBlock *> blocks_to_write;
	std::vector<WriteCacheBlock *> parity_blocks;

	OperationId operationId = allocateId();
	for (auto &fdAndExecutor : executors_) {
		WriteExecutor &executor = *fdAndExecutor.second;
		ChunkPartType chunk_type = executor.chunkType();
		int data_part_count = slice_traits::getNumberOfDataParts(chunk_type);

		blocks_to_write.clear();

		if (slice_traits::isDataPart(chunk_type)) {
			unsigned part_index = slice_traits::getDataPartIndex(chunk_type);

			for (const JournalPosition &position : operation.journalPositions) {
				if ((position->blockIndex % data_part_count) == part_index &&
				    position->type != WriteCacheBlock::kReadBlock) {
					blocks_to_write.push_back(&(*position));
				}
			}
		} else {
			// How many stripes of that type fit to combined stripe size
			int stripe_count = combinedStripeSize_ / data_part_count;

			parity_blocks.clear();
			for (int i = 0; i < stripe_count; ++i) {
				// Check if any data for computing this parity is available
				if (!stripe_element[i * data_part_count]) {
					continue;
				}

				operation.parityBuffers.push_back(
					WriteCacheBlock(locator_->chunkIndex(), 0, WriteCacheBlock::kParityBlock));
				WriteCacheBlock &block = operation.parityBuffers.back();
				parity_blocks.push_back(&block);
				blocks_to_write.push_back(&block);
				block.blockIndex = first_block + i * data_part_count;
				block.from = block_from;
				block.to = block_to;
			}

			for (int i = 0; i < (int)parity_blocks.size(); ++i) {
				computeParityBlock(chunk_type, parity_blocks[i]->data(), stripe_element,
				                   i * data_part_count, block_size);
			}
		}

		for (const WriteCacheBlock *block : blocks_to_write) {
			LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::startOperation::addDataPackets");
			WriteId writeId = allocateId();
			writeIdToOperationId_[writeId] = operationId;
			executor.addDataPacket(writeId, block->blockIndex / data_part_count, block->from,
			                       block->size(), block->data());
			++operation.unfinishedWrites;
		}
	}
	pendingOperations_[operationId] = std::move(operation);
}

/*!
 * Reads blocks from chunkservers into output buffer.
 * \param block_index first block to be read
 * \param size number of blocks to be read
 * \param block_from internal offset of data to be read from block
 * \param block_to internal end of data to be read from block
 * \param blocks output buffer
 */
void ChunkWriter::readBlocks(int block_index, int size, int block_from, int block_to,
		std::vector<WriteCacheBlock> &blocks) {
	LOG_AVG_TILL_END_OF_SCOPE0("ChunkWriter::readBlock");

	ChunkReadPlanner::PartsContainer available_parts;
	ChunkReadPlanner::ScoreContainer best_scores;
	ReadPlanExecutor::ChunkTypeLocations chunk_type_locations;
	ChunkReadPlanner planner;
	std::vector<uint8_t> buffer;

	for (const ChunkTypeWithAddress& chunk_type_with_address : locator_->locationInfo().locations) {
		const ChunkPartType& type = chunk_type_with_address.chunk_type;
		const NetworkAddress& address = chunk_type_with_address.address;

		float score = globalChunkserverStats.getStatisticsFor(address).score();

		if (chunk_type_locations.count(type) == 0) {
			// first location of this type, choose it (for now)
			chunk_type_locations[type] = chunk_type_with_address;
			best_scores[type] = score;
			available_parts.push_back(type);
			// we already know other locations
		} else if (score > best_scores[type]) {
			// this location is better, switch to it
			chunk_type_locations[type] = chunk_type_with_address;
			best_scores[type] = score;
		}
	}
	planner.setScores(std::move(best_scores));
	planner.prepare(block_index, size, available_parts);
	if (!planner.isReadingPossible()) {
		throw RecoverableWriteException("Not enough chunkservers to read full data");
	}

	auto plan = planner.buildPlan();
	if (!read_data_get_prefetchxorstripes()) {
		plan->disable_prefetch = true;
	}

	ReadPlanExecutor executor(globalChunkserverStats,
	locator_->locationInfo().chunkId, locator_->locationInfo().version, std::move(plan));

	assert(buffer.size() == 0);
	executor.executePlan(buffer, chunk_type_locations, connector_,
			read_data_get_connect_timeout_ms(), read_data_get_wave_read_timeout_ms(),
			Timeout{std::chrono::milliseconds(read_data_get_wave_read_timeout_ms())});

	int offset = 0;
	for (int index = block_index; index < block_index + size; ++index) {
		assert(index < MFSBLOCKSINCHUNK);

		WriteCacheBlock block(locator_->chunkIndex(), index, WriteCacheBlock::kReadBlock);
		memcpy(block.data(), buffer.data() + offset, MFSBLOCKSIZE);
		block.from = block_from;
		block.to = block_to;
		blocks.push_back(std::move(block));
		offset += MFSBLOCKSIZE;
	}
}

void ChunkWriter::processStatus(const WriteExecutor& executor,
		const WriteExecutor::Status& status) {
	if (status.chunkId != locator_->locationInfo().chunkId) {
		throw ChunkserverConnectionException(
				"Received inconsistent write status message"
				", expected chunk " + std::to_string(locator_->locationInfo().chunkId) +
				", got chunk " + std::to_string(status.chunkId),
				executor.server());
	}
	if (status.status != LIZARDFS_STATUS_OK) {
		throw RecoverableWriteException("Chunk write error", status.status);
	}

	// Translate writeId to operationId
	OperationId operationId = 0;
	if (status.writeId != 0) {
		try {
			operationId = writeIdToOperationId_.at(status.writeId);
			writeIdToOperationId_.erase(status.writeId);
		} catch (std::out_of_range &e) {
			throw RecoverableWriteException(
				"Chunk write error: unexpected status for operation #" +
				std::to_string(status.writeId));
		}
	} else if (pendingOperations_.count(0) == 0) {
		throw RecoverableWriteException("Chunk write error: unexpected status for WRITE_INIT");
	}

	sassert(pendingOperations_.count(operationId) == 1);
	auto& operation = pendingOperations_[operationId];
	if (--operation.unfinishedWrites == 0) {
		// Operation has just finished: update file size if changed and delete the operation
		if (operationId != 0) {
			// This was a WRITE_DATA operation, not WRITE_INIT
			if (operation.offsetOfEnd > locator_->locationInfo().fileLength) {
				locator_->updateFileLength(operation.offsetOfEnd);
			}
			for (const auto& position : operation.journalPositions) {
				journal_.erase(position);
			}
		}
		pendingOperations_.erase(operationId);
	}
}
