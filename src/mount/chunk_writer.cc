#include "mount/chunk_writer.h"

#include <poll.h>
#include <algorithm>
#include <bitset>
#include <cstring>

#include "common/block_xor.h"
#include "common/goal.h"
#include "common/massert.h"
#include "common/read_operation_executor.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "mount/exceptions.h"
#include "mount/mastercomm.h"
#include "mount/write_executor.h"

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

ChunkWriter::Operation::Operation(JournalPosition journalPosition)
		: journalPositions{journalPosition},
		  unfinishedWrites(0),
		  offsetOfEnd(journalPosition->offsetInFile() + journalPosition->size()) {
	// Blocks of type WriteCacheBlock::kJournalBlock (ie. these that were read from chunkservers,
	// not written by clients), should not influence the length of the file
	if (journalPosition->type == WriteCacheBlock::kJournalBlock) {
		offsetOfEnd = 0;
	}
}

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

bool ChunkWriter::Operation::expand(JournalPosition newPosition, uint32_t stripeSize) {
	if (!isExpandPossible(newPosition, stripeSize)) {
		return false;
	}

	uint64_t newOffsetOfEnd = newPosition->offsetInFile() + newPosition->size();
	if (newPosition->type != WriteCacheBlock::kJournalBlock && newOffsetOfEnd > offsetOfEnd) {
		offsetOfEnd = newOffsetOfEnd;
	}
	journalPositions.push_back(newPosition);
	return true;
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

ChunkWriter::ChunkWriter(ChunkserverStats& chunkserverStats, ChunkConnector& connector)
	: chunkserverStats_(chunkserverStats),
	  connector_(connector),
	  locator_(nullptr),
	  currentWriteId_(0),
	  combinedStripeSize_(0) {
}

ChunkWriter::~ChunkWriter() {
	try {
		abortOperations();
	} catch (...) {
	}
}

void ChunkWriter::init(WriteChunkLocator* locator, uint32_t msTimeout) {
	sassert(currentWriteId_ == 0);
	sassert(pendingOperations_.empty());
	sassert(executors_.empty());

	Timeout connectTimeout{std::chrono::milliseconds(msTimeout)};
	combinedStripeSize_ = 0;
	locator_ = locator;

	for (const ChunkTypeWithAddress& location : locator_->locationInfo().locations) {
		// If we have an executor writing the same chunkType, use it
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

		// Update combinedStripeSize_
		uint32_t stripeSize = location.chunkType.getStripeSize();
		if (combinedStripeSize_ == 0) {
			combinedStripeSize_ = stripeSize;
		} else {
			combinedStripeSize_ =
					stripeSize * combinedStripeSize_ / gcd(combinedStripeSize_, stripeSize);
		}

		// Create an executor
		int fd = connector_.connect(location.address, connectTimeout);
		std::unique_ptr<WriteExecutor> executor(new WriteExecutor(
				chunkserverStats_, location.address, fd,
				locator_->locationInfo().chunkId, locator_->locationInfo().version,
				location.chunkType));
		executors_.insert(std::make_pair(fd, std::move(executor)));
	}

	// Initialize all the executors
	for (const auto& fdAndExecutor : executors_) {
		fdAndExecutor.second->addInitPacket();
		pendingOperations_[currentWriteId_].unfinishedWrites++;
	}
}

void ChunkWriter::processOperations(uint32_t msTimeout) {
	// Start all possible operations. Break at the first operation that can't be started, because
	// we have to preserve the order of operations in order to ensure the files contain proper data
	for (auto i = newOperations_.begin(); i != newOperations_.end(); i = newOperations_.erase(i)) {
		Operation& operation = *i;
		// Don't start partial-stripe writes if they can be extended in the future (only the last
		// one can) and we have anything else to do
		if (i == std::prev(newOperations_.end())
				&& pendingOperations_.size() > 0
				&& !operation.isFullStripe(combinedStripeSize_)) {
			break;
		}
		if (!canStartOperation(operation)) {
			break;
		}
		startOperation(std::move(operation));
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
}

void ChunkWriter::abortOperations() {
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
	sassert(block.chunkIndex == locator_->chunkIndex());
	if (block.type == WriteCacheBlock::kWritableBlock) {
		// Block is writeable until the first try of writing it to chunkservers, ie. until now
		block.type = WriteCacheBlock::kReadOnlyBlock;
	}
	journal_.push_back(std::move(block));
	JournalPosition journalPosition = std::prev(journal_.end());
	if (newOperations_.empty()
			|| !newOperations_.back().isExpandPossible(journalPosition, combinedStripeSize_)) {
		newOperations_.push_back(Operation(journalPosition));
	} else {
		newOperations_.back().expand(journalPosition, combinedStripeSize_);
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

void ChunkWriter::startOperation(Operation&& operation) {
	uint32_t combinedStripe = operation.journalPositions.front()->blockIndex / combinedStripeSize_;
	uint32_t size = operation.journalPositions.front()->size();

	// If the operation is a partial-stripe write, read all the missing blocks first
	std::vector<bool> stripeElementsPresent(combinedStripeSize_);
	for (const auto& position : operation.journalPositions) {
		stripeElementsPresent[position->blockIndex % combinedStripeSize_] = true;
	}
	for (uint32_t indexInStripe = 0; indexInStripe < combinedStripeSize_; ++indexInStripe) {
		if (stripeElementsPresent[indexInStripe]) {
			continue;
		}
		uint32_t blockIndex = combinedStripe * combinedStripeSize_ + indexInStripe;
		if (blockIndex >= MFSBLOCKSINCHUNK) {
			break;
		}
		WriteCacheBlock newBlock = readBlock(blockIndex);
		newBlock.from = operation.journalPositions.front()->from;
		newBlock.to = operation.journalPositions.front()->to;
		// Insert the new block into the journal just after the last block of the operation
		auto position = journal_.insert(operation.journalPositions.back(), std::move(newBlock));
		operation.journalPositions.push_back(position);
	}

	// Now operation.journalElements is a complete stripe.
	sassert(operation.isFullStripe(combinedStripeSize_));

	// Send all the data
	WriteId id = ++currentWriteId_;
	for (auto& fdAndExecutor : executors_) {
		WriteExecutor& executor = *fdAndExecutor.second;
		ChunkType chunkType = executor.chunkType();
		uint32_t stripeSize = chunkType.getStripeSize();
		sassert(combinedStripeSize_ % stripeSize == 0);
		std::vector<WriteCacheBlock*> blocksToWrite;

		if (chunkType.isStandardChunkType()) {
			for (const JournalPosition& position : operation.journalPositions) {
				uint32_t indexCombinedInStripe = position->blockIndex % combinedStripeSize_;
				if (!stripeElementsPresent[indexCombinedInStripe]) {
					// Don't write the data, which has just been read
					continue;
				}
				blocksToWrite.push_back(&(*position));
			}
		} else if (chunkType.isXorChunkType() && chunkType.isXorParity()) {
			uint32_t substripeCount = combinedStripeSize_ / stripeSize;
			std::vector<WriteCacheBlock*> parityBlocks;
			for (uint32_t i = 0; i < substripeCount; ++i) {
				// Block index will be added later
				operation.parityBuffers.push_back(WriteCacheBlock(
						locator_->chunkIndex(), 0, WriteCacheBlock::kJournalBlock));
				parityBlocks.push_back(&operation.parityBuffers.back());
				blocksToWrite.push_back(&operation.parityBuffers.back());
			}
			for (const JournalPosition& position : operation.journalPositions) {
				sassert(position->size() == size);
				uint32_t currentParityBlock =
						(position->blockIndex - combinedStripe * combinedStripeSize_) / stripeSize;
				if (parityBlocks[currentParityBlock]->size() == 0) {
					// We need a block index in ordinary chunk
					//  - it'll be converted to a block index in xor parity later
					parityBlocks[currentParityBlock]->blockIndex = position->blockIndex;
					bool expanded = parityBlocks[currentParityBlock]->expand(
							position->from, position->to, position->data());
					sassert(expanded);
				} else {
					blockXor(parityBlocks[currentParityBlock]->data(), position->data(), size);
				}
			}
		} else {
			for (const JournalPosition& position : operation.journalPositions) {
				uint32_t indexInCombinedStripe = position->blockIndex % combinedStripeSize_;
				if (!stripeElementsPresent[indexInCombinedStripe]) {
					// Don't write the data, which has just been read
					continue;
				}

				if ((position->blockIndex % stripeSize) + 1 == chunkType.getXorPart()) {
					blocksToWrite.push_back(&(*position));
				}
			}
		}

		for (const WriteCacheBlock* block : blocksToWrite) {
			executor.addDataPacket(id,
					block->blockIndex / stripeSize, block->from, block->size(), block->data());
			++operation.unfinishedWrites;
		}
	}
	pendingOperations_[id] = std::move(operation);
}

WriteCacheBlock ChunkWriter::readBlock(uint32_t blockIndex) {
	Timeout timeout{std::chrono::seconds(1)};

	// Find a server from which we will be able to read the block
	NetworkAddress sourceServer;
	ChunkType sourceChunkType = ChunkType::getStandardChunkType();
	for (const auto& fdAndExecutor : executors_) {
		const auto& executor = *fdAndExecutor.second;
		ChunkType chunkType = executor.chunkType();
		if (chunkType.isStandardChunkType()) {
			sourceServer = executor.server();
			sourceChunkType = chunkType;
			break;
		} else {
			sassert(chunkType.isXorChunkType());
			if (chunkType.isXorParity()) {
				continue;
			}
			if (blockIndex % chunkType.getXorLevel() + 1 == chunkType.getXorPart()) {
				sourceServer = executor.server();
				sourceChunkType = chunkType;
				break;
			}
		}
	}
	if (sourceServer == NetworkAddress()) {
		throw RecoverableWriteException("No server to read block " + std::to_string(blockIndex));
	}

	// Prepare the read operation
	uint32_t stripe = blockIndex;
	if (sourceChunkType.isXorChunkType()) {
		stripe /= sourceChunkType.getXorLevel();
	}
	ReadPlanner::ReadOperation readOperation;
	readOperation.requestOffset = stripe * MFSBLOCKSIZE;
	readOperation.requestSize = MFSBLOCKSIZE;
	readOperation.readDataOffsets.push_back(0);

	// Connect to the chunkserver and execute the read operation
	int fd = connector_.connect(sourceServer, timeout);
	try {
		WriteCacheBlock block(locator_->chunkIndex(), blockIndex, WriteCacheBlock::kJournalBlock);
		block.from = 0;
		block.to = MFSBLOCKSIZE;
		ReadOperationExecutor readExecutor(readOperation,
				locator_->locationInfo().chunkId, locator_->locationInfo().version,
				sourceChunkType, sourceServer, fd, block.data());
		readExecutor.sendReadRequest(timeout);
		readExecutor.readAll(timeout);
		connector_.returnToPool(fd, sourceServer);
		return block;
	} catch (...) {
		tcpclose(fd);
		throw;
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
			if (operation.offsetOfEnd > locator_->locationInfo().fileLength) {
				locator_->updateFileLength(operation.offsetOfEnd);
			}
			for (const auto& position : operation.journalPositions) {
				journal_.erase(position);
			}
		}
		pendingOperations_.erase(status.writeId);
	}
}
