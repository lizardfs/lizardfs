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

#pragma once

#include "common/platform.h"

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <vector>

#include "common/chunk_type_with_address.h"
#include "common/write_executor.h"
#include "mount/chunk_locator.h"
#include "mount/write_cache_block.h"

class ChunkserverStats;
class ChunkConnector;

class ChunkWriter {
public:
	/**
	 * Constructor
	 *
	 * \param stats - database which will be updated by the object when accessing servers
	 * \param connector - object that will be used to create connection with chunkservers
	 *        to write to them and read from them
	 * \param dataChainFd - end of pipe; if anything is written to it, ChunkWriter will break its
	 *        poll call and look for some new data in write cache for the currently written chunk
	 */
	ChunkWriter(ChunkserverStats& stats, ChunkConnector& connector, int dataChainFd);
	ChunkWriter(const ChunkWriter&) = delete;
	~ChunkWriter();
	ChunkWriter& operator=(const ChunkWriter&) = delete;

	/**
	 * Creates connection chain between client and all chunkservers.
	 * This method will throw an exception if all the connections can't be established
	 * within the given timeout.
	 *
	 * \param locator - an object which will be used during the write process that we initialize
	 *        to ask the master server about chunkservers that need to be written to
	 * \param chunkserverTimeout_ms - a timeout which will be used be used during the write process
	 *        that we initialize; it represents the maximum time that can elapse when we are
	 *        waiting for each chunkserver to accept connection or send a status message
	 */
	void init(WriteChunkLocator* locator, uint32_t chunkserverTimeout_ms);

	/*!
	 * \return minimum number of blocks which will be written to chunkservers by
	 * the ChunkWriter if the flush mode is off.
	 */
	uint32_t getMinimumBlockCountWorthWriting();

	/*!
	 * Adds a new write operation.
	 */
	void addOperation(WriteCacheBlock&& block);

	/*!
	 * Starts these added operations, which are worth starting right now.
	 * Returns number of operations started.
	 */
	uint32_t startNewOperations(bool can_expect_next_block);

	/*!
	 * Processes all started operations for at most specified time (0 - asap)
	 */
	void processOperations(uint32_t msTimeout);

	/*!
	 * \return number of new and pending write operations.
	 */
	uint32_t getUnfinishedOperationsCount();

	/*!
	 * \return number of pending write operations.
	 */
	uint32_t getPendingOperationsCount();

	/*!
	 * Tells ChunkWriter that no more operations will be added and it can flush all the data
	 * to chunkservers. No new operations can be started after calling this method.
	 */
	void startFlushMode();

	/*!
	 * Tells ChunkWriter, that it may not start any operation, that did't start yet, because writing
	 * this chunk will be finished later and any then data left in the journal will be written.
	 * No new operations can be started after calling this method.
	 */
	void dropNewOperations();

	/*!
	 * Closes connection chain, releases all the acquired locks.
	 * This method can be called when all the write operations have been finished.
	 */
	void finish(uint32_t msTimeout);

	/*!
	 * Immediately closes write operations and connection chains.
	 */
	void abortOperations();

	/*!
	 * Removes writer's journal and returns it
	 */
	std::list<WriteCacheBlock> releaseJournal();

	bool acceptsNewOperations() { return acceptsNewOperations_; }

private:
	typedef uint32_t WriteId;
	typedef uint32_t OperationId;
	typedef std::list<WriteCacheBlock>::iterator JournalPosition;

	class Operation {
	public:
		std::vector<JournalPosition> journalPositions;  // stripe in the written journal
		std::list<WriteCacheBlock> parityBuffers;       // memory for parity blocks
		uint32_t unfinishedWrites;                      // number of write request sent
		uint64_t offsetOfEnd;                           // offset in the file

		Operation();
		Operation(Operation&&) = default;
		Operation(const Operation&) = delete;
		Operation& operator=(const Operation&) = delete;
		Operation& operator=(Operation&&) = default;

		/**
		 * Checks if expansion can be performed for given stripe
		 */
		bool isExpandPossible(ChunkWriter::JournalPosition newPosition, uint32_t stripeSize);
		/*
		 * Tries to add a new journal position to an existing operation.
		 * Returns true if succeeded, false if the operation wasn't modified.
		 */
		void expand(JournalPosition journalPosition);

		/*
		 * Returns true if two operations write the same place in a file.
		 * One of these operations have to be complete, ie. contain a full stripe
		 */
		bool collidesWith(const Operation& operation) const;

		/*
		 * Returns true if the operation is not a partial-stripe write operation
		 * for a given stripe size
		 */
		bool isFullStripe(uint32_t stripeSize) const;
	};

	ChunkserverStats& chunkserverStats_;
	ChunkConnector& connector_;
	WriteChunkLocator* locator_;
	uint32_t idCounter_;
	bool acceptsNewOperations_;
	int combinedStripeSize_;
	int dataChainFd_;

	std::map<int, std::unique_ptr<WriteExecutor>> executors_;
	std::list<WriteCacheBlock> journal_;
	std::list<Operation> newOperations_;
	std::map<WriteId, OperationId> writeIdToOperationId_;
	std::map<OperationId, Operation> pendingOperations_;

	bool canStartOperation(const Operation& operation);
	void startOperation(Operation operation);
	void fillOperation(Operation &operation, int first_block, int first_index, int size,
			std::vector<uint8_t *> &stripe_element);
	void fillStripe(Operation &operation, int first_block, std::vector<uint8_t *> &stripe_element);
	void readBlocks(int block_index, int size, int block_from, int block_to,
			std::vector<WriteCacheBlock> &blocks);
	void computeParityBlock(const ChunkPartType &chunk_type, uint8_t *parity_block,
			const std::vector<uint8_t *> &data_blocks, int offset, int size);

	void processStatus(const WriteExecutor& executor, const WriteExecutor::Status& status);
	uint32_t allocateId() {
		// we never return id=0 because it's reserved for WRITE_INIT
		idCounter_++;
		return idCounter_;
	}
};
