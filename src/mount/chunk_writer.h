#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <vector>

#include "common/chunk_type_with_address.h"
#include "mount/chunk_locator.h"
#include "mount/write_cache_block.h"
#include "mount/write_executor.h"

class ChunkserverStats;
class ChunkConnector;

class ChunkWriter {
public:
	ChunkWriter(ChunkserverStats& stats, ChunkConnector& connector);
	ChunkWriter(const ChunkWriter&) = delete;
	~ChunkWriter();
	ChunkWriter& operator=(const ChunkWriter&) = delete;

	/*
	 * Creates connection chain between client and all chunkservers
	 * This method will throw an exception if all the connections can't be established
	 * within the given timeout.
	 */
	void init(uint32_t inode, uint32_t index, uint32_t msTimeout);

	/*
	 * Returns information about the location of the currently written chunk
	 */
	const ChunkLocationInfo& chunkLocationInfo() const;

	/*
	 * Adds a new pending write operation.
	 */
	void addOperation(WriteCacheBlock&& block);

	/*
	 * Processes all pending operations for at most specified time (0 - asap)
	 */
	void processOperations(uint32_t msTimeout);

	/*
	 * Returns number of pending write operations.
	 */
	uint32_t getUnfinishedOperationsCount();

	/*
	 * Closes connection chain, releases all the acquired locks.
	 * This method can be called when all the write operations have been finished.
	 */
	void finish(uint32_t msTimeout);

	/*
	 * Immediately closes write operations and connection chains,
	 * releases all the acquired  locks.
	 */
	void abortOperations();

	/*
	 * Removes writer's journal and returns it
	 */
	std::list<WriteCacheBlock> releaseJournal();

private:
	typedef uint32_t WriteId;
	typedef std::list<WriteCacheBlock>::iterator JournalPosition;

	struct Operation {
		JournalPosition journalPosition;  // position in the journal being written in this operation
		std::list<std::vector<uint8_t>> parityBuffers; // memory allocated for parity blocks
		uint32_t unfinishedWrites;                     // number of write request sent
		uint64_t offsetOfEnd;                          // offset in the file

		Operation() : unfinishedWrites(0), offsetOfEnd(0) {}
		Operation(Operation&&) = default;
		Operation(const Operation&) = delete;
		Operation& operator=(const Operation&) = delete;
		Operation& operator=(Operation&&) = default;
	};

	ChunkserverStats& chunkserverStats_;
	ChunkConnector& connector_;
	WriteChunkLocator locator_;
	uint32_t inode_;
	uint32_t index_;
	WriteId currentWriteId_;
	std::map<int, std::unique_ptr<WriteExecutor>> executors_;

	std::list<WriteCacheBlock> journal_;
	std::list<Operation> newOperations_;
	std::map<WriteId, Operation> pendingOperations_;
	std::map<ChunkType, std::map<uint32_t, std::vector<uint8_t>>> buffers_;

	void releaseChunk();
	void startOperation(Operation&& operation);
	void processStatus(const WriteExecutor& executor, const WriteExecutor::Status& status);
};
