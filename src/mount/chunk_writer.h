#ifndef LIZARDFS_MOUNT_CHUNK_WRITER_H_
#define LIZARDFS_MOUNT_CHUNK_WRITER_H_

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <vector>

#include "common/chunk_type_with_address.h"
#include "mount/chunk_locator.h"

class ChunkserverStats;
class ChunkConnector;
class WriteExecutor;

class ChunkWriter {
public:
	typedef uint32_t WriteId;

	ChunkWriter(ChunkserverStats& stats, ChunkConnector& connector);
	ChunkWriter(const ChunkWriter&) = delete;
	ChunkWriter& operator=(const ChunkWriter&) = delete;
	~ChunkWriter();

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
	WriteId addOperation(const uint8_t* data, uint32_t offset, uint32_t size);

	/*
	 * Processes all pending operations for at most specified time (0 - asap)
	 * Returns ID's of write operations, which have been completed in this call.
	 */
	std::vector<WriteId> processOperations(uint32_t msTimeout);

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
	void abort();

private:
	ChunkserverStats& chunkserverStats_;
	ChunkConnector& connector_;
	WriteChunkLocator locator_;
	uint32_t inode_;
	uint32_t index_;
	WriteId currentWriteId_;
	std::map<int, std::unique_ptr<WriteExecutor>> executors_;

	// TODO(msulikowski) data from buffers_ and paritiesBeingSent_ should be removed when not needed
	std::map<ChunkType, std::map<uint32_t, std::vector<uint8_t>>> buffers_;
	std::list<std::vector<uint8_t>> paritiesBeingSent_;
	std::map<WriteId, uint32_t> unfinishedOperationCounters_;
	std::map<WriteId, uint64_t> offsetOfEnd_;

	void releaseChunk();
};

#endif //LIZARDFS_MOUNT_CHUNK_WRITER_H_
