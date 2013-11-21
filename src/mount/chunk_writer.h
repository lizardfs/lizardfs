#ifndef LIZARDFS_MOUNT_CHUNK_WRITER_H_
#define LIZARDFS_MOUNT_CHUNK_WRITER_H_

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <vector>

#include "common/chunk_type_with_address.h"
#include "common/exceptions.h"

class ChunkserverStats;
class ChunkConnector;
class WriteExecutor;

class ChunkWriter {
public:
	typedef uint32_t WriteId;

	ChunkWriter(ChunkserverStats& stats, ChunkConnector& connector,
			uint64_t chunkId, uint32_t chunkVersion);
	ChunkWriter(const ChunkWriter&) = delete;
	ChunkWriter& operator=(const ChunkWriter&) = delete;
	~ChunkWriter();

	/*
	 * Creates connection chain between client and all chunkservers
	 * This method will throw an exception if all the connections can't be established
	 * within the given timeout.
	 */
	void init(const std::vector<ChunkTypeWithAddress>& chunkLocations,
			uint32_t msTimeout);

	/*
	 * Processes all pending operations for at most specified time (0 - asap)
	 * Returns ID's of write operations, which have been completed in this call.
	 */
	std::vector<WriteId> processOperations(uint32_t msTimeout);

	/*
	 * Closes connection chain
	 * This method can be called when all the write operations have been finished.
	 */
	void finish(uint32_t msTimeout);

	/*
	 * Adds a new pending write operation.
	 */
	WriteId addOperation(const uint8_t* data, uint32_t offset, uint32_t size);

	/*
	 * Returns number of pending write operations.
	 */
	uint32_t getUnfinishedOperationsCount();

private:
	ChunkConnector& connector_;
	ChunkserverStats& chunkserverStats_;
	const uint64_t chunkId_;
	const uint32_t chunkVersion_;
	WriteId currentWriteId_;
	std::map<int, std::unique_ptr<WriteExecutor>> executors_;

	// TODO(msulikowski) data from buffers_ and paritiesBeingSent_ should be removed when not needed
	std::map<ChunkType, std::map<uint32_t, std::vector<uint8_t>>> buffers_;
	std::list<std::vector<uint8_t>> paritiesBeingSent_;
	std::map<WriteId, uint32_t> unfinishedOperationCounters_;

	/*
	 * Immediately closes write operations and connection chains.
	 */
	void abort();
};

#endif //LIZARDFS_MOUNT_CHUNK_WRITER_H_
