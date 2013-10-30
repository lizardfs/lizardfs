#include "mount/chunk_reader.h"

#include <cstring>

#include "mount/exceptions.h"
#include "mount/read_plan_executor.h"

ChunkReader::ChunkReader(
		ChunkConnector& connector, ChunkLocator& locator, ConnectionPool& connectionPool)
	: connectionPool_(connectionPool), connector_(connector), locator_(locator), planner_({}) {
}

void ChunkReader::prepareReadingChunkIfNeeded(uint32_t inode, uint32_t index) {
	if (locator_.inode() != inode || locator_.index() != index) {
		prepareReadingChunk(inode, index);
	}
}

void ChunkReader::prepareReadingChunk(uint32_t inode, uint32_t index) {
	chunkTypeLocations_.clear();
	locator_.locateChunk(inode, index);
	if (locator_.isChunkEmpty()) {
		return;
	}
	std::vector<ChunkType> availableChunkTypes;
	// For nonempty chunks we have to choose some locations, which will be used to read the chunk
	for (const ChunkTypeWithAddress& chunkTypeWithAddress : locator_.locations()) {
		// TODO(msulikowski) use an object wrapping csdb to choose the chunkservers
		// For now use the first location received from master. Master is supposed to send
		// chunk locations in reasonable order.
		if (chunkTypeLocations_.find(chunkTypeWithAddress.chunkType) == chunkTypeLocations_.end()) {
			chunkTypeLocations_[chunkTypeWithAddress.chunkType] = chunkTypeWithAddress.address;
		}
		availableChunkTypes.push_back(chunkTypeWithAddress.chunkType);
	}
	planner_ = ReadOperationPlanner(availableChunkTypes);
	if (!planner_.isReadingPossible()) {
		throw NoValidCopiesReadError("no valid copies");
	}
}

uint32_t ChunkReader::readData(std::vector<uint8_t>& buffer, uint32_t offset, uint32_t size) {
	if (size == 0) {
		return 0;
	}
	sassert(offset + size <= MFSCHUNKSIZE);
	uint64_t offsetInFile = static_cast<uint64_t>(locator_.index()) * MFSCHUNKSIZE + offset;
	uint32_t availableSize = size;	// requested data may lie beyond end of file
	if (offsetInFile >= locator_.fileLength()) {
		// read request entirely beyond EOF, can't read anything
		availableSize = 0;
	} else if (offsetInFile + availableSize > locator_.fileLength()) {
		// read request partially beyond EOF, truncate request to EOF
		availableSize = locator_.fileLength() - offsetInFile;
	}
	if (availableSize == 0) {
		return 0;
	}

	if (locator_.isChunkEmpty()) {
		// We just have to append some zeros to the buffer
		buffer.resize(buffer.size() + availableSize, 0);
	} else {
		// We have to request for availableSize rounded up to MFSBLOCKSIZE
		uint32_t firstBlockToRead = offset / MFSBLOCKSIZE;
		uint32_t blockToReadCount = (availableSize + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE;
		ReadOperationPlanner::Plan plan = planner_.buildPlanFor(firstBlockToRead, blockToReadCount);
		ReadPlanExecutor executor(locator_.chunkId(), locator_.version(), plan);
		uint32_t initialBufferSize = buffer.size();
		executor.executePlan(buffer, chunkTypeLocations_, connectionPool_, connector_);
		// Shrink the buffer. If availableSize is not divisible by MFSBLOCKSIZE
		// we have to chop off the trailing zeros, that we've just read from a chunkserver.
		buffer.resize(initialBufferSize + availableSize);
	}
	return availableSize;
}
