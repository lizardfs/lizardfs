#include "mount/chunk_reader.h"

#include <algorithm>
#include <cstring>

#include "common/time_utils.h"
#include "mount/chunkserver_stats.h"
#include "mount/exceptions.h"
#include "mount/read_plan_executor.h"

ChunkReader::ChunkReader(ChunkConnector& connector, ChunkLocator& locator)
	: connector_(connector), locator_(locator),
	  planner_({}, std::map<ChunkType, float>()) {
}

void ChunkReader::prepareReadingChunk(uint32_t inode, uint32_t index) {
	if (locator_.inode() != inode || locator_.index() != index) {
		crcErrors_.clear();
	}
	locator_.locateChunk(inode, index);
	if (locator_.isChunkEmpty()) {
		return;
	}
	chunkTypeLocations_.clear();
	std::vector<ChunkType> availableChunkTypes;
	std::map<ChunkType, float> bestScores;

	for (const ChunkTypeWithAddress& chunkTypeWithAddress : locator_.locations()) {
		const ChunkType &type = chunkTypeWithAddress.chunkType;
		const NetworkAddress &address = chunkTypeWithAddress.address;

		if (std::count(crcErrors_.begin(), crcErrors_.end(), chunkTypeWithAddress) > 0) {
			continue;
		}

		float score = globalChunkserverStats.getStatisticsFor(address).score();

		if (chunkTypeLocations_.count(type) == 0) {
			// first location of this type, choose it (for now)
			chunkTypeLocations_[type] = address;
			bestScores[type] = score;
			availableChunkTypes.push_back(type);
		} else {
			// we already know other locations
			if (score > bestScores[type]) {
				// this location is better, switch to it
				chunkTypeLocations_[type] = address;
				bestScores[type] = score;
			}
		}
	}
	planner_ = ReadOperationPlanner(availableChunkTypes, bestScores);
	if (!planner_.isReadingPossible()) {
		throw NoValidCopiesReadError("no valid copies");
	}
}

uint32_t ChunkReader::readData(std::vector<uint8_t>& buffer, uint32_t offset, uint32_t size,
		const Timeout& communicationTimeout) {
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
		try {
			executor.executePlan(buffer, chunkTypeLocations_, connector_, communicationTimeout);
		} catch (ChunkCrcError &err) {
			crcErrors_.push_back(ChunkTypeWithAddress(err.server(), err.chunkType()));
			throw;
		}
		// Shrink the buffer. If availableSize is not divisible by MFSBLOCKSIZE
		// we have to chop off the trailing zeros, that we've just read from a chunkserver.
		buffer.resize(initialBufferSize + availableSize);
	}
	return availableSize;
}
