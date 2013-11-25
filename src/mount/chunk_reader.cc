#include "mount/chunk_reader.h"

#include <algorithm>
#include <cstring>

#include "common/time_utils.h"
#include "mount/chunkserver_stats.h"
#include "mount/exceptions.h"
#include "mount/read_plan_executor.h"

ChunkReader::ChunkReader(ChunkConnector& connector, ReadChunkLocator& locator)
	: connector_(connector), locator_(locator), inode_(0), index_(0),
	  planner_({}, std::map<ChunkType, float>()) {
}

void ChunkReader::prepareReadingChunk(uint32_t inode, uint32_t index) {
	if (inode != inode_ || index != index_) {
		// we moved to a new chunk
		crcErrors_.clear();
	} else {
		// we are called due to an error
		locator_.invalidateCache(inode, index);
	}
	inode_ = inode;
	index_ = index;
	location_ = locator_.locateChunk(inode, index);
	if (location_->isEmptyChunk()) {
		return;
	}
	chunkTypeLocations_.clear();
	std::vector<ChunkType> availableChunkTypes;
	std::map<ChunkType, float> bestScores;

	for (const ChunkTypeWithAddress& chunkTypeWithAddress : location_->locations) {
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
		throw NoValidCopiesReadException("no valid copies");
	}
}

uint32_t ChunkReader::readData(std::vector<uint8_t>& buffer, uint32_t offset, uint32_t size,
		const Timeout& communicationTimeout) {
	if (size == 0) {
		return 0;
	}
	sassert(offset + size <= MFSCHUNKSIZE);
	uint64_t offsetInFile = static_cast<uint64_t>(index_) * MFSCHUNKSIZE + offset;
	uint32_t availableSize = size;	// requested data may lie beyond end of file
	if (offsetInFile >= location_->fileLength) {
		// read request entirely beyond EOF, can't read anything
		availableSize = 0;
	} else if (offsetInFile + availableSize > location_->fileLength) {
		// read request partially beyond EOF, truncate request to EOF
		availableSize = location_->fileLength - offsetInFile;
	}
	if (availableSize == 0) {
		return 0;
	}

	if (location_->isEmptyChunk()) {
		// We just have to append some zeros to the buffer
		buffer.resize(buffer.size() + availableSize, 0);
	} else {
		// We have to request for availableSize rounded up to MFSBLOCKSIZE
		uint32_t firstBlockToRead = offset / MFSBLOCKSIZE;
		uint32_t blockToReadCount = (availableSize + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE;
		ReadOperationPlanner::Plan plan = planner_.buildPlanFor(firstBlockToRead, blockToReadCount);
		ReadPlanExecutor executor(location_->chunkId, location_->version, plan);
		uint32_t initialBufferSize = buffer.size();
		try {
			executor.executePlan(buffer, chunkTypeLocations_, connector_, communicationTimeout);
		} catch (ChunkCrcException &err) {
			crcErrors_.push_back(ChunkTypeWithAddress(err.server(), err.chunkType()));
			throw;
		}
		// Shrink the buffer. If availableSize is not divisible by MFSBLOCKSIZE
		// we have to chop off the trailing zeros, that we've just read from a chunkserver.
		buffer.resize(initialBufferSize + availableSize);
	}
	return availableSize;
}
