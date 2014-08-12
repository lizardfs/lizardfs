#include "common/platform.h"
#include "mount/chunk_reader.h"

#include <algorithm>
#include <cstring>

#include "common/read_plan_executor.h"
#include "common/time_utils.h"
#include "mount/exceptions.h"
#include "mount/global_chunkserver_stats.h"

ChunkReader::ChunkReader(ChunkConnector& connector)
		: connector_(connector),
		  inode_(0),
		  index_(0) {
}

void ChunkReader::prepareReadingChunk(uint32_t inode, uint32_t index, bool forcePrepare) {
	if (inode != inode_ || index != index_) {
		// we moved to a new chunk
		crcErrors_.clear();
	} else if (!forcePrepare) {
		// we didn't change chunk and aren't forced to prepare again
		return;
	}
	inode_ = inode;
	index_ = index;
	locator_.invalidateCache(inode, index);
	location_ = locator_.locateChunk(inode, index);
	if (location_->isEmptyChunk()) {
		return;
	}
	chunkTypeLocations_.clear();
	std::vector<ChunkType> availableChunkTypes;
	std::map<ChunkType, float> bestScores;

	for (const ChunkTypeWithAddress& chunkTypeWithAddress : location_->locations) {
		const ChunkType& type = chunkTypeWithAddress.chunkType;
		const NetworkAddress& address = chunkTypeWithAddress.address;

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
	planner_.setScores(std::move(bestScores));
	planner_.prepare(availableChunkTypes);
	if (!planner_.isReadingPossible()) {
		throw NoValidCopiesReadException("no valid copies");
	}
}

uint32_t ChunkReader::readData(std::vector<uint8_t>& buffer, uint32_t offset, uint32_t size,
		uint32_t connectTimeout_ms, uint32_t basicTimeout_ms,
		const Timeout& communicationTimeout) {
	if (size == 0) {
		return 0;
	}
	sassert(offset + size <= MFSCHUNKSIZE);
	uint64_t offsetInFile = static_cast<uint64_t>(index_) * MFSCHUNKSIZE + offset;
	uint32_t availableSize = size;  // requested data may lie beyond end of file
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
		ReadPlanExecutor executor(globalChunkserverStats, location_->chunkId, location_->version,
				planner_.buildPlanFor(firstBlockToRead, blockToReadCount));
		uint32_t initialBufferSize = buffer.size();
		try {
			executor.executePlan(buffer, chunkTypeLocations_, connector_,
					ReadPlanExecutor::Timeouts(connectTimeout_ms, basicTimeout_ms),
					communicationTimeout);
			// After executing the plan we want use the feedback to modify our planer a bit
			for (auto partOmited : executor.partsOmitted()) {
				planner_.startAvoidingPart(partOmited);
			}
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
