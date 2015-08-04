/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
		  index_(0),
		  chunkAlreadyRead(false) {
}

void ChunkReader::prepareReadingChunk(uint32_t inode, uint32_t index, bool forcePrepare) {
	if (inode != inode_ || index != index_) {
		// we moved to a new chunk
		crcErrors_.clear();
	} else if (!forcePrepare) {
		// we didn't change chunk and aren't forced to prepare again
		return;
	}
	++preparations;
	inode_ = inode;
	index_ = index;
	locator_.invalidateCache(inode, index);
	location_ = locator_.locateChunk(inode, index);
	chunkAlreadyRead = false;
	if (location_->isEmptyChunk()) {
		return;
	}
	chunkTypeLocations_.clear();
	std::vector<ChunkPartType> availableChunkTypes;
	std::map<ChunkPartType, float> bestScores;

	for (const ChunkTypeWithAddress& chunkTypeWithAddress : location_->locations) {
		const ChunkPartType& type = chunkTypeWithAddress.chunkType;
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
		uint32_t connectTimeout_ms, uint32_t basicTimeout_ms, const Timeout& communicationTimeout,
		bool prefetchXorStripes) {
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
		auto plan = planner_.buildPlanFor(firstBlockToRead, blockToReadCount);
		if (!prefetchXorStripes || chunkAlreadyRead || size != availableSize) {
			// Disable prefetching if:
			// - it was disabled with a config option
			// - all chunk parts were read before (in this case we rely on pagecache)
			// - we're reading the end of a chunk (there is no point in prefetching anything)
			plan->prefetchOperations.clear();
		}
		ReadPlanExecutor executor(globalChunkserverStats, location_->chunkId, location_->version,
				std::move(plan));
		uint32_t initialBufferSize = buffer.size();
		try {
			chunkAlreadyRead = true;
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

std::atomic<uint64_t> ChunkReader::preparations;
