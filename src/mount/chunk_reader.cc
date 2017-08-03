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

#include "common/platform.h"
#include "mount/chunk_reader.h"

#include <algorithm>
#include <cstring>

#include "common/exceptions.h"
#include "common/read_plan_executor.h"
#include "common/time_utils.h"
#include "mount/global_chunkserver_stats.h"

ChunkReader::ChunkReader(ChunkConnector& connector, double bandwidth_overuse)
		: connector_(connector),
		  inode_(0),
		  index_(0),
		  planner_(bandwidth_overuse),
		  chunkAlreadyRead(false) {
}

void ChunkReader::prepareReadingChunk(uint32_t inode, uint32_t index, bool force_prepare) {
	if (inode != inode_ || index != index_) {
		// we moved to a new chunk
		crcErrors_.clear();
	} else if (!force_prepare) {
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
	chunk_type_locations_.clear();

	ChunkReadPlanner::ScoreContainer best_scores;

	available_parts_.clear();
	for (const ChunkTypeWithAddress& chunk_type_with_address : location_->locations) {
		const ChunkPartType& type = chunk_type_with_address.chunk_type;

		if (std::count(crcErrors_.begin(), crcErrors_.end(), chunk_type_with_address) > 0) {
			continue;
		}

		float score = globalChunkserverStats.getStatisticsFor(chunk_type_with_address.address).score();
		if (chunk_type_locations_.count(type) == 0) {
			// first location of this type, choose it (for now)
			chunk_type_locations_[type] = chunk_type_with_address;
			best_scores[type] = score;
			available_parts_.push_back(type);
		} else {
			// we already know other locations
			if (score > best_scores[type]) {
				// this location is better, switch to it
				chunk_type_locations_[type] = chunk_type_with_address;
				best_scores[type] = score;
			}
		}
	}
	planner_.setScores(std::move(best_scores));
}

uint32_t ChunkReader::readData(std::vector<uint8_t>& buffer, uint32_t offset, uint32_t size,
		uint32_t connectTimeout_ms, uint32_t wave_timeout_ms, const Timeout& communicationTimeout,
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

		planner_.prepare(firstBlockToRead, blockToReadCount, available_parts_);
		if (!planner_.isReadingPossible()) {
			throw NoValidCopiesReadException("no valid copies");
		}

		auto plan = planner_.buildPlan();
		if (!prefetchXorStripes || chunkAlreadyRead || size != availableSize) {
			// Disable prefetching if:
			// - it was disabled with a config option
			// - all chunk parts were read before (in this case we rely on pagecache)
			// - we're reading the end of a chunk (there is no point in prefetching anything)
			plan->disable_prefetch = true;
		}
		ReadPlanExecutor executor(globalChunkserverStats, location_->chunkId, location_->version,
				std::move(plan));
		uint32_t initialBufferSize = buffer.size();
		try {
			chunkAlreadyRead = true;
			executor.executePlan(buffer, chunk_type_locations_, connector_,
					connectTimeout_ms, wave_timeout_ms,
					communicationTimeout);
			//TODO(haze): Improve scoring system so it can deal with disconnected chunkservers.
		} catch (ChunkCrcException &err) {
			crcErrors_.push_back(ChunkTypeWithAddress(err.server(), err.chunkType(), 0));
			throw;
		}
		// Shrink the buffer. If availableSize is not divisible by MFSBLOCKSIZE
		// we have to chop off the trailing zeros, that we've just read from a chunkserver.
		buffer.resize(initialBufferSize + availableSize);
	}
	return availableSize;
}

std::atomic<uint64_t> ChunkReader::preparations;
