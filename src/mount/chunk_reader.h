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

#pragma once

#include "common/platform.h"

#include <poll.h>
#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "common/chunk_connector.h"
#include "common/chunk_type_with_address.h"
#include "common/connection_pool.h"
#include "common/massert.h"
#include "common/network_address.h"
#include "common/time_utils.h"
#include "mount/chunk_locator.h"
#include "mount/multi_variant_read_planner.h"

class ChunkReader {
public:
	ChunkReader(ChunkConnector& connector);

	/**
	 * Uses a locator to locate the chunk and chooses chunkservers to read from.
	 * Doesn't do anything if the chunk given by (inode, index) is already known to the reader
	 * (ie. the last call to this method had the same inode and index) unless forcePrepare is true.
	 */
	void prepareReadingChunk(uint32_t inode, uint32_t index, bool forcePrepare);

	/**
	 * Reads data from the previously located chunk and appends it to the buffer
	 */
	uint32_t readData(std::vector<uint8_t>& buffer, uint32_t offset, uint32_t size,
			uint32_t connectTimeout_ms, uint32_t basicTimeout_ms,
			const Timeout& communicationTimeout, bool prefetchXorStripes);

	bool isChunkLocated() const {
		return (bool)location_;
	}
	uint32_t inode() const {
		return inode_;
	}
	uint32_t index() const {
		return index_;
	}
	uint64_t chunkId() const {
		return location_->chunkId;
	}
	uint32_t version() const {
		return location_->version;
	}

	/// Counter for the .lizardfds_tweaks file.
	static std::atomic<uint64_t> preparations;

private:
	ChunkConnector& connector_;
	ReadChunkLocator locator_;
	uint32_t inode_;
	uint32_t index_;
	std::shared_ptr<const ChunkLocationInfo> location_;
	MultiVariantReadPlanner planner_;
	std::map<ChunkType, NetworkAddress> chunkTypeLocations_;
	std::vector<ChunkTypeWithAddress> crcErrors_;
	bool chunkAlreadyRead;
};
