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

#pragma once

#include "common/platform.h"

#include <cstdint>
#include <memory>
#include <mutex>

#include "chunkserver/chunk_file_creator.h"
#include "chunkserver/slice_recovery_planner.h"
#include "common/chunk_connector.h"
#include "common/chunk_type_with_address.h"
#include "common/chunkserver_stats.h"
#include "common/exception.h"

class ChunkReplicator {
public:
	static constexpr unsigned kDefaultTotalTimeout_ms = 60 * 1000;
	static constexpr unsigned kDefaultWaveTimeout_ms = 500;
	static constexpr unsigned kDefaultConnectionTimeout_ms = 1000;

	ChunkReplicator(ChunkConnector& connector);
	void replicate(ChunkFileCreator& fileCreator, const std::vector<ChunkTypeWithAddress>& sources);
	uint32_t getStats();

	void setTotalTimeout(unsigned timeout_ms) {
		total_timeout_ms_ = timeout_ms;
	}

	void setWaveTimeout(unsigned timeout_ms) {
		wave_timeout_ms_ = timeout_ms;
	}

	void setConnectionTimeout(unsigned timeout_ms) {
		connection_timeout_ms_ = timeout_ms;
	}

private:
	ChunkserverStats chunkserverStats_;
	ChunkConnector& connector_;
	uint32_t stats_;
	std::mutex mutex_;

	uint32_t getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
			ChunkTypeWithAddress type_with_address);

	uint32_t getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
			const std::vector<ChunkTypeWithAddress>& sources);

	void incStats();

	unsigned total_timeout_ms_;
	unsigned wave_timeout_ms_;
	unsigned connection_timeout_ms_;
};

extern ChunkReplicator gReplicator;
