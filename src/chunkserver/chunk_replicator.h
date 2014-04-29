#pragma once

#include "config.h"

#include <cstdint>
#include <memory>
#include <mutex>

#include "chunkserver/chunk_file_creator.h"
#include "common/chunk_connector.h"
#include "common/chunk_type_with_address.h"
#include "common/chunkserver_stats.h"
#include "common/exception.h"
#include "common/standard_chunk_read_planner.h"
#include "common/xor_chunk_read_planner.h"

class ChunkReplicator {
public:
	ChunkReplicator(ChunkConnector& connector);
	void replicate(ChunkFileCreator& fileCreator, const std::vector<ChunkTypeWithAddress>& sources);
	uint32_t getStats();

private:
	ChunkserverStats chunkserverStats_;
	ChunkConnector& connector_;
	uint32_t stats_;
	std::mutex mutex_;

	std::unique_ptr<ReadPlanner> getPlanner(ChunkType chunkType,
			const std::vector<ChunkTypeWithAddress>& sources);

	uint32_t getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
			ChunkType chunkType, NetworkAddress server) throw (Exception);

	uint32_t getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
			const std::vector<ChunkTypeWithAddress>& sources);
};

extern ChunkReplicator gReplicator;
