#pragma once

#include "common/platform.h"

#include <map>

#include "common/chunk_connector.h"
#include "common/chunk_type.h"
#include "common/chunkserver_stats.h"
#include "common/connection_pool.h"
#include "common/MFSCommunication.h"
#include "common/network_address.h"
#include "common/packet.h"
#include "common/read_planner.h"
#include "common/time_utils.h"

class ReadPlanExecutor {
public:
	typedef std::map<ChunkType, NetworkAddress> ChunkTypeLocations;

	ReadPlanExecutor(
			ChunkserverStats& chunkserverStats,
			uint64_t chunkId, uint32_t chunkVersion,
			std::unique_ptr<ReadPlanner::Plan> plan);

	/*
	 * Executes the plan using given locations, connection pool and connector.
	 * The data will be appended to the buffer.
	 */
	void executePlan(std::vector<uint8_t>& buffer,
			const ChunkTypeLocations& locations,
			ChunkConnector& connector, const Timeout& communicationTimeout);

private:
	ChunkserverStats& chunkserverStats_;
	const uint64_t chunkId_;
	const uint32_t chunkVersion_;
	std::unique_ptr<const ReadPlanner::Plan> plan_;

	void executeReadOperations(uint8_t* buffer,
			const ChunkTypeLocations& locations,
			ChunkConnector& connector, const Timeout& communicationTimeout);
	void executeXorOperations(uint8_t* buffer);
};
