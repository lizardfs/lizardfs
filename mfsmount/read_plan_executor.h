#ifndef LIZARDSFS_MFSMOUNT_READ_PLAN_EXECUTOR_H_
#define LIZARDSFS_MFSMOUNT_READ_PLAN_EXECUTOR_H_

#include <map>

#include "mfscommon/chunk_type.h"
#include "mfscommon/connection_pool.h"
#include "mfscommon/MFSCommunication.h"
#include "mfscommon/network_address.h"
#include "mfscommon/packet.h"
#include "mfsmount/chunk_connector.h"
#include "mfsmount/read_operation_planner.h"

class ReadPlanExecutor {
public:
	typedef std::map<ChunkType, NetworkAddress> ChunkTypeLocations;

	ReadPlanExecutor(uint64_t chunkId, uint32_t chunkVersion,
			const ReadOperationPlanner::Plan& plan);

	/*
	 * Executes the plan using given locations, connection pool and connector.
	 * The data will be appended to the buffer.
	 */
	void executePlan(std::vector<uint8_t>& buffer,
			const ChunkTypeLocations& locations,
			ConnectionPool& connectionPool,
			ChunkConnector& connector);

private:
	const uint64_t chunkId_;
	const uint32_t chunkVersion_;
	const ReadOperationPlanner::Plan plan_;

	void executeReadOperations(uint8_t* buffer,
			const ChunkTypeLocations& locations,
			ConnectionPool& connectionPool,
			ChunkConnector& connector);
	void executeXorOperations(uint8_t* buffer);
};

#endif // LIZARDSFS_MFSMOUNT_READ_PLAN_EXECUTOR_H_
