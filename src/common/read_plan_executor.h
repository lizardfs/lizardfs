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

#include <atomic>
#include <map>

#include "common/chunk_connector.h"
#include "common/chunk_type.h"
#include "common/chunkserver_stats.h"
#include "common/connection_pool.h"
#include "protocol/MFSCommunication.h"
#include "common/network_address.h"
#include "protocol/packet.h"
#include "common/read_planner.h"
#include "common/time_utils.h"

class ReadPlanExecutor {
public:
	typedef std::map<ChunkType, NetworkAddress> ChunkTypeLocations;

	/// Timeouts for ReadPlanExecutor::executePlan.
	struct Timeouts {
		/// Timeout for creating TCP connections and rending requests.
		uint32_t connectTimeout_ms;

		/// Timeout after which additional read operations will be started.
		uint32_t basicTimeout_ms;

		Timeouts(uint32_t connectTimeout_ms, uint32_t basicTimeout_ms)
				: connectTimeout_ms(connectTimeout_ms),
				  basicTimeout_ms(basicTimeout_ms) {
		}
	};

	ReadPlanExecutor(
			ChunkserverStats& chunkserverStats,
			uint64_t chunkId, uint32_t chunkVersion,
			std::unique_ptr<ReadPlan> plan);

	/**
	 * Executes the plan.
	 * The data will be appended to the buffer.
	 * \param buffer       buffer, where the data will be stored (>= plan_.requiredBufferSize)
	 * \param locations    locations of all the chunkTypes that exist in the plan
	 * \param connector    object which will be used to obtain connections to chunkservers
	 * \param timeouts     settings of timeouts which influence how the executor works
	 * \param totalTimeout timeout of the whole operation
	 */
	void executePlan(std::vector<uint8_t>& buffer,
			const ChunkTypeLocations& locations,
			ChunkConnector& connector,
			const Timeouts& timeouts,
			const Timeout& totalTimeout);

	/**
	 * Gets feedback from the execution phase.
	 * \return set of parts from which reading data was abandoned
	 *         during the last successful call to executePlan
	 */
	const std::set<ChunkType>& partsOmitted() const {
		return partsOmitted_;
	}

	/// Counter for the .lizardfds_tweaks file.
	static std::atomic<uint64_t> executionsTotal;

	/// Counter for the .lizardfds_tweaks file.
	static std::atomic<uint64_t> executionsWithAdditionalOperations;

	/// Counter for the .lizardfds_tweaks file.
	static std::atomic<uint64_t> executionsFinishedByAdditionalOperations;

private:
	ChunkserverStats& chunkserverStats_;
	const uint64_t chunkId_;
	const uint32_t chunkVersion_;
	std::unique_ptr<const ReadPlan> plan_;
	std::set<ChunkType> partsOmitted_;

	/**
	 * Executes read operations from plan_.
	 * Starts with basicReadOperations and (if basicTimeout expires) starts
	 * additionalReadOperations.
	 * \param buffer     buffer, where the data will be stored (>= plan_.requiredBufferSize)
	 * \param locations  locations of all the chunkTypes that exist in the plan
	 * \param connector  object which will be used to obtain connections to chunkservers
	 * \param timeouts   a set of timeouts for the execution
	 * \return list of post-process operations that need to be done
	 */
	std::vector<ReadPlan::PostProcessOperation> executeReadOperations(
			uint8_t* buffer,
			const ChunkTypeLocations& locations,
			ChunkConnector& connector,
			const Timeouts& timeouts,
			const Timeout& totalTimeout);

	/**
	 * Executes given post-processing operations
	 * \param operations  list of operations to execute
	 * \param buffer      buffer to post-process
	 */
	void executePostProcessing(
			const std::vector<ReadPlan::PostProcessOperation> operations,
			uint8_t* buffer);
};
