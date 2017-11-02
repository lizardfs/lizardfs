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
#include "chunkserver/chunk_replicator.h"

#include <unistd.h>
#include <cassert>
#include <algorithm>
#include <initializer_list>
#include <memory>
#include <string>

#include "chunkserver/g_limiters.h"
#include "common/crc.h"
#include "common/exception.h"
#include "common/lizardfs_version.h"
#include "common/read_plan_executor.h"
#include "common/sockets.h"
#include "protocol/cstocs.h"

static ConnectionPool gPool;
static ChunkConnectorUsingPool gConnector(gPool);
ChunkReplicator gReplicator(gConnector);

ChunkReplicator::ChunkReplicator(ChunkConnector& connector)
	: connector_(connector), stats_(0), total_timeout_ms_(kDefaultTotalTimeout_ms),
	  wave_timeout_ms_(kDefaultWaveTimeout_ms), connection_timeout_ms_(kDefaultConnectionTimeout_ms) {}

uint32_t ChunkReplicator::getStats() {
	std::unique_lock<std::mutex> lock(mutex_);
	uint32_t ret = stats_;
	stats_ = 0;
	return ret;
}

uint32_t ChunkReplicator::getChunkBlocks(uint64_t chunk_id, uint32_t chunk_version,
		ChunkTypeWithAddress type_with_address) {
	NetworkAddress server = type_with_address.address;
	ChunkPartType chunk_type = type_with_address.chunk_type;
	int fd = connector_.startUsingConnection(server, Timeout{std::chrono::milliseconds(connection_timeout_ms_)});
	sassert(fd >= 0);

	std::vector<uint8_t> output_buffer;
	if (type_with_address.chunkserver_version >= kFirstECVersion) {
		cstocs::getChunkBlocks::serialize(output_buffer, chunk_id, chunk_version, chunk_type);
	} else if (type_with_address.chunkserver_version >= kFirstXorVersion) {
		assert((int)chunk_type.getSliceType() < Goal::Slice::Type::kECFirst);
		cstocs::getChunkBlocks::serialize(output_buffer, chunk_id, chunk_version, (legacy::ChunkPartType)chunk_type);
	} else {
		assert(slice_traits::isStandard(chunk_type));
		serializeMooseFsPacket(output_buffer, CSTOCS_GET_CHUNK_BLOCKS, chunk_id, chunk_version);
	}
	tcptowrite(fd, output_buffer.data(), output_buffer.size(), 1000);

	std::vector<uint8_t> input_buffer;
	PacketHeader header;
	receivePacket(header, input_buffer, fd, 1000);
	if (header.type != LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS
			&& header.type != CSTOCS_GET_CHUNK_BLOCKS_STATUS) {
		close(fd);
		throw Exception("Unexpected response for chunk get blocks request");
	}
	connector_.endUsingConnection(fd, server);

	uint64_t rx_chunk_id;
	uint32_t rx_chunk_version;
	ChunkPartType rx_chunk_type = slice_traits::standard::ChunkPartType();
	uint16_t nr_of_blocks;
	uint8_t status;
	if (header.type == LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS) {
		PacketVersion v;
		deserializePacketVersionNoHeader(input_buffer, v);
		if (v == cstocs::getChunkBlocksStatus::kECChunks) {
			cstocs::getChunkBlocksStatus::deserialize(input_buffer, rx_chunk_id, rx_chunk_version, rx_chunk_type,
				nr_of_blocks, status);
		} else {
			legacy::ChunkPartType legacy_type;
			cstocs::getChunkBlocksStatus::deserialize(input_buffer, rx_chunk_id, rx_chunk_version, legacy_type,
				nr_of_blocks, status);
			rx_chunk_type = legacy_type;
		}
	} else {
		deserializeAllMooseFsPacketDataNoHeader(input_buffer.data(), input_buffer.size(),
				rx_chunk_id, rx_chunk_version, nr_of_blocks, status);
	}
	auto expected =
	    std::make_tuple(chunk_id, chunk_version, chunk_type, uint8_t(LIZARDFS_STATUS_OK));
	auto actual = std::make_tuple(rx_chunk_id, rx_chunk_version, rx_chunk_type, status);
	if (actual != expected) {
		throw Exception("Received invalid response for chunk get block");
	}

	if (slice_traits::isParityPart(chunk_type) || slice_traits::getDataPartIndex(chunk_type) == 0) {
		return std::min(MFSBLOCKSINCHUNK,
		                nr_of_blocks * slice_traits::getNumberOfDataParts(chunk_type));
	}
	return std::min(MFSBLOCKSINCHUNK,
	                (nr_of_blocks + 1) * slice_traits::getNumberOfDataParts(chunk_type));
}

uint32_t ChunkReplicator::getChunkBlocks(uint64_t chunk_id, uint32_t chunk_version,
		const std::vector<ChunkTypeWithAddress> &sources) {
	std::vector<ChunkTypeWithAddress> src(sources);

	std::partition(src.begin(), src.end(), [](const ChunkTypeWithAddress &ctwa) {
		return slice_traits::isParityPart(ctwa.chunk_type) ||
		       slice_traits::getDataPartIndex(ctwa.chunk_type) == 0;
	});

	for (const auto &ctwa : src) {
		try {
			return getChunkBlocks(chunk_id, chunk_version, ctwa);
		} catch (Exception &e) {
			lzfs_pretty_syslog(LOG_WARNING, "%s", e.what());
			// there might be some problems with this specific part/connection
			// let's just ignore them and try to get the size from some other part
		}
	}

	return MFSBLOCKSINCHUNK;
}

void ChunkReplicator::replicate(ChunkFileCreator& fileCreator,
		const std::vector<ChunkTypeWithAddress>& sources) {
	// Get number of blocks to replicate
	int blocks = getChunkBlocks(fileCreator.chunkId(), fileCreator.chunkVersion(), sources);
	int batchSize = 50;
	int data_part_count = slice_traits::getNumberOfDataParts(fileCreator.chunkType());
	blocks = slice_traits::getNumberOfBlocks(fileCreator.chunkType(), blocks);
	batchSize = data_part_count * ((batchSize + data_part_count - 1) / data_part_count);

	SliceRecoveryPlanner planner;
	ReadPlanExecutor::ChunkTypeLocations locations;
	SliceRecoveryPlanner::PartsContainer available_parts;
	std::vector<uint8_t> buffer;

	for (const auto& source : sources) {
		available_parts.push_back(source.chunk_type);

		if (locations.count(source.chunk_type)) {
			continue;
		}
		locations[source.chunk_type] = source;
	}

	fileCreator.create();
	static const SteadyDuration max_wait_time = std::chrono::milliseconds(total_timeout_ms_);
	Timeout timeout{max_wait_time};
	for (int firstBlock = 0; firstBlock < blocks; firstBlock += batchSize) {
		int nrOfBlocks = std::min(blocks - firstBlock, batchSize);

		planner.prepare(fileCreator.chunkType(), firstBlock, nrOfBlocks, available_parts);
		if (!planner.isReadingPossible()) {
			throw Exception("No copies to read from");
		}

		// Wait for limit to be assigned
		uint8_t status = replicationBandwidthLimiter().wait(nrOfBlocks * MFSBLOCKSIZE, max_wait_time);
		if (status != LIZARDFS_STATUS_OK) {
			throw Exception("Replication limiting error", status);
		}

		// Build and execute the plan
		buffer.clear();
		ReadPlanExecutor executor(chunkserverStats_,
				fileCreator.chunkId(), fileCreator.chunkVersion(),
				planner.buildPlan());
		executor.executePlan(buffer, locations, connector_, timeout.remaining_ms(), wave_timeout_ms_, timeout);

		for (int i = 0; i < nrOfBlocks; ++i) {
			uint32_t offset = i * MFSBLOCKSIZE;
			const uint8_t* dataBlock = buffer.data() + offset;
			uint32_t crc = mycrc32(0, dataBlock, MFSBLOCKSIZE);
			uint32_t offsetInChunk = offset + firstBlock * MFSBLOCKSIZE;
			fileCreator.write(offsetInChunk, MFSBLOCKSIZE, crc, dataBlock);
		}
	}

	fileCreator.commit();
	incStats();
}

void ChunkReplicator::incStats() {
	std::unique_lock<std::mutex> lock(mutex_);
	stats_++;
}
