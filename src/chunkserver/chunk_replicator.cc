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
#include "chunkserver/chunk_replicator.h"

#include <unistd.h>
#include <algorithm>
#include <initializer_list>
#include <memory>
#include <string>

#include "chunkserver/g_limiters.h"
#include "common/crc.h"
#include "common/cstocs_communication.h"
#include "common/exception.h"
#include "common/packet.h"
#include "common/read_plan_executor.h"
#include "common/sockets.h"
#include "common/standard_chunk_read_planner.h"
#include "common/xor_chunk_read_planner.h"

static ConnectionPool pool;
static ChunkConnectorUsingPool connector(pool);
ChunkReplicator gReplicator(connector);

ChunkReplicator::ChunkReplicator(ChunkConnector& connector) : connector_(connector), stats_(0) {}

uint32_t ChunkReplicator::getStats() {
	std::unique_lock<std::mutex> lock(mutex_);
	uint32_t ret = stats_;
	stats_ = 0;
	return ret;
}

std::unique_ptr<ReadPlanner> ChunkReplicator::getPlanner(ChunkType chunkType,
		const std::vector<ChunkTypeWithAddress>& sources) {
	std::unique_ptr<ReadPlanner> planner;
	if (chunkType.isXorChunkType()) {
		planner.reset(new XorChunkReadPlanner(chunkType));
	} else {
		planner.reset(new StandardChunkReadPlanner);
	}

	std::vector<ChunkType> availableParts;
	for (const auto& source : sources) {
		availableParts.push_back(source.chunkType);
	}
	planner->prepare(availableParts);
	return planner;
}

uint32_t ChunkReplicator::getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType, NetworkAddress server) throw (Exception) {
	int fd = connector.startUsingConnection(server, Timeout{std::chrono::seconds(1)});
	sassert(fd >= 0);

	std::vector<uint8_t> outputBuffer;
	cstocs::getChunkBlocks::serialize(outputBuffer, chunkId, chunkVersion, chunkType);
	tcptowrite(fd, outputBuffer.data(), outputBuffer.size(), 1000);

	std::vector<uint8_t> inputBuffer;
	PacketHeader header;
	receivePacket(header, inputBuffer, fd, 1000);
	if (header.type != LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS) {
		close(fd);
		throw Exception("Unexpected response for chunk get blocks request");
	}
	connector.endUsingConnection(fd, server);

	uint64_t rxChunkId;
	uint32_t rxChunkVersion;
	ChunkType rxChunkType = ChunkType::getStandardChunkType();
	uint16_t nrOfBlocks;
	uint8_t status;
	cstocs::getChunkBlocksStatus::deserialize(inputBuffer, rxChunkId, rxChunkVersion, rxChunkType,
			nrOfBlocks, status);
	auto expected = std::make_tuple(chunkId, chunkVersion, chunkType, uint8_t(LIZARDFS_STATUS_OK));
	auto actual = std::make_tuple(rxChunkId, rxChunkVersion, rxChunkType, status);
	if (actual != expected) {
		throw Exception("Received invalid response for chunk get block");
	}

	// Success!
	if (chunkType.isStandardChunkType()) {
		return nrOfBlocks;
	} else if (chunkType.isXorChunkType()
			&& (chunkType.isXorParity() || chunkType.getXorPart() == 1)) {
		return std::min(MFSBLOCKSINCHUNK, nrOfBlocks * chunkType.getXorLevel());
	} else {
		sassert(chunkType.isXorChunkType() && !chunkType.isXorParity());
		return std::min(MFSBLOCKSINCHUNK, (nrOfBlocks + 1) * chunkType.getXorLevel());
	}
}

uint32_t ChunkReplicator::getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
		const std::vector<ChunkTypeWithAddress>& sources) {
	auto isStandardChunkType = [](const ChunkTypeWithAddress& ctwa) {
		return ctwa.chunkType.isStandardChunkType();
	};
	auto isParityOrXorFirstPart = [](const ChunkTypeWithAddress& ctwa) {
		return ctwa.chunkType.isXorChunkType() &&
				(ctwa.chunkType.isXorParity() || ctwa.chunkType.getXorPart() == 1);
	};
	auto standardOnes =
			std::find_if(sources.begin(), sources.end(), isStandardChunkType);
	auto parityAndFirstOnes =
			std::find_if(sources.begin(), sources.end(), isParityOrXorFirstPart);
	// If there is neither standard copy nor part1 nor parity, replication will fail anyway

	for (auto chunkTypesWithAdressesIterator : {standardOnes, parityAndFirstOnes}) {
		for (auto it = chunkTypesWithAdressesIterator; it != sources.end(); ++it) {
			try {
				return getChunkBlocks(chunkId, chunkVersion, it->chunkType, it->address);
			} catch (Exception& e) {
				syslog(LOG_WARNING, "%s", e.what());
				// there might be some problems with this specific part/connection
				// let's just ignore them and try to get the size from some other part
				continue;
			}
		}
	}

	return MFSBLOCKSINCHUNK;
}

void ChunkReplicator::replicate(ChunkFileCreator& fileCreator,
		const std::vector<ChunkTypeWithAddress>& sources) {
	// Create planner
	std::unique_ptr<ReadPlanner> planner = getPlanner(fileCreator.chunkType(), sources);
	if (!planner->isReadingPossible()) {
		throw Exception("No copies to read from");
	}

	// Get number of blocks to replicate
	uint32_t blocks = getChunkBlocks(fileCreator.chunkId(), fileCreator.chunkVersion(), sources);
	uint32_t batchSize = 50;
	if (fileCreator.chunkType().isXorChunkType()) {
		ChunkType::XorLevel level = fileCreator.chunkType().getXorLevel();
		blocks = fileCreator.chunkType().getNumberOfBlocks(blocks);
		// Round batchSize to work better with available xor level:
		batchSize = level * ((batchSize + level - 1) / level);
	}

	fileCreator.create();
	static const SteadyDuration maxWaitTime = std::chrono::seconds(60);
	Timeout timeout{maxWaitTime};
	for (uint32_t firstBlock = 0; firstBlock < blocks; firstBlock += batchSize) {
		uint32_t nrOfBlocks = std::min(blocks - firstBlock, batchSize);

		// Wait for limit to be assigned
		uint8_t status = replicationBandwidthLimiter().wait(nrOfBlocks * MFSBLOCKSIZE, maxWaitTime);
		if (status != LIZARDFS_STATUS_OK) {
			syslog(LOG_WARNING, "Replication bandwidth limiting error: %s", mfsstrerr(status));
			return;
		}
		// Build and execute the plan
		std::vector<uint8_t> buffer;
		ReadPlanExecutor::ChunkTypeLocations locations;
		for (const auto& source : sources) {
			locations[source.chunkType] = source.address;
		}
		ReadPlanExecutor::Timeouts timeouts(timeout.remaining_ms(), timeout.remaining_ms());
		ReadPlanExecutor executor(chunkserverStats_,
				fileCreator.chunkId(), fileCreator.chunkVersion(),
				planner->buildPlanFor(firstBlock, nrOfBlocks));
		executor.executePlan(buffer, locations, connector_, timeouts, timeout);

		for (uint32_t i = 0; i < nrOfBlocks; ++i) {
			uint32_t offset = i * MFSBLOCKSIZE;
			const uint8_t* dataBlock = buffer.data() + offset;
			uint32_t crc = mycrc32(0, dataBlock, MFSBLOCKSIZE);
			uint32_t offsetInChunk = offset + firstBlock * MFSBLOCKSIZE;
			fileCreator.write(offsetInChunk, MFSBLOCKSIZE, crc, dataBlock);
		}
	}
	fileCreator.commit();
}
