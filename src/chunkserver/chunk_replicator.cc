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
#include "common/standard_chunk_read_planner.h"
#include "common/xor_chunk_read_planner.h"
#include "protocol/cstocs.h"
#include "protocol/packet.h"

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

std::unique_ptr<ReadPlanner> ChunkReplicator::getPlanner(ChunkPartType chunkType,
		const std::vector<ChunkTypeWithAddress>& sources) {
	std::unique_ptr<ReadPlanner> planner;
	if (slice_traits::isXor(chunkType)) {
		planner.reset(new XorChunkReadPlanner(chunkType));
	} else {
		planner.reset(new StandardChunkReadPlanner);
	}

	std::vector<ChunkPartType> availableParts;
	for (const auto& source : sources) {
		availableParts.push_back(source.chunk_type);
	}
	planner->prepare(availableParts);
	return planner;
}

uint32_t ChunkReplicator::getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
		ChunkTypeWithAddress type_with_address) throw (Exception) {
	NetworkAddress server = type_with_address.address;
	ChunkPartType chunkType = type_with_address.chunk_type;
	int fd = connector.startUsingConnection(server, Timeout{std::chrono::seconds(1)});
	sassert(fd >= 0);

	std::vector<uint8_t> outputBuffer;
	if (type_with_address.chunkserver_version >= kFirstECVersion) {
		cstocs::getChunkBlocks::serialize(outputBuffer, chunkId, chunkVersion, chunkType);
	} else if (type_with_address.chunkserver_version >= kFirstXorVersion) {
		assert((int)chunkType.getSliceType() < Goal::Slice::Type::kECFirst);
		cstocs::getChunkBlocks::serialize(outputBuffer, chunkId, chunkVersion, (legacy::ChunkPartType)chunkType);
	} else {
		assert(slice_traits::isStandard(chunkType));
		serializeMooseFsPacket(outputBuffer, CSTOCS_GET_CHUNK_BLOCKS, chunkId, chunkVersion);
	}
	tcptowrite(fd, outputBuffer.data(), outputBuffer.size(), 1000);

	std::vector<uint8_t> inputBuffer;
	PacketHeader header;
	receivePacket(header, inputBuffer, fd, 1000);
	if (header.type != LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS
			&& header.type != CSTOCS_GET_CHUNK_BLOCKS_STATUS) {
		close(fd);
		throw Exception("Unexpected response for chunk get blocks request");
	}
	connector.endUsingConnection(fd, server);

	uint64_t rxChunkId;
	uint32_t rxChunkVersion;
	ChunkPartType rxChunkType = slice_traits::standard::ChunkPartType();
	uint16_t nrOfBlocks;
	uint8_t status;
	if (header.type == LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS) {
		PacketVersion v;
		deserializePacketVersionNoHeader(inputBuffer, v);
		if (v == cstocs::getChunkBlocksStatus::kECChunks) {
			cstocs::getChunkBlocksStatus::deserialize(inputBuffer, rxChunkId, rxChunkVersion, rxChunkType,
				nrOfBlocks, status);
		} else {
			legacy::ChunkPartType legacy_type;
			cstocs::getChunkBlocksStatus::deserialize(inputBuffer, rxChunkId, rxChunkVersion, legacy_type,
				nrOfBlocks, status);
			rxChunkType = legacy_type;
		}
	} else {
		deserializeAllMooseFsPacketDataNoHeader(inputBuffer.data(), inputBuffer.size(),
				rxChunkId, rxChunkVersion, nrOfBlocks, status);
	}
	auto expected = std::make_tuple(chunkId, chunkVersion, chunkType, uint8_t(LIZARDFS_STATUS_OK));
	auto actual = std::make_tuple(rxChunkId, rxChunkVersion, rxChunkType, status);
	if (actual != expected) {
		throw Exception("Received invalid response for chunk get block");
	}

	// Success!
	if (slice_traits::isStandard(chunkType)) {
		return nrOfBlocks;
	} else if (slice_traits::isXor(chunkType)
			&& (slice_traits::xors::isXorParity(chunkType) || slice_traits::xors::getXorPart(chunkType) == 1)) {
		return std::min(MFSBLOCKSINCHUNK, nrOfBlocks * slice_traits::xors::getXorLevel(chunkType));
	} else {
		sassert(slice_traits::isXor(chunkType) && !slice_traits::xors::isXorParity(chunkType));
		return std::min(MFSBLOCKSINCHUNK, (nrOfBlocks + 1) * slice_traits::xors::getXorLevel(chunkType));
	}
}

uint32_t ChunkReplicator::getChunkBlocks(uint64_t chunkId, uint32_t chunkVersion,
		const std::vector<ChunkTypeWithAddress>& sources) {
	auto isStandardChunkType = [](const ChunkTypeWithAddress& ctwa) {
		return slice_traits::isStandard(ctwa.chunk_type);
	};
	auto isParityOrXorFirstPart = [](const ChunkTypeWithAddress& ctwa) {
		return slice_traits::isXor(ctwa.chunk_type) &&
				(slice_traits::xors::isXorParity(ctwa.chunk_type) || slice_traits::xors::getXorPart(ctwa.chunk_type) == 1);
	};
	auto standardOnes =
			std::find_if(sources.begin(), sources.end(), isStandardChunkType);
	auto parityAndFirstOnes =
			std::find_if(sources.begin(), sources.end(), isParityOrXorFirstPart);
	// If there is neither standard copy nor part1 nor parity, replication will fail anyway

	for (auto chunkTypesWithAdressesIterator : {standardOnes, parityAndFirstOnes}) {
		for (auto it = chunkTypesWithAdressesIterator; it != sources.end(); ++it) {
			try {
				return getChunkBlocks(chunkId, chunkVersion, *it);
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
	if (slice_traits::isXor(fileCreator.chunkType())) {
		int level = slice_traits::xors::getXorLevel(fileCreator.chunkType());
		blocks = slice_traits::getNumberOfBlocks(fileCreator.chunkType(), blocks);
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
			locations[source.chunk_type] = source;
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
	incStats();
}

void ChunkReplicator::incStats() {
	std::unique_lock<std::mutex> lock(mutex_);
	stats_++;
}
