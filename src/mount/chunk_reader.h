#pragma once

#include <poll.h>
#include <cstdint>
#include <map>
#include <mutex>
#include <vector>
#include <string>

#include "common/chunk_connector.h"
#include "common/chunk_type_with_address.h"
#include "common/connection_pool.h"
#include "common/massert.h"
#include "common/network_address.h"
#include "common/standard_chunk_read_planner.h"
#include "common/time_utils.h"
#include "mount/chunk_locator.h"

class ChunkReader {
public:
	ChunkReader(ChunkConnector& connector, ReadChunkLocator& locator);

	/*
	 * Uses locator to locate the chunk and chooses chunkservers to read from
	 */
	void prepareReadingChunk(uint32_t inode, uint32_t index);

	/*
	 * Reads data from the previously located chunk and appends it to the buffer
	 */
	uint32_t readData(std::vector<uint8_t>& buffer, uint32_t offset, uint32_t size,
		const Timeout& communicationTimeout);

	uint32_t inode() {
		return inode_;
	}
	uint32_t index() {
		return index_;
	}
	uint64_t chunkId() {
		return location_->chunkId;
	}
	uint32_t version() {
		return location_->version;
	}

private:
	ChunkConnector& connector_;
	ReadChunkLocator& locator_;
	uint32_t inode_;
	uint32_t index_;
	std::shared_ptr<const ChunkLocationInfo> location_;
	StandardChunkReadPlanner planner_;
	std::map<ChunkType, NetworkAddress> chunkTypeLocations_;
	std::vector<ChunkTypeWithAddress> crcErrors_;
};
