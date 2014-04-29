#pragma once

#include "config.h"

#include <cstdint>

#include "common/chunk_type.h"

class ChunkFileCreator {
public:
	ChunkFileCreator(uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType)
			: chunkId_(chunkId),
			  chunkVersion_(chunkVersion),
			  chunkType_(chunkType) {
	}
	virtual ~ChunkFileCreator() {}
	virtual void create() = 0;
	virtual void write(uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer) = 0;
	virtual void commit() = 0;
	uint64_t chunkId() const { return chunkId_; }
	uint32_t chunkVersion() const { return chunkVersion_; }
	ChunkType chunkType() const { return chunkType_; }

private:
	uint64_t chunkId_;
	uint32_t chunkVersion_;
	ChunkType chunkType_;
};
