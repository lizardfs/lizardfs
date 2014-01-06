#pragma once

#include <cstdint>

struct WriteCacheBlock {
public:
	uint8_t* blockData;
	uint32_t chunkIndex;
	uint32_t blockIndex;
	uint32_t from;
	uint32_t to;

	WriteCacheBlock(uint32_t chunkIndex, uint32_t blockIndex);
	WriteCacheBlock(const WriteCacheBlock&) = delete;
	WriteCacheBlock(WriteCacheBlock&& block);
	~WriteCacheBlock();
	WriteCacheBlock& operator=(const WriteCacheBlock&) = delete;
	WriteCacheBlock& operator=(WriteCacheBlock&&) = delete;
	bool expand(uint32_t from, uint32_t to, const uint8_t *buffer);
	uint64_t offsetInFile() const;
	uint32_t offsetInChunk() const;
	uint32_t size() const;
	const uint8_t* data() const;
	uint8_t* data();
};
