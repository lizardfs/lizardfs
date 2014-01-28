#pragma once

#include <cstdint>

struct WriteCacheBlock {
public:
	enum Type {
		kWritableBlock, // normal block, written by clients
		kReadOnlyBlock, // a kWriteableBlock after it is passed to ChunkWriter for the first time
		kParityBlock,   // a parity block
		kReadBlock      // a block read from a chunkserver to calculate a parity
	};

	uint8_t* blockData;
	uint32_t chunkIndex;
	uint32_t blockIndex;
	uint32_t from;
	uint32_t to;
	Type type;

	WriteCacheBlock(uint32_t chunkIndex, uint32_t blockIndex, Type type);
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
