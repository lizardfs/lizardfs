#include "mount/write_cache_block.h"

#include <cstring>

#include "common/massert.h"
#include "common/MFSCommunication.h"

WriteCacheBlock::WriteCacheBlock(uint32_t chunkIndex, uint32_t blockIndex)
		: chunkIndex(chunkIndex),
		  blockIndex(blockIndex),
		  from(0),
		  to(0) {
	blockData = new uint8_t[MFSBLOCKSIZE];
}

WriteCacheBlock::WriteCacheBlock(WriteCacheBlock&& block) {
	blockData = block.blockData;
	chunkIndex = block.chunkIndex;
	blockIndex = block.blockIndex;
	from = block.from;
	to = block.to;
	block.blockData = nullptr;
}

WriteCacheBlock::~WriteCacheBlock() {
	if (blockData != nullptr) {
		delete[] blockData;
	}
}

bool WriteCacheBlock::expand(uint32_t from, uint32_t to, const uint8_t *buffer) {
	if (size() == 0) {
		this->from = from;
		this->to = to;
		memcpy(blockData + from, buffer, to - from);
		return true;
	}
	if (from > this->to || to < this->from) { // can't expand
		return false;
	}
	memcpy(blockData + from, buffer, to - from);
	if (from < this->from) {
		this->from = from;
	}
	if (to > this->to) {
		this->to = to;
	}
	return true;
}

uint64_t WriteCacheBlock::offsetInFile() const {
	return static_cast<uint64_t>(chunkIndex) * MFSCHUNKSIZE + offsetInChunk();
}

uint32_t WriteCacheBlock::offsetInChunk() const {
	return blockIndex * MFSBLOCKSIZE + from;
}

uint32_t WriteCacheBlock::size() const {
	return to - from;
}

const uint8_t* WriteCacheBlock::data() const {
	return blockData + from;
}
