#pragma once

#include "config.h"

#include <atomic>
#include <cstdint>

#include "common/MFSCommunication.h"

class HDDReadAhead {
public:
	uint16_t maxBlocksToBeReadBehind() {
		return maxBlocksToBeReadBehind_;
	}
	uint16_t blocksToBeReadAhead() {
		return blocksToBeReadAhead_;
	}
	void setMaxReadBehind_kB(uint32_t readbehind_kB) {
		maxBlocksToBeReadBehind_ = kBToBlocks(readbehind_kB);
	}
	void setReadAhead_kB(uint32_t readahead_kB) {
		blocksToBeReadAhead_ = kBToBlocks(readahead_kB);
	}

	static uint16_t kBToBlocks(uint32_t kB) {
		return (kB * 1024) / MFSBLOCKSIZE;
	}
private:
	std::atomic<uint16_t> maxBlocksToBeReadBehind_;
	std::atomic<uint16_t> blocksToBeReadAhead_;
};

extern HDDReadAhead gHDDReadAhead;
