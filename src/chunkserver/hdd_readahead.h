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
#include <cstdint>

#include "protocol/MFSCommunication.h"

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
