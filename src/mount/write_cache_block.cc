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
#include "mount/write_cache_block.h"

#include <cstring>

#include "common/massert.h"
#include "common/MFSCommunication.h"

WriteCacheBlock::WriteCacheBlock(uint32_t chunkIndex, uint32_t blockIndex, Type type)
		: chunkIndex(chunkIndex),
		  blockIndex(blockIndex),
		  from(0),
		  to(0),
		  type(type) {
	sassert(blockIndex < MFSBLOCKSINCHUNK);
	blockData = new uint8_t[MFSBLOCKSIZE];
}

WriteCacheBlock::WriteCacheBlock(WriteCacheBlock&& block) {
	blockData = block.blockData;
	chunkIndex = block.chunkIndex;
	blockIndex = block.blockIndex;
	from = block.from;
	to = block.to;
	type = block.type;
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

uint8_t* WriteCacheBlock::data() {
	return blockData + from;
}
