/*
   Copyright 2013-2019 Skytechnology sp. z o.o.

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
	WriteCacheBlock(WriteCacheBlock&& block) noexcept;
	~WriteCacheBlock();
	WriteCacheBlock& operator=(const WriteCacheBlock&) = delete;
	WriteCacheBlock& operator=(WriteCacheBlock&&);
	bool expand(uint32_t from, uint32_t to, const uint8_t *buffer);
	uint64_t offsetInFile() const;
	uint32_t offsetInChunk() const;
	uint32_t size() const;
	const uint8_t* data() const;
	uint8_t* data();
};
