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
