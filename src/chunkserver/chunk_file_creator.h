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

#include "common/chunk_part_type.h"
#include "chunkserver/chunk.h"

/**
 * @brief Helper class to create chunk with data.
 *
 * This class is used to safely create chunk with data.
 * If data isn't written or committed before d-tor,
 * chunk will be deleted.
 */
class ChunkFileCreator {
public:
	ChunkFileCreator(uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType);
	~ChunkFileCreator();

	void create();
	void write(uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer);
	void commit();

	uint64_t chunkId() const { return chunk_id_; }
	uint32_t chunkVersion() const { return chunk_version_; }
	ChunkPartType chunkType() const { return chunk_type_; }

protected:
	uint64_t chunk_id_;
	uint32_t chunk_version_;
	ChunkPartType chunk_type_;

	Chunk *chunk_;

	bool is_created_;
	bool is_open_;
	bool is_commited_;
};
