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
#include "chunkserver/chunk_file_creator.h"

#include "chunkserver/hddspacemgr.h"

ChunkFileCreator::ChunkFileCreator(uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType)
	: chunk_id_(chunkId),
	  chunk_version_(chunkVersion),
	  chunk_type_(chunkType),
	  chunk_(nullptr),
	  is_created_(false),
	  is_open_(false),
	  is_commited_(false) {
}

ChunkFileCreator::~ChunkFileCreator() {
	if (is_open_) {
		assert(chunk_);
		hdd_close(chunk_);
	}
	if (is_created_ && !is_commited_) {
		assert(chunk_);
		hdd_int_delete(chunk_, 0);
		chunk_ = nullptr;
	}
	if (chunk_) {
		hdd_chunk_release(chunk_);
	}
}

void ChunkFileCreator::create() {
	assert(!is_created_ && !chunk_);

	auto result = hdd_int_create_chunk(chunk_id_, 0, chunk_type_);
	if (result.first == LIZARDFS_STATUS_OK) {
		chunk_ = result.second;
		is_created_ = true;
	} else {
		throw Exception("failed to create chunk", result.first);
	}
	int status = hdd_open(chunk_);
	if (status == LIZARDFS_STATUS_OK) {
		is_open_ = true;
	} else {
		throw Exception("failed to open created chunk", status);
	}
}

void ChunkFileCreator::write(uint32_t offset, uint32_t size, uint32_t crc, const uint8_t *buffer) {
	assert(is_open_ && !is_commited_ && chunk_);
	int blocknum = offset / MFSBLOCKSIZE;
	offset = offset % MFSBLOCKSIZE;
	int status = hdd_write(chunk_, 0, blocknum, offset, size, crc, buffer);
	if (status != LIZARDFS_STATUS_OK) {
		throw Exception("failed to write chunk", status);
	}
}

void ChunkFileCreator::commit() {
	assert(is_open_ && !is_commited_);
	int status = hdd_close(chunk_);
	if (status == LIZARDFS_STATUS_OK) {
		is_open_ = false;
	} else {
		throw Exception("failed to close chunk", status);
	}
	status = hdd_int_version(chunk_, 0, chunk_version_);
	if (status == LIZARDFS_STATUS_OK) {
		is_commited_ = true;
	} else {
		throw Exception("failed to set chunk's version", status);
	}
}
