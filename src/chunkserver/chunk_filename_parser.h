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

#include <string>

#include "chunkserver/chunk_format.h"
#include "common/chunk_type.h"
#include "common/parser.h"

class ChunkFilenameParser : public Parser {
public:
	enum Status {
		OK,
		ERROR_INVALID_FILENAME
	};

	ChunkFilenameParser(const std::string& filename);
	Status parse();
	ChunkFormat chunkFormat() const;
	ChunkType chunkType() const;
	uint32_t chunkVersion() const;
	uint64_t chunkId() const;
	ChunkType::XorPart xorPart() const;
	ChunkType::XorLevel xorLevel() const;

private:
	static const size_t kChunkVersionStringSize = 8;
	static const size_t kChunkIdStringSize = 16;

	ChunkFormat chunkFormat_;
	std::string chunkName_;
	ChunkType chunkType_;
	uint32_t chunkVersion_;
	uint64_t chunkId_;
	ChunkType::XorPart xorPart_;
	ChunkType::XorLevel xorLevel_;

	Status parseChunkType();
};
