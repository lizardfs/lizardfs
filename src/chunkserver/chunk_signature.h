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
#include <cstdlib>

#include "common/chunk_part_type.h"
#include "common/serialization_macros.h"

class ChunkSignature {
public:
	static const size_t kSignatureIdOffset = 0;
	static const size_t kSignatureIdSize = 8;
	static const size_t kChunkIdOffset = kSignatureIdOffset + kSignatureIdSize;
	static const size_t kVersionOffset = kChunkIdOffset + sizeof(uint64_t);
	static const size_t kChunkTypeOffset = kVersionOffset + sizeof(uint32_t);

	// Constructs a signature that can be initialized using readFromDescriptor
	ChunkSignature();

	// Constructs a initialized signature that can be serialized
	ChunkSignature(uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType);

	// Initialize this object having a descriptor to file where signature is written at given offset
	bool readFromDescriptor(int fd, off_t offset);

	// True if the object is initialized from a file which has proper signature ID
	bool hasValidSignatureId() const {
		return hasValidSignatureId_;
	}

	// Returns chunk ID stored in this signature
	uint64_t chunkId() const {
		return chunkId_;
	}

	// Returns version of chunk stored in this signature
	uint32_t chunkVersion() const {
		return chunkVersion_;
	}

	// Returns chunk type ID stored in this signature
	ChunkPartType chunkType() const {
		return chunkType_;
	}

	// Serialization
	uint32_t serializedSize() const;
	void serialize(uint8_t **destination) const;

	// Signature ID of chunks created by MooseFS or old LizardFS versions (without xor support)
	static const char kMfsSignatureId[];

	// Signature ID of chunks created by LizardFS versions with xor support
	static const char kLizSignatureId[];

private:
	uint64_t chunkId_;
	uint32_t chunkVersion_;
	ChunkPartType chunkType_;
	bool hasValidSignatureId_;
};
