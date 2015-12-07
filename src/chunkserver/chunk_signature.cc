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
#include "chunkserver/chunk_signature.h"

#include <unistd.h>
#include <cstring>

#include "common/slice_traits.h"
#include "protocol/MFSCommunication.h"

const char ChunkSignature::kMfsSignatureId[] = MFSSIGNATURE "C 1.0";
const char ChunkSignature::kLizSignatureId10[] = "LIZC 1.0";
const char ChunkSignature::kLizSignatureId[] = "LIZC 1.1";

ChunkSignature::ChunkSignature()
		: chunkId_(0),
		  chunkVersion_(0),
		  chunkType_(),
		  hasValidSignatureId_(false) {
}

ChunkSignature::ChunkSignature(uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType)
		: chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  chunkType_(chunkType),
		  hasValidSignatureId_(true) {
}

bool ChunkSignature::readFromDescriptor(int fd, off_t offset) {
	uint8_t buffer[kSignatureSize];
	ssize_t ret = pread(fd, buffer, kSignatureSize, offset);
	if (ret != (ssize_t)kSignatureSize) {
		return false;
	}

	const uint8_t* ptr = buffer + kSignatureIdSize;
	chunkId_ = get64bit(&ptr);
	chunkVersion_ = get32bit(&ptr);
	chunkType_ = slice_traits::standard::ChunkPartType();

	// Check if signature is equal to kMfsSignatureId or kLizSignatureId
	if (memcmp(buffer, kMfsSignatureId, kSignatureIdSize) == 0) {
		hasValidSignatureId_ = true;
	} else if (memcmp(buffer, kLizSignatureId10, kSignatureIdSize) == 0) {
		hasValidSignatureId_ = true;
		legacy::ChunkPartType legacy_chunk_type;
		try {
			::deserialize(ptr, sizeof(legacy::ChunkPartType), legacy_chunk_type);
			chunkType_ = legacy_chunk_type;
		} catch (Exception& ex) {
			return false;
		}
	} else if (memcmp(buffer, kLizSignatureId, kSignatureIdSize) == 0) {
		hasValidSignatureId_ = true;
		try {
			::deserialize(ptr, sizeof(ChunkPartType), chunkType_);
		} catch (Exception& ex) {
			return false;
		}
	} else {
		hasValidSignatureId_ = false;
	}
	return true;
}

uint32_t ChunkSignature::serializedSize() const {
	return kSignatureIdSize + ::serializedSize(chunkId_, chunkVersion_, chunkType_);
}

void ChunkSignature::serialize(uint8_t **destination) const {
	memcpy(*destination, kLizSignatureId, kSignatureIdSize);
	*destination += kSignatureIdSize;
	::serialize(destination, chunkId_, chunkVersion_, chunkType_);
}
