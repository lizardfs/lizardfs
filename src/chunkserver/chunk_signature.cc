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

#include "common/MFSCommunication.h"

const char ChunkSignature::kMfsSignatureId[] = MFSSIGNATURE "C 1.0";
const char ChunkSignature::kLizSignatureId[] = "LIZC 1.0";

ChunkSignature::ChunkSignature()
		: chunkId_(0),
		  chunkVersion_(0),
		  chunkType_(ChunkType::getStandardChunkType()),
		  hasValidSignatureId_(false) {
}

ChunkSignature::ChunkSignature(uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType)
		: chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  chunkType_(chunkType),
		  hasValidSignatureId_(true) {
}

bool ChunkSignature::readFromDescriptor(int fd, off_t offset) {
	const ssize_t maxSignatureSize = kSignatureIdSize + 13;
	uint8_t buffer[maxSignatureSize];
	ssize_t ret = pread(fd, buffer, maxSignatureSize, offset);
	if (ret != maxSignatureSize) {
		return false;
	}

	const uint8_t* ptr = buffer + kSignatureIdSize;
	chunkId_ = get64bit(&ptr);
	chunkVersion_ = get32bit(&ptr);
	chunkType_ = ChunkType::getStandardChunkType();

	// Check if signature is equal to kMfsSignatureId or kLizSignatureId
	if (memcmp(buffer, kMfsSignatureId, kSignatureIdSize) == 0) {
		hasValidSignatureId_ = true;
	} else if (memcmp(buffer, kLizSignatureId, kSignatureIdSize) == 0) {
		hasValidSignatureId_ = true;
		uint8_t chunkTypeId = get8bit(&ptr);
		try {
			::deserialize(&chunkTypeId, sizeof(chunkTypeId), chunkType_);
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

