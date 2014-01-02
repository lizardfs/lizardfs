#pragma once

#include <inttypes.h>
#include <cstdlib>

class ChunkSignature {
public:
	static const size_t kSignatureIdOffset = 0;
	static const size_t kSignatureIdSize = 8;
	static const size_t kChunkIdOffset = kSignatureIdOffset + kSignatureIdSize;
	static const size_t kVersionOffset = kChunkIdOffset + sizeof(uint64_t);
	static const size_t kChunkTypeOffset = kVersionOffset + sizeof(uint32_t);

	ChunkSignature();
	bool readFromDescriptor(int fd, off_t offset);

	bool hasValidSignatureId() const {
		return hasValidSignatureId_;
	}

	uint64_t chunkId() const {
		return chunkId_;
	}

	uint32_t chunkVersion() const {
		return chunkVersion_;
	}

	uint8_t chunkTypeId() const {
		return chunkTypeId_;
	}

	static const char kMfsSignatureId[];
	static const char kLizSignatureId[];

private:
	uint64_t chunkId_;
	uint32_t chunkVersion_;
	uint8_t chunkTypeId_;
	bool hasValidSignatureId_;
};
