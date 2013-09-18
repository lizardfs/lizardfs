#ifndef LIZARDFS_MFSCHUNKSERVER_CHUNK_SIGNATURE_H
#define LIZARDFS_MFSCHUNKSERVER_CHUNK_SIGNATURE_H

#include <inttypes.h>
#include <cstdlib>

class ChunkSignature {
public:
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

private:
	static const size_t kSignatureIdSize = 8;
	static const char kMfsSignatureId[];
	static const char kLizSignatureId[];

	uint64_t chunkId_;
	uint32_t chunkVersion_;
	uint8_t chunkTypeId_;
	bool hasValidSignatureId_;
};

#endif /* LIZARDFS_MFSCHUNKSERVER_CHUNK_SIGNATURE_H */
