#include "chunk_signature.h"

#include "config.h"
#include <unistd.h>
#include <cstring>

#include "common/chunk_type.h"
#include "common/datapack.h"
#include "common/MFSCommunication.h"

const char ChunkSignature::kMfsSignatureId[] = MFSSIGNATURE "C 1.0";
const char ChunkSignature::kLizSignatureId[] = "LIZC 1.0";

ChunkSignature::ChunkSignature()
		: chunkId_(0),
		  chunkVersion_(0),
		  chunkTypeId_(ChunkType::getStandardChunkType().chunkTypeId()),
		  hasValidSignatureId_(false) {
}

bool ChunkSignature::readFromDescriptor(int fd, off_t offset) {
	const ssize_t maxSignatureSize = kSignatureIdSize + 13;
	uint8_t buffer[maxSignatureSize];
#ifdef HAVE_PREAD
	ssize_t ret = pread(fd, buffer, maxSignatureSize, offset);
#else
	if (lseek(fd, offset, SEEK_SET) != offset) {
		return false;
	}
	ssize_t ret = read(fs, buffer, maxSignatureSize);
#endif
	if (ret != maxSignatureSize) {
		return false;
	}

	const uint8_t* ptr = buffer + kSignatureIdSize;
	chunkId_ = get64bit(&ptr);
	chunkVersion_ = get32bit(&ptr);

	// Check if signature is equal to kMfsSignatureId or kLizSignatureId
	if (memcmp(buffer, kMfsSignatureId, kSignatureIdSize) == 0) {
		hasValidSignatureId_ = true;
		chunkTypeId_ = ChunkType::getStandardChunkType().chunkTypeId();
	} else if (memcmp(buffer, kLizSignatureId, kSignatureIdSize) == 0) {
		hasValidSignatureId_ = true;
		chunkTypeId_ = get8bit(&ptr);
	} else {
		hasValidSignatureId_ = false;
		chunkTypeId_ = ChunkType::getStandardChunkType().chunkTypeId();
	}
	return true;
}
