#include "config.h"
#include "chunkserver/chunk.h"

#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>
#include <sstream>

#include "common/massert.h"

Chunk::Chunk(uint64_t chunkId, ChunkType type, ChunkState state)
	: chunkid(chunkId),
	  owner(NULL),
	  version(0),
	  blocks(0),
	  crcrefcount(0),
	  opensteps(0),
	  crcsteps(0),
	  crcchanged(0),
	  state(state),
	  ccond(NULL),
	  crc(NULL),
	  fd(-1),
	  blockExpectedToBeReadNext(0),
	  validattr(0),
	  todel(0),
	  testnext(NULL),
	  testprev(NULL),
	  next(NULL),
	  type_(type) {
}

std::string Chunk::generateFilenameForVersion(uint32_t version) const {
	std::stringstream ss;
	char buffer[30];
	ss << owner->path << Chunk::getSubfolderNameGivenChunkId(chunkid) << "/chunk_";
	if (type_.isXorChunkType()) {
		if (type_.isXorParity()) {
			ss << "xor_parity_of_";
		} else {
			ss << "xor_" << (unsigned)type_.getXorPart() << "_of_";
		}
		ss << (unsigned)type_.getXorLevel() << "_";
	}
	sprintf(buffer, "%016" PRIX64 "_%08" PRIX32 ".mfs", chunkid, version);
	ss << buffer;
	return ss.str();
}

off_t Chunk::getCrcOffset() const {
	return 1024;
}

size_t Chunk::getCrcSize() const {
	return 4 * maxBlocksInFile();
}

uint32_t Chunk::getCrc(uint16_t blockNumber) const {
	sassert(blockNumber < blocks);
	const uint8_t *ptr = crc + (blockNumber * serializedSize(uint32_t()));
	return get32bit(&ptr);
}

off_t Chunk::getDataBlockOffset(uint16_t blockNumber) const {
	return getHeaderSize() + static_cast<uint32_t>(blockNumber) * MFSBLOCKSIZE;
}

off_t Chunk::getFileSizeFromBlockCount(uint32_t blockCount) const {
	return getHeaderSize() + blockCount * MFSBLOCKSIZE;
}

size_t Chunk::getHeaderSize() const {
	sassert(type_.isStandardChunkType() || type_.isXorChunkType());
	if (type_.isStandardChunkType()) {
		return 1024 + 4 * MFSBLOCKSINCHUNK;
	} else {
		uint32_t requiredHeaderSize = 1024 + 4 * maxBlocksInFile();
		// header size is equal to the requiredHeaderSize rounded up to typical disk block size
		uint32_t diskBlockSize = 4096; // 4 kB
		off_t dataOffset = (requiredHeaderSize + diskBlockSize - 1) / diskBlockSize * diskBlockSize;
		return dataOffset;
	}
}

void Chunk::readaheadHeader() const {
	posix_fadvise(fd, 0, getHeaderSize(), POSIX_FADV_WILLNEED);
}

off_t Chunk::getSignatureOffset() const {
	return 0;
}

bool Chunk::isFileSizeValid(off_t fileSize) const {
	if (fileSize < static_cast<off_t>(getHeaderSize())) {
		return false;
	}
	fileSize -= getHeaderSize();
	if (fileSize % MFSBLOCKSIZE != 0) {
		return false;
	}
	if (fileSize / MFSBLOCKSIZE > maxBlocksInFile()) {
		return false;
	}
	return true;
}

uint32_t Chunk::maxBlocksInFile() const {
	sassert(type_.isStandardChunkType() || type_.isXorChunkType());
	if (type_.isStandardChunkType()) {
		return MFSBLOCKSINCHUNK;
	} else {
		uint32_t xorLevel = type_.getXorLevel();
		return (MFSBLOCKSINCHUNK + xorLevel - 1) / xorLevel;
	}
}

int Chunk::renameChunkFile(const std::string& newFilename) {
	int status = rename(filename().c_str(), newFilename.c_str());
	if (status < 0) {
		return status;
	}
	setFilename(newFilename);
	return 0;
}

void Chunk::setBlockCountFromFizeSize(off_t fileSize) {
	sassert(isFileSizeValid(fileSize));
	fileSize -= getHeaderSize();
	blocks = fileSize / MFSBLOCKSIZE;
}

uint32_t Chunk::getSubfolderNumber(uint64_t chunkId) {
	return (chunkId >> 16) & 0xFF;
}

std::string Chunk::getSubfolderNameGivenNumber(uint32_t subfolderNumber) {
	sassert(subfolderNumber < Chunk::kNumberOfSubfolders);
	char buffer[16];
	sprintf(buffer, "chunks%02X", unsigned(subfolderNumber));
	return std::string(buffer);
}

std::string Chunk::getSubfolderNameGivenChunkId(uint64_t chunkId) {
	return Chunk::getSubfolderNameGivenNumber(Chunk::getSubfolderNumber(chunkId));
}
