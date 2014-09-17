#include "common/platform.h"
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
	  refcount(0),
	  opensteps(0),
	  wasChanged(false),
	  state(state),
	  ccond(NULL),
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
	sprintf(buffer, "%016" PRIX64 "_%08" PRIX32 ".liz", chunkid, version);
	ss << buffer;
	return ss.str();
}

off_t Chunk::getDataBlockOffset(uint16_t blockNumber) const {
	return static_cast<uint32_t>(blockNumber) * kHddBlockSize + 4;
}
off_t Chunk::getCrcAndDataBlockOffset(uint16_t blockNumber) const {
	return static_cast<uint32_t>(blockNumber) * kHddBlockSize;
}

off_t Chunk::getFileSizeFromBlockCount(uint32_t blockCount) const {
	return blockCount * kHddBlockSize;
}

bool Chunk::isFileSizeValid(off_t fileSize) const {
	return fileSize % kHddBlockSize == 0;
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
	blocks = fileSize / kHddBlockSize;
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
