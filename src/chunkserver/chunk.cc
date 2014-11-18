#include "common/platform.h"
#include "chunkserver/chunk.h"

#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>
#include <sstream>

#include "common/massert.h"

Chunk::Chunk(uint64_t chunkId, ChunkType type, ChunkState state, ChunkFormat format)
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
	  type_(type),
	  chunkFormat_(format){
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
	if (chunkFormat() == ChunkFormat::INTERLEAVED) {
		memcpy(buffer + 26, "liz", 3);
	}
	ss << buffer;
	return ss.str();
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

MooseFSChunk::MooseFSChunk(uint64_t chunkId, ChunkType type, ChunkState state) :
		Chunk(chunkId, type, state, ChunkFormat::MOOSEFS),
		crc(nullptr),
		crcsteps(0) {
}

off_t MooseFSChunk::getBlockOffset(uint16_t blockNumber) const {
	return getHeaderSize() + MFSBLOCKSIZE * blockNumber;
}

off_t MooseFSChunk::getFileSizeFromBlockCount(uint32_t blockCount) const {
	return getHeaderSize() + blockCount * MFSBLOCKSIZE;
}

void MooseFSChunk::setBlockCountFromFizeSize(off_t fileSize) {
	sassert(isFileSizeValid(fileSize));
	fileSize -= getHeaderSize();
	blocks = fileSize / MFSBLOCKSIZE;
}

bool MooseFSChunk::isFileSizeValid(off_t fileSize) const {
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

off_t MooseFSChunk::getSignatureOffset() const {
	return 0;
}

void MooseFSChunk::readaheadHeader() const {
	posix_fadvise(fd, 0, getHeaderSize(), POSIX_FADV_WILLNEED);
}

size_t MooseFSChunk::getHeaderSize() const {
	sassert(type_.isStandardChunkType() || type_.isXorChunkType());
	if (type_.isStandardChunkType()) {
		return kMaxSignatureBlockSize + serializedSize(uint32_t()) * maxBlocksInFile();
	} else {
		uint32_t requiredHeaderSize = kMaxSignatureBlockSize + serializedSize(uint32_t()) * maxBlocksInFile();
		// header size is equal to the requiredHeaderSize rounded up to typical disk block size
		uint32_t diskBlockSize = kDiskBlockSize;
		off_t dataOffset = (requiredHeaderSize + diskBlockSize - 1) / diskBlockSize * diskBlockSize;
		return dataOffset;
	}
}

off_t MooseFSChunk::getCrcOffset() const {
	return kMaxSignatureBlockSize;
}

size_t MooseFSChunk::getCrcBlockSize() const {
	return serializedSize(uint32_t()) * maxBlocksInFile();
}

uint8_t* MooseFSChunk::getCrcBuffer(uint16_t blockNumber) {
	sassert(blockNumber < blocks);
	return crc->data() + (blockNumber * serializedSize(uint32_t()));
}

const uint8_t* MooseFSChunk::getCrcBuffer(uint16_t blockNumber) const {
	sassert(blockNumber < blocks);
	return crc->data() + (blockNumber * serializedSize(uint32_t()));
}

uint32_t MooseFSChunk::getCrc(uint16_t blockNumber) const {
	const uint8_t *ptr = getCrcBuffer(blockNumber);
	return get32bit(&ptr);
}

void MooseFSChunk::initEmptyCrc() {
	crc.reset(new std::array<uint8_t, kMaxCrcBlockSize>);
	memset(crc->data(), 0, getCrcBlockSize());
}

void MooseFSChunk::clearCrc() {
	crc.reset();
	crcsteps = 0;
	if (wasChanged) {
		syslog(LOG_ERR, "serious error: crc changes lost (chunk:%016" PRIX64 "_%08" PRIX32 ")",
				chunkid, version);
	}
}

InterleavedChunk::InterleavedChunk(uint64_t chunkId, ChunkType type, ChunkState state) :
		Chunk(chunkId, type, state, ChunkFormat::INTERLEAVED) {
}

off_t InterleavedChunk::getBlockOffset(uint16_t blockNumber) const {
	return static_cast<uint32_t>(blockNumber) * kHddBlockSize;
}

off_t InterleavedChunk::getFileSizeFromBlockCount(uint32_t blockCount) const {
	return blockCount * kHddBlockSize;
}

void InterleavedChunk::setBlockCountFromFizeSize(off_t fileSize) {
	sassert(isFileSizeValid(fileSize));
	blocks = fileSize / kHddBlockSize;
}

bool InterleavedChunk::isFileSizeValid(off_t fileSize) const {
	return fileSize % kHddBlockSize == 0;
}
