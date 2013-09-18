#include "chunk.h"

#include "mfscommon/massert.h"

Chunk::Chunk(uint64_t chunkId, ChunkType type, ChunkState state)
	: filename(NULL),
	  chunkid(chunkId),
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
	  validattr(0),
	  todel(0),
	  testnext(NULL),
	  testprev(NULL),
	  next(NULL),
	  type_(type) {
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

off_t Chunk::getSignatureOffset() const {
	return 0;
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

off_t Chunk::getCrcOffset() const {
	return 1024;
}

size_t Chunk::getCrcSize() const {
	return 4 * maxBlocksInFile();
}

off_t Chunk::getDataBlockOffset(uint32_t blockNumber) const {
	return getHeaderSize() + blockNumber * MFSBLOCKSIZE;
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

off_t Chunk::getFileSizeFromBlockCount(uint32_t blockCount) const {
	return getHeaderSize() + blockCount * MFSBLOCKSIZE;
}

void Chunk::setBlockCountFromFizeSize(off_t fileSize) {
	sassert(isFileSizeValid(fileSize));
	fileSize -= getHeaderSize();
	blocks = fileSize / MFSBLOCKSIZE;
}
