#ifndef CHUNK_H_
#define CHUNK_H_

#include <cstdint>
#include <cstdlib>

#include "mfscommon/chunk_type.h"
#include "mfscommon/MFSCommunication.h"

enum ChunkState {
	CH_AVAIL,
	CH_LOCKED,
	CH_DELETED,
	CH_TOBEDELETED,
};

struct cntcond {
	pthread_cond_t cond;
	uint32_t wcnt;
	struct cntcond *next;
};

class Chunk {
public:
	static const size_t kMaxSignatureBlockSize = 1024;
	static const size_t kMaxCrcBlokSize = MFSBLOCKSINCHUNK * sizeof(uint32_t);
	static const size_t kMaxPaddingBlockSize = 4096;
	static const size_t kMaxHeaderSize =
			kMaxSignatureBlockSize + kMaxCrcBlokSize + kMaxPaddingBlockSize;

	char *filename;
	uint64_t chunkid;
	struct folder *owner;
	uint32_t version;
	uint16_t blocks;
	uint16_t crcrefcount;
	uint8_t opensteps;
	uint8_t crcsteps;
	uint8_t crcchanged;
	ChunkState state;
	cntcond *ccond;
	uint8_t *crc;
	int fd;
	uint8_t validattr;
	uint8_t todel;
	struct Chunk *testnext,**testprev;
	struct Chunk *next;

	Chunk(uint64_t chunkId, ChunkType type, ChunkState state);

	ChunkType type() const {
		return type_;
	}

	uint32_t maxBlocksInFile() const;
	off_t getSignatureOffset() const;
	size_t getHeaderSize() const;
	off_t getCrcOffset() const;
	size_t getCrcSize() const;
	off_t getDataBlockOffset(uint32_t blockNumber) const;
	bool isFileSizeValid(off_t fileSize) const;
	off_t getFileSizeFromBlockCount(uint32_t blockCount) const;
	void setBlockCountFromFizeSize(off_t fileSize);

private:
	ChunkType type_;
};

#endif /* CHUNK_H_ */
