#ifndef LIZARDFS_MFSCHUNKSERVER_CHUNK_H_
#define LIZARDFS_MFSCHUNKSERVER_CHUNK_H_

#include <cstdint>
#include <cstdlib>
#include <string>

#include "mfscommon/chunk_type.h"
#include "mfscommon/MFSCommunication.h"

#define STATSHISTORY (24*60)
#define LASTERRSIZE 30

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

struct hddstats {
	uint64_t rbytes;
	uint64_t wbytes;
	uint64_t usecreadsum;
	uint64_t usecwritesum;
	uint64_t usecfsyncsum;
	uint32_t rops;
	uint32_t wops;
	uint32_t fsyncops;
	uint32_t usecreadmax;
	uint32_t usecwritemax;
	uint32_t usecfsyncmax;
};

struct ioerror {
	uint64_t chunkid;
	uint32_t timestamp;
	int errornumber;
};

struct folder {
	char *path;
#define SCST_SCANNEEDED 0u
#define SCST_SCANINPROGRESS 1u
#define SCST_SCANTERMINATE 2u
#define SCST_SCANFINISHED 3u
#define SCST_SENDNEEDED 4u
#define SCST_WORKING 5u
	unsigned int scanstate:3;
	unsigned int needrefresh:1;
	unsigned int todel:2;
	unsigned int damaged:1;
	unsigned int toremove:2;
	uint8_t scanprogress;
	uint64_t sizelimit;
	uint64_t leavefree;
	uint64_t avail;
	uint64_t total;
	hddstats cstat;
	hddstats stats[STATSHISTORY];
	uint32_t statspos;
	ioerror lasterrtab[LASTERRSIZE];
	uint32_t chunkcount;
	uint32_t lasterrindx;
	uint32_t lastrefresh;
	dev_t devid;
	ino_t lockinode;
	int lfd;
	double carry;
	pthread_t scanthread;
	struct Chunk *testhead,**testtail;
	struct folder *next;
};

class Chunk {
public:
	static const size_t kMaxSignatureBlockSize = 1024;
	static const size_t kMaxCrcBlockSize = MFSBLOCKSINCHUNK * sizeof(uint32_t);
	static const size_t kMaxPaddingBlockSize = 4096;
	static const size_t kMaxHeaderSize =
			kMaxSignatureBlockSize + kMaxCrcBlockSize + kMaxPaddingBlockSize;

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
	const std::string& filename() const { return filename_; };
	off_t getCrcOffset() const;
	size_t getCrcSize() const;
	off_t getDataBlockOffset(uint32_t blockNumber) const;
	off_t getFileSizeFromBlockCount(uint32_t blockCount) const;
	size_t getHeaderSize() const;
	off_t getSignatureOffset() const;
	bool isFileSizeValid(off_t fileSize) const;
	uint32_t maxBlocksInFile() const;
	std::string generateFilenameForVersion(uint32_t version) const;
	int renameChunkFile(const std::string& newFilename);
	void setBlockCountFromFizeSize(off_t fileSize);
	void setFilename(const std::string& filename) { filename_ = filename; }
	ChunkType type() const { return type_; }

private:
	ChunkType type_;
	std::string filename_;
};

#endif // LIZARDFS_MFSCHUNKSERVER_CHUNK_H_
