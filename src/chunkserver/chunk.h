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

#pragma once

#include "common/platform.h"

#include <cstdint>
#include <cstdlib>
#include <string>
#include <sys/types.h>

#include "chunkserver/chunk_format.h"
#include "common/chunk_part_type.h"
#include "common/disk_info.h"
#include "protocol/MFSCommunication.h"

#define STATSHISTORY (24*60)
#define LASTERRSIZE 30

// Block data and crc summaric size.
constexpr uint32_t kHddBlockSize = MFSBLOCKSIZE + 4;

enum ChunkState {
	CH_AVAIL,
	CH_LOCKED,
	CH_DELETED,
	CH_TOBEDELETED
};

class Chunk;

struct cntcond {
	pthread_cond_t cond;
	uint32_t wcnt;
	Chunk *owner;
	struct cntcond *next;
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
#define MGST_MIGRATEDONE 0u
#define MGST_MIGRATEINPROGRESS 1u
#define MGST_MIGRATETERMINATE 2u
#define MGST_MIGRATEFINISHED 3u
	uint8_t migratestate;
	uint64_t sizelimit;
	uint64_t leavefree;
	uint64_t avail;
	uint64_t total;
	HddStatistics cstat;
	HddStatistics stats[STATSHISTORY];
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
	pthread_t migratethread;
	Chunk *testhead,**testtail;
	struct folder *next;
};

class Chunk {
public:
	static const uint32_t kNumberOfSubfolders = 256;

	Chunk(uint64_t chunkId, ChunkPartType type, ChunkState state, ChunkFormat format);
	virtual ~Chunk() {};
	const std::string& filename() const { return filename_; };
	virtual off_t getBlockOffset(uint16_t blockNumber) const = 0;
	virtual off_t getFileSizeFromBlockCount(uint32_t blockCount) const = 0;
	virtual bool isFileSizeValid(off_t fileSize) const = 0;
	uint32_t maxBlocksInFile() const;
	std::string generateFilenameForVersion(uint32_t version, int layout_version = 0) const;
	int renameChunkFile(const std::string& newFilename);
	virtual void setBlockCountFromFizeSize(off_t fileSize) = 0;
	void setFilename(const std::string& filename) { filename_ = filename; }
	ChunkPartType type() const { return type_; }
	ChunkFormat chunkFormat() const { return chunkFormat_; };
	static uint32_t getSubfolderNumber(uint64_t chunkId, int layout_version = 0);
	static std::string getSubfolderNameGivenNumber(uint32_t subfolderNumber, int layout_version = 0);
	static std::string getSubfolderNameGivenChunkId(uint64_t chunkId, int layout_version = 0);
	bool isLocked() const { return state == CH_LOCKED; }

	uint64_t chunkid;
	struct folder *owner;
	uint32_t version;
	uint16_t blocks;
	uint16_t refcount;
	bool wasChanged;
	ChunkState state;
	cntcond *ccond;
	int fd;
	uint16_t blockExpectedToBeReadNext;
	uint8_t validattr;
	uint8_t todel;
	Chunk *testnext, **testprev;
	Chunk *next;

protected:
	ChunkPartType type_;
	std::string filename_;

private:
	ChunkFormat chunkFormat_;

};

class MooseFSChunk : public Chunk {
public:
	static const size_t kMaxSignatureBlockSize = 1024;
	static const size_t kMaxCrcBlockSize = MFSBLOCKSINCHUNK * sizeof(uint32_t);
	static const size_t kMaxPaddingBlockSize = 4096;
	static const size_t kMaxHeaderSize =
			kMaxSignatureBlockSize + kMaxCrcBlockSize + kMaxPaddingBlockSize;
	static const size_t kDiskBlockSize = 4096; // 4kB

	typedef std::array<uint8_t, kMaxCrcBlockSize> CrcDataContainer;

	MooseFSChunk(uint64_t chunkId, ChunkPartType type, ChunkState state);
	off_t getBlockOffset(uint16_t blockNumber) const override;
	off_t getFileSizeFromBlockCount(uint32_t blockCount) const override;
	bool isFileSizeValid(off_t fileSize) const override;
	void setBlockCountFromFizeSize(off_t fileSize) override;
	off_t getSignatureOffset() const;
	void readaheadHeader() const;
	size_t getHeaderSize() const;
	off_t getCrcOffset() const;
	size_t getCrcBlockSize() const;
};

class InterleavedChunk : public Chunk {
public:
	InterleavedChunk(uint64_t chunkId, ChunkPartType type, ChunkState state);
	off_t getBlockOffset(uint16_t blockNumber) const override;
	off_t getFileSizeFromBlockCount(uint32_t blockCount) const override;
	bool isFileSizeValid(off_t fileSize) const override;
	void setBlockCountFromFizeSize(off_t fileSize) override;
};

#define IF_MOOSEFS_CHUNK(mc, chunk) \
	if (MooseFSChunk *mc = dynamic_cast<MooseFSChunk *>(chunk))

#define IF_INTERLEAVED_CHUNK(lc, chunk) \
	if (InterleavedChunk *lc = dynamic_cast<InterleavedChunk *>(chunk))
