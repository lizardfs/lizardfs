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
#include <memory>
#include <mutex>
#include <vector>

#include "common/chunk_type_with_address.h"
#include "common/slogger.h"

struct ChunkLocationInfo {
	typedef std::vector<ChunkTypeWithAddress> ChunkLocations;

	uint64_t chunkId;
	uint32_t version;
	uint64_t fileLength;
	ChunkLocations locations;

	ChunkLocationInfo()
			: chunkId(0),
			  version(0),
			  fileLength(0) {
	}

	ChunkLocationInfo(
			const uint64_t chunkId,
			const uint32_t version,
			const uint64_t fileLength,
			const ChunkLocations locations) :
		chunkId(chunkId),
		version(version),
		fileLength(fileLength),
		locations(locations) {
	}

	bool isEmptyChunk() const {
		return chunkId == 0;
	}
};

// Intended to be instantiated per descriptor.
// May cache locations of previously queried chunks.
// Thread safe.
class ReadChunkLocator {
public:
	ReadChunkLocator(const ReadChunkLocator&) = delete;
	ReadChunkLocator() {}

	std::shared_ptr<const ChunkLocationInfo> locateChunk(uint32_t inode, uint32_t index);
	void invalidateCache(uint32_t inode, uint32_t index);

private:
	uint32_t inode_;
	uint32_t index_;

	std::shared_ptr<const ChunkLocationInfo> cache_;
	std::mutex mutex_;
};

class WriteChunkLocator {
public:
	WriteChunkLocator() : inode_(0), index_(0), lockId_(0) {}

	virtual ~WriteChunkLocator() {
		try {
			if (lockId_) {
				unlockChunk();
			}
		} catch (Exception& ex) {
			lzfs_pretty_syslog(LOG_WARNING,
					"unlocking chunk error, inode: %" PRIu32 ", index: %" PRIu32 " - %s",
					inode_, index_, ex.what());
		}
	}

	virtual void locateAndLockChunk(uint32_t inode, uint32_t index);
	virtual void unlockChunk();

	uint32_t chunkIndex() {
		return index_;
	}

	void updateFileLength(uint64_t fileLength) {
		locationInfo_. fileLength = fileLength;
	}

	const ChunkLocationInfo& locationInfo() const {
		return locationInfo_;
	}

protected:
	WriteChunkLocator(uint32_t inode, uint32_t index, uint32_t lockId)
			: inode_(inode),
			  index_(index),
			  lockId_(lockId) {
	}

	uint32_t inode_;
	uint32_t index_;
	uint32_t lockId_;
	ChunkLocationInfo locationInfo_;
};

// Fit for truncating xor chunks down when master, not client, locks a chunk
class TruncateWriteChunkLocator : public WriteChunkLocator {
public:
	// Locator is created for single operation
	explicit TruncateWriteChunkLocator(uint32_t inode, uint32_t index, uint32_t lockId)
		: WriteChunkLocator(inode, index, lockId) {
	}

	~TruncateWriteChunkLocator() {
		// Remove information about the lock to prevent ~WriteChunkLocator from unlocking the chunk
		lockId_ = 0;
	}

	// In this case a chunk is unlocked by master so this locator will be simply destroyed
	void unlockChunk() {}
};
