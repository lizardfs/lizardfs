#ifndef LIZARDFS_MFSMOUNT_CHUNK_LOCATOR_H_
#define LIZARDFS_MFSMOUNT_CHUNK_LOCATOR_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "common/chunk_type_with_address.h"

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
	WriteChunkLocator()
		: inode_(0),
		  index_(0),
		  isChunkLocked_(false),
		  locationInfo_(0, 0, 0, {}) {
	}

	void locateAndLockChunk(uint32_t inode, uint32_t index);
	void unlockChunk();

	void updateFileLength(uint64_t fileLength) {
		locationInfo_. fileLength = fileLength;
	}

	const ChunkLocationInfo& locationInfo() const {
		return locationInfo_;
	}

	~WriteChunkLocator() {
		try {
			if (isChunkLocked_) {
				unlockChunk();
			}
		} catch (...) {
		}
	}

private:
	uint32_t inode_;
	uint32_t index_;
	bool isChunkLocked_;
	ChunkLocationInfo locationInfo_;
};

#endif // LIZARDFS_MFSMOUNT_CHUNK_LOCATOR_H_
