#include "mount/chunk_locator.h"

#include "common/MFSCommunication.h"
#include "common/mfsstrerr.h"
#include "common/strerr.h"
#include "mount/exceptions.h"
#include "mount/mastercomm.h"


void ReadChunkLocator::invalidateCache(uint32_t inode, uint32_t index) {
	std::unique_lock<std::mutex> lock(mutex_);
	if (cache_ && inode == inode_ && index == index_) {
		cache_ = nullptr;
	}
}

std::shared_ptr<const ChunkLocationInfo> ReadChunkLocator::locateChunk(uint32_t inode, uint32_t index) {
	{
		std::unique_lock<std::mutex> lock(mutex_);
		if (cache_ && inode == inode_ && index == index_) {
			return cache_;
		}
	}
	uint64_t chunkId;
	uint32_t version;
	uint64_t fileLength;
	ChunkLocationInfo::ChunkLocations locations;
#ifdef USE_LEGACY_READ_MESSAGES
	const uint8_t *chunkserversData;
	uint32_t chunkserversDataSize;
	uint8_t status = fs_readchunk(inode, index, &fileLength, &chunkId, &version,
			&chunkserversData, &chunkserversDataSize);
#else
	uint8_t status = fs_lizreadchunk(locations, chunkId, version, fileLength, inode, index);
#endif

	if (status != 0) {
		if (status == ERROR_ENOENT) {
			throw UnrecoverableReadException("Chunk locator: error sent by master server", status);
		} else {
			throw RecoverableReadException("Chunk locator: error sent by master server", status);
		}
	}

#ifdef USE_LEGACY_READ_MESSAGES
	if (chunkserversData != NULL) {
		uint32_t ip;
		uint16_t port;
		uint32_t entrySize = serializedSize(ip, port);
		for (const uint8_t *rptr = chunkserversData;
				rptr < chunkserversData + chunkserversDataSize;
				rptr += entrySize) {
			deserialize(rptr, entrySize, ip, port);
			locations.push_back(
					ChunkTypeWithAddress(NetworkAddress(ip, port),
						ChunkType::getStandardChunkType()));
		}
	}
#endif
	{
		std::unique_lock<std::mutex> lock(mutex_);
		inode_ = inode;
		index_ = index;
		cache_ = std::make_shared<ChunkLocationInfo>(chunkId, version, fileLength, locations);
		return cache_;
	}
}

void WriteChunkLocator::locateAndLockChunk(uint32_t inode, uint32_t index) {
	uint8_t status = fs_lizwritechunk(inode, index, locationInfo_.fileLength,
			locationInfo_.chunkId, locationInfo_.version, locationInfo_.locations);
	if (status != STATUS_OK) {
		if (status == ERROR_ENOENT || status == ERROR_QUOTA || status == ERROR_NOSPACE) {
			throw UnrecoverableWriteException("Chunk locator: error sent by master server", status);
		} else {
			throw RecoverableWriteException("Chunk locator: error sent by master server", status);
		}
	}
	inode_ = inode;
	index_ = index;
	isChunkLocked_ = true;
}

void WriteChunkLocator::unlockChunk() {
	if (!isChunkLocked_) {
		return;
	}
	uint8_t status = fs_writeend(locationInfo_.chunkId, inode_, locationInfo_.fileLength);
	if (status == STATUS_OK) {
		isChunkLocked_ = false;
	} else {
		throw RecoverableWriteException("Sending WRITE_END to the master failed", status);
	}
}
