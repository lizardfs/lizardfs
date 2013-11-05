#ifndef LIZARDFS_MFSMOUNT_CHUNK_LOCATOR_H_
#define LIZARDFS_MFSMOUNT_CHUNK_LOCATOR_H_

#include <cstdint>
#include <vector>

#include "common/chunk_type_with_address.h"

class ChunkLocator {
public:
	typedef std::vector<ChunkTypeWithAddress> ChunkLocations;

	ChunkLocator();
	virtual ~ChunkLocator() {}
	uint64_t chunkId() const { return chunkId_; }
	uint64_t fileLength() const { return fileLength_; }
	uint32_t inode() const { return inode_; }
	uint32_t index() const { return index_; }
	bool isChunkEmpty() const;
	virtual void locateChunk(uint32_t inode, uint32_t index) = 0;
	const ChunkLocations& locations() const { return locations_; }
	uint32_t version() const { return version_; }

protected:
	uint32_t inode_;
	uint32_t index_;
	uint64_t chunkId_;
	uint32_t version_;
	uint64_t fileLength_;
	ChunkLocations locations_;
};

class MountChunkLocator : public ChunkLocator {
public:
	void locateChunk(uint32_t inode, uint32_t index);
};

#endif // LIZARDFS_MFSMOUNT_CHUNK_LOCATOR_H_
