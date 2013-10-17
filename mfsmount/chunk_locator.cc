#include "mfsmount/chunk_locator.h"

#include "mfscommon/MFSCommunication.h"
#include "mfscommon/mfsstrerr.h"
#include "mfscommon/strerr.h"
#include "mfsmount/exceptions.h"
#include "mfsmount/mastercomm.h"

#include <map>


ChunkLocator::ChunkLocator()
		: inode_(0), index_(0), chunkId_(0), version_(0), fileLength_(0) {
}

bool ChunkLocator::isChunkEmpty() const {
	return chunkId_ == 0;
}

void MountChunkLocator::locateChunk(uint32_t inode, uint32_t index) {
	inode_ = inode;
	index_ = index;
	locations_.clear();
#ifdef USE_LEGACY_READ_MESSAGES
	const uint8_t *chunkserversData;
	uint32_t chunkserversDataSize;
	uint8_t status = fs_readchunk(inode_, index_, &fileLength_, &chunkId_, &version_,
			&chunkserversData, &chunkserversDataSize);
#else
	uint8_t status = fs_liz_readchunk(locations_, chunkId_, version_, fileLength_, inode_, index_);
#endif

	if (status != 0) {
		if (status == ERROR_ENOENT) {
			throw UnrecoverableReadError("Chunk locator: error sent by master server", status);
		} else {
			throw RecoverableReadError("Chunk locator: error sent by master server", status);
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
			locations_.push_back(
					ChunkTypeWithAddress(ip, port, ChunkType::getStandardChunkType()));
		}
	}
#endif
}
