#ifndef _LIZARDFS_MFSCOMMON_CLTOCS_COMMUNICATION_H_
#define _LIZARDFS_MFSCOMMON_CLTOCS_COMMUNICATION_H_

#include "mfscommon/chunk_type.h"
#include "mfscommon/packet.h"

namespace cltocs {

namespace read {

void serialize(std::vector<uint8_t>& buffer, uint64_t chunkId, uint32_t chunkVersion,
		const ChunkType& chunkType, uint32_t readOffset, uint32_t readSize) {
	serializePacket(buffer, LIZ_CLTOCS_READ, 0, chunkId, chunkVersion, chunkType, readOffset,
			readSize);
}

void deserialize(const std::vector<uint8_t>& buffer, uint64_t& chunkId, uint32_t& chunkVersion,
		ChunkType& chunkType, uint32_t& readOffset, uint32_t& readSize) {
	deserializePacketDataNoHeader(buffer, chunkId, chunkVersion,
			chunkType, readOffset, readSize);
}

}

}

#endif /*_LIZARDFS_MFSCOMMON_CLTOCS_COMMUNICATION_H_*/
