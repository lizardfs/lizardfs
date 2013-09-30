#ifndef LIZARDFS_MFSCOMMON_CHUNK_TYPE_WITH_ADDRESS_H_
#define LIZARDFS_MFSCOMMON_CHUNK_TYPE_WITH_ADDRESS_H_

#include "mfscommon/chunk_type.h"
#include "mfscommon/serialization.h"

struct ChunkTypeWithAddress {
	ChunkTypeWithAddress(uint32_t ip, uint16_t port, const ChunkType& chunkType)
		: ip(ip), port(port), chunkType(chunkType) {
	}

	ChunkTypeWithAddress() :
		ip(0), port(0), chunkType(ChunkType::getStandardChunkType()) {
	}

	uint32_t ip;
	uint16_t port;
	ChunkType chunkType;
};

inline uint32_t serializedSize(const ChunkTypeWithAddress& chunkTypeWithAddress) {
	return serializedSize(chunkTypeWithAddress.ip, chunkTypeWithAddress.port,
			chunkTypeWithAddress.chunkType);
}

inline void serialize(uint8_t** destination, const ChunkTypeWithAddress& chunkTypeWithAddress) {
	serialize(destination, chunkTypeWithAddress.ip, chunkTypeWithAddress.port,
			chunkTypeWithAddress.chunkType);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkTypeWithAddress& chunkTypeWithAddress) {
	deserialize(source, bytesLeftInBuffer, chunkTypeWithAddress.ip, chunkTypeWithAddress.port,
			chunkTypeWithAddress.chunkType);
}

#endif // LIZARDFS_MFSCOMMON_CHUNK_TYPE_WITH_ADDRESS_H_
