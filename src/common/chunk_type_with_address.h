#ifndef LIZARDFS_MFSCOMMON_CHUNK_TYPE_WITH_ADDRESS_H_
#define LIZARDFS_MFSCOMMON_CHUNK_TYPE_WITH_ADDRESS_H_

#include "common/chunk_type.h"
#include "common/network_address.h"
#include "common/serialization.h"

struct ChunkTypeWithAddress {
	ChunkTypeWithAddress(const NetworkAddress& address, const ChunkType& chunkType)
		: address(address), chunkType(chunkType) {
	}

	ChunkTypeWithAddress() :
		chunkType(ChunkType::getStandardChunkType()) {
	}

	NetworkAddress address;
	ChunkType chunkType;
};

inline uint32_t serializedSize(const ChunkTypeWithAddress& chunkTypeWithAddress) {
	return serializedSize(chunkTypeWithAddress.address, chunkTypeWithAddress.chunkType);
}

inline void serialize(uint8_t** destination, const ChunkTypeWithAddress& chunkTypeWithAddress) {
	serialize(destination, chunkTypeWithAddress.address, chunkTypeWithAddress.chunkType);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkTypeWithAddress& chunkTypeWithAddress) {
	deserialize(source, bytesLeftInBuffer, chunkTypeWithAddress.address,
			chunkTypeWithAddress.chunkType);
}

#endif // LIZARDFS_MFSCOMMON_CHUNK_TYPE_WITH_ADDRESS_H_
