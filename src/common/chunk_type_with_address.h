#pragma once

#include "common/chunk_type.h"
#include "common/network_address.h"
#include "common/serialization.h"

struct ChunkTypeWithAddress {
	NetworkAddress address;
	ChunkType chunkType;

	ChunkTypeWithAddress() :
		chunkType(ChunkType::getStandardChunkType()) {
	}

	ChunkTypeWithAddress(const NetworkAddress& address, const ChunkType& chunkType)
		: address(address), chunkType(chunkType) {
	}

	bool operator==(const ChunkTypeWithAddress& other) const {
		return std::make_pair(address, chunkType) == std::make_pair(other.address, other.chunkType);
	}

	bool operator<(const ChunkTypeWithAddress& other) const {
		return std::make_pair(address, chunkType) < std::make_pair(other.address, other.chunkType);
	}
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
