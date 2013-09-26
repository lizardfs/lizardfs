#ifndef LIZARDFS_MFSCOMMON_CHUNKSERVER_HOLDING_PART_OF_CHUNK_H_
#define LIZARDFS_MFSCOMMON_CHUNKSERVER_HOLDING_PART_OF_CHUNK_H_

#include "mfscommon/chunk_type.h"
#include "mfscommon/serialization.h"

struct ChunkserverHoldingPartOfChunk {
	ChunkserverHoldingPartOfChunk(uint32_t ip, uint16_t port, const ChunkType& chunkType)
		: ip(ip), port(port), chunkType(chunkType) {
	}

	ChunkserverHoldingPartOfChunk() :
		ip(0), port(0), chunkType(ChunkType::getStandardChunkType()) {
	}

	uint32_t ip;
	uint16_t port;
	ChunkType chunkType;
};

inline uint32_t serializedSize(const ChunkserverHoldingPartOfChunk& chunkServer) {
	return serializedSize(chunkServer.ip, chunkServer.port, chunkServer.chunkType.chunkTypeId());
}

inline void serialize(uint8_t** destination, const ChunkserverHoldingPartOfChunk& chunkServer) {
	return serialize(destination, chunkServer.ip, chunkServer.port, chunkServer.chunkType);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkserverHoldingPartOfChunk& chunkServer) {
	deserialize(source, bytesLeftInBuffer, chunkServer.ip, chunkServer.port,
			chunkServer.chunkType);
}

#endif // LIZARDFS_MFSCOMMON_CHUNKSERVER_HOLDING_PART_OF_CHUNK_H_
