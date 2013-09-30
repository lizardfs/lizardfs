#ifndef LIZARDFS_MFSCOMMON_CHUNK_WITH_VERSION_AND_TYPE_H_
#define LIZARDFS_MFSCOMMON_CHUNK_WITH_VERSION_AND_TYPE_H_

#include <cstdint>

#include "mfscommon/chunk_type.h"
#include "mfscommon/serialization.h"

struct ChunkWithVersionAndType {
	uint64_t id;
	uint32_t version;
	ChunkType type;
	ChunkWithVersionAndType(
			uint64_t id,
			uint32_t version,
			ChunkType type)
		: id(id), version(version), type(type) {
	}
	ChunkWithVersionAndType() : id(0), version(0), type(ChunkType::getStandardChunkType()) {
	}
};
inline uint32_t serializedSize(const ChunkWithVersionAndType& chunk) {
	return serializedSize(chunk.id, chunk.version, chunk.type);
}
inline void serialize(uint8_t** destination, const ChunkWithVersionAndType& chunk) {
	return serialize(destination, chunk.id, chunk.version, chunk.type);
}
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkWithVersionAndType& chunk) {
	return deserialize(source, bytesLeftInBuffer, chunk.id, chunk.version, chunk.type);
}

#endif // LIZARDFS_MFSCOMMON_CHUNK_WITH_VERSION_AND_TYPE_H_
