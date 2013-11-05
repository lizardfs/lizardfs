#ifndef LIZARDFS_MFSCOMMON_CHUNK_WITH_VERSION_H_
#define LIZARDFS_MFSCOMMON_CHUNK_WITH_VERSION_H_

#include <cstdint>

#include "common/serialization.h"

struct ChunkWithVersion {
	uint64_t id;
	uint32_t version;
};
inline uint32_t serializedSize(const ChunkWithVersion& chunk) {
	return serializedSize(chunk.id, chunk.version);
}
inline void serialize(uint8_t** destination, const ChunkWithVersion& chunk) {
	return serialize(destination, chunk.id, chunk.version);
}
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkWithVersion& chunk) {
	return deserialize(source, bytesLeftInBuffer, chunk.id, chunk.version);
}

#endif // LIZARDFS_MFSCOMMON_CHUNK_WITH_VERSION_H_
