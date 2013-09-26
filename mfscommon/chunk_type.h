#ifndef LIZARDFS_COMMON_CHUNK_TYPE_H_
#define LIZARDFS_COMMON_CHUNK_TYPE_H_

#include <cstdint>

#include "mfscommon/serialization.h"

class ChunkType {
public:
	typedef uint8_t XorLevel;
	typedef uint8_t XorPart;

	static ChunkType getStandardChunkType();
	static ChunkType getXorChunkType(XorLevel level, XorPart part);
	static ChunkType getXorParityChunkType(XorLevel level);

	bool isStandardChunkType() const;
	bool isXorChunkType() const;
	bool isXorParity() const;
	uint8_t chunkTypeId() const;
	XorLevel getXorLevel() const;
	XorPart getXorPart() const;

	bool operator==(const ChunkType& otherChunkType) const {
		return chunkTypeId_ == otherChunkType.chunkTypeId_;
	}

private:
	static const uint8_t StandardChunkTypeId = 0;
	static const uint8_t XorParityPart = 0;

	// just one 8b number to save space
	// (this class will be stored in RAM of master)
	uint8_t chunkTypeId_;

	explicit ChunkType(uint8_t chunkType);

	friend uint32_t serializedSize(const ChunkType&);
	friend void serialize(uint8_t **, const ChunkType&);
	friend void deserialize(const uint8_t**, uint32_t&, ChunkType&);
};

inline uint32_t serializedSize(const ChunkType& chunkType) {
	return serializedSize(chunkType.chunkTypeId_);
}

inline void serialize(uint8_t **destination, const ChunkType& chunkType) {
	serialize(destination, chunkType.chunkTypeId_);
}

inline void deserialize(const uint8_t **source, uint32_t& bytesLeftInBuffer, ChunkType& chunkType) {
	deserialize(source, bytesLeftInBuffer, chunkType.chunkTypeId_);
}

#endif // LIZARDFS_COMMON_CHUNK_TYPE_H_
