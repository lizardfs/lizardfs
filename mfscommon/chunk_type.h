#ifndef LIZARDFS_MFSCOMMON_CHUNK_TYPE_H_
#define LIZARDFS_MFSCOMMON_CHUNK_TYPE_H_

#include <cstdint>
#include <exception>

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
	static bool validChunkTypeID(uint8_t chunkTypeId);

	bool operator==(const ChunkType& otherChunkType) const {
		return chunkTypeId_ == otherChunkType.chunkTypeId_;
	}

	bool operator<(const ChunkType& otherChunkType) const {
		return chunkTypeId_ < otherChunkType.chunkTypeId_;
	}

private:
	static const uint8_t kStandardChunkTypeId = 0;
	static const uint8_t kXorParityPart = 0;

	// Just one 8 bytes to save space (this class will be stored in RAM of master)
	uint8_t chunkTypeId_;

	explicit ChunkType(uint8_t chunkType) : chunkTypeId_(chunkType) {
	}

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

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, ChunkType& chunkType) {
	uint8_t chunkTypeId;
	deserialize(source, bytesLeftInBuffer, chunkTypeId);
	if (ChunkType::validChunkTypeID(chunkTypeId)) {
		chunkType.chunkTypeId_ = chunkTypeId;
	} else {
		throw IncorrectDeserializationException();
	}
}

#endif // LIZARDFS_MFSCOMMON_CHUNK_TYPE_H_
