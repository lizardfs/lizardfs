#ifndef LIZARDFS_COMMON_CHUNK_TYPE_H_
#define LIZARDFS_COMMON_CHUNK_TYPE_H_

#include <cstdint>

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

private:
	ChunkType(uint8_t chunkType);

	static const uint8_t StandardChunkTypeId = 0;
	static const uint8_t XorParityPart = 0;

	// just one 8b number to save space
	// (this class will be stored in RAM of master)
	uint8_t chunkTypeId_;
};

#endif // LIZARDFS_COMMON_CHUNK_TYPE_H_
