#ifndef LIZARDFS_COMMON_CHUNK_TYPE_H_
#define LIZARDFS_COMMON_CHUNK_TYPE_H_

#include <cstdint>

namespace lizardfs {

class ChunkType {
public:
	typedef uint8_t XorLevel;
	typedef uint8_t XorPart;

	static const uint8_t StandardChunkType = 0;

	static ChunkType getStandardChunkType();
	static ChunkType getXorChunkType(XorLevel level, XorPart part);

	bool isStandardChunkType();
	bool isXorChunkType();
	bool isXorParity();

	XorLevel getXorLevel();
	XorPart getXorPart();

private:
	ChunkType(uint8_t chunkType);

	static const uint8_t XorParityPart = 0;

	// just one 8b number to save space
	// (this class will be stored in RAM of master)
	uint8_t chunkType_;
};

} // namespace lizardfs

#endif // LIZARDFS_COMMON_CHUNK_TYPE_H_
