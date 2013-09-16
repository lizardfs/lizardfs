#include "mfscommon/chunk_type.h"
#include "mfscommon/goal.h"
#include "mfscommon/massert.h"

ChunkType::ChunkType(uint8_t chunkType) : chunkType_(chunkType) {
}

ChunkType ChunkType::getStandardChunkType() {
	return ChunkType(ChunkType::StandardChunkType);
}

ChunkType ChunkType::getXorChunkType(XorLevel level, XorPart part) {
	eassert(part <= level);
	eassert(isValidXorGoal(level));
	return ChunkType((MaxXorLevel + 1) * level + part);
}

bool ChunkType::isStandardChunkType() {
	return chunkType_ == ChunkType::StandardChunkType;
}

bool ChunkType::isXorChunkType() {
	return !isStandardChunkType();
}

bool ChunkType::isXorParity() {
	eassert(isXorChunkType());
	return getXorPart() == ChunkType::XorParityPart;
}

ChunkType::XorLevel ChunkType::getXorLevel() {
	eassert(isXorChunkType());
	return chunkType_ / (MaxXorLevel + 1);
}

ChunkType::XorPart ChunkType::getXorPart() {
	eassert(isXorChunkType());
	return chunkType_ % (MaxXorLevel + 1);
}
