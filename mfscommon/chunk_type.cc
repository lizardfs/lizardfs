#include "mfscommon/chunk_type.h"
#include "mfscommon/goal.h"
#include "mfscommon/massert.h"

ChunkType::ChunkType(uint8_t chunkTypeId) : chunkTypeId_(chunkTypeId) {
}

ChunkType ChunkType::getStandardChunkType() {
	return ChunkType(ChunkType::StandardChunkTypeId);
}

ChunkType ChunkType::getXorChunkType(XorLevel level, XorPart part) {
	sassert(part <= level);
	sassert(level >= kMinXorLevel);
	sassert(level <= kMaxXorLevel);
	return ChunkType((kMaxXorLevel + 1) * level + part);
}

ChunkType ChunkType::getXorParityChunkType(XorLevel level) {
	sassert(level >= kMinXorLevel);
	sassert(level <= kMaxXorLevel);
	return ChunkType((kMaxXorLevel + 1) * level + ChunkType::XorParityPart);
}

bool ChunkType::isStandardChunkType() const {
	return chunkTypeId_ == ChunkType::StandardChunkTypeId;
}

bool ChunkType::isXorChunkType() const {
	return !isStandardChunkType();
}

bool ChunkType::isXorParity() const {
	sassert(isXorChunkType());
	return getXorPart() == ChunkType::XorParityPart;
}

uint8_t ChunkType::chunkTypeId() const {
	return chunkTypeId_;
}

ChunkType::XorLevel ChunkType::getXorLevel() const {
	sassert(isXorChunkType());
	return chunkTypeId_ / (kMaxXorLevel + 1);
}

ChunkType::XorPart ChunkType::getXorPart() const {
	sassert(isXorChunkType());
	return chunkTypeId_ % (kMaxXorLevel + 1);
}
