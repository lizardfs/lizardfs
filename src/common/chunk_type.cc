#include "config.h"
#include "common/chunk_type.h"

#include "common/goal.h"
#include "common/massert.h"

bool ChunkType::validChunkTypeID(uint8_t chunkTypeId) {
	if (chunkTypeId == ChunkType::kStandardChunkTypeId) {
		return true;
	}
	uint8_t xorLevel = chunkTypeId / (kMaxXorLevel + 1);
	if (xorLevel < kMinXorLevel || xorLevel > kMaxXorLevel) {
		return false;
	}
	uint8_t xorPart = chunkTypeId % (kMaxXorLevel + 1);
	if (xorPart == ChunkType::kXorParityPart) {
		return true;
	}
	if (xorPart < 1 || xorPart > xorLevel) {
		return false;
	}
	return true;
}

ChunkType ChunkType::getStandardChunkType() {
	return ChunkType(ChunkType::kStandardChunkTypeId);
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
	return ChunkType((kMaxXorLevel + 1) * level + ChunkType::kXorParityPart);
}

bool ChunkType::isStandardChunkType() const {
	return chunkTypeId_ == ChunkType::kStandardChunkTypeId;
}

bool ChunkType::isXorChunkType() const {
	return !isStandardChunkType();
}

bool ChunkType::isXorParity() const {
	sassert(isXorChunkType());
	return getXorPart() == ChunkType::kXorParityPart;
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
