#include "common/chunk_type.h"

#include <gtest/gtest.h>
#include <vector>

#include "common/goal.h"

TEST(ChunkTypeTests, SerializeDeserialize) {
	// Create array with all chunk types
	std::vector<ChunkType> allChunkTypes = { ChunkType::getStandardChunkType() };
	for (ChunkType::XorLevel level = kMinXorLevel; level < kMaxXorLevel; ++level) {
		for (ChunkType::XorPart part = 1; part <= level; ++part) {
			allChunkTypes.push_back(ChunkType::getXorChunkType(level, part));
		}
		allChunkTypes.push_back(ChunkType::getXorParityChunkType(level));
	}

	std::vector<uint8_t> buffer;
	ChunkType deserializedChunkType = ChunkType::getXorParityChunkType(2);
	for (ChunkType chunkType : allChunkTypes) {
		buffer.clear();
		serialize(buffer, chunkType);
		deserialize(buffer, deserializedChunkType);
		EXPECT_EQ(chunkType, deserializedChunkType);
	}
}

TEST(ChunkTypeTests, validChunkTypeIDTest) {
	std::vector<bool> chunkIDValidity(256, false);
	chunkIDValidity[ChunkType::getStandardChunkType().chunkTypeId()] = true;
	for (uint32_t xorLevel = kMinXorLevel; xorLevel <= kMaxXorLevel; ++xorLevel) {
		chunkIDValidity[ChunkType::getXorParityChunkType(xorLevel).chunkTypeId()] = true;
		for (uint32_t xorPart = 1; xorPart <= xorLevel; ++xorPart) {
			chunkIDValidity[ChunkType::getXorChunkType(xorLevel, xorPart).chunkTypeId()] = true;
		}
	}
	for (uint id = 0; id < 256; ++id) {
		SCOPED_TRACE("ID: " + std::to_string(id));
		EXPECT_EQ(chunkIDValidity[id], ChunkType::validChunkTypeID(id));
	}
}
