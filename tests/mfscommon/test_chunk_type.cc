#include "mfscommon/chunk_type.h"
#include "mfscommon/goal.h"

#include <vector>
#include <gtest/gtest.h>

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
