#include "common/chunk_type.h"

#include <gtest/gtest.h>
#include <vector>

#include "common/goal.h"
#include "unittests/operators.h"

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

#define CHECK_CHUNK_TYPE_LENGTH(chunkType, expectedChunkTypeLen, wholeChunkLen) \
	EXPECT_EQ(expectedChunkTypeLen, \
			ChunkType::chunkLengthToChunkTypeLength(chunkType, wholeChunkLen))

TEST(ChunkTypeTests, chunkTypeLengthTest) {
	ChunkType parity2 = ChunkType::getXorParityChunkType(2);
	ChunkType part1 = ChunkType::getXorChunkType(2, 1);
	ChunkType part2 = ChunkType::getXorChunkType(2, 2);
	ChunkType standard = ChunkType::getStandardChunkType();

	CHECK_CHUNK_TYPE_LENGTH(parity2 , 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(parity2 , 2U * MFSBLOCKSIZE + 1, 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(parity2 , 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(parity2 , 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(part1   , 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(part1   , 2U * MFSBLOCKSIZE + 1, 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(part1   , 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(part1   , 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(part2   , 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(part2   , 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(part2   , 2U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(part2   , 2U * MFSBLOCKSIZE + 1, 5U * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(standard, 4U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(standard, 4U * MFSBLOCKSIZE + 1, 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(standard, 5U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(standard, 5U * MFSBLOCKSIZE + 1, 5U * MFSBLOCKSIZE + 1);
}
