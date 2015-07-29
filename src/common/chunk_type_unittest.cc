/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "common/chunk_type.h"

#include <vector>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "unittests/chunk_type_constants.h"

// FIXME
/*TEST(ChunkTypeTests, SerializeDeserialize) {
	// Create array with all chunk types
	std::vector<ChunkType> allChunkTypes = { ChunkType::getStandardChunkType() };
	for (ChunkType::XorLevel level = goal::kMinXorLevel; level < goal::kMaxXorLevel; ++level) {
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
	for (uint32_t xorLevel = goal::kMinXorLevel; xorLevel <= goal::kMaxXorLevel; ++xorLevel) {
		chunkIDValidity[ChunkType::getXorParityChunkType(xorLevel).chunkTypeId()] = true;
		for (uint32_t xorPart = 1; xorPart <= xorLevel; ++xorPart) {
			chunkIDValidity[ChunkType::getXorChunkType(xorLevel, xorPart).chunkTypeId()] = true;
		}
	}
	for (uint32_t id = 0; id < 256; ++id) {
		SCOPED_TRACE("ID: " + std::to_string(id));
		EXPECT_EQ(chunkIDValidity[id], ChunkType::validChunkTypeID(id));
	}
}

#define CHECK_CHUNK_TYPE_LENGTH(chunkType, expectedChunkTypeLen, wholeChunkLen) \
	EXPECT_EQ(expectedChunkTypeLen, \
			ChunkType::chunkLengthToChunkTypeLength(chunkType, wholeChunkLen))

TEST(ChunkTypeTests, chunkTypeLengthTest) {
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 2U * MFSBLOCKSIZE + 1, 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 2U * MFSBLOCKSIZE + 1, 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 3U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2U * MFSBLOCKSIZE + 1, 5U * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(standard,   4U * MFSBLOCKSIZE    , 4U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(standard,   4U * MFSBLOCKSIZE + 1, 4U * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(standard,   5U * MFSBLOCKSIZE    , 5U * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(standard,   5U * MFSBLOCKSIZE + 1, 5U * MFSBLOCKSIZE + 1);
}

TEST(ChunkTypeTests, GetNumberOfBlocks) {
	ASSERT_EQ(1U, standard.getNumberOfBlocks(1));

	ASSERT_EQ(1U, xor_1_of_2.getNumberOfBlocks(1));
	ASSERT_EQ(0U, xor_2_of_2.getNumberOfBlocks(1));
	ASSERT_EQ(1U, xor_p_of_2.getNumberOfBlocks(1));

	ASSERT_EQ(1U, xor_1_of_2.getNumberOfBlocks(2));
	ASSERT_EQ(1U, xor_2_of_2.getNumberOfBlocks(2));
	ASSERT_EQ(1U, xor_p_of_2.getNumberOfBlocks(2));

	ASSERT_EQ(3U, xor_1_of_3.getNumberOfBlocks(8));
	ASSERT_EQ(3U, xor_2_of_3.getNumberOfBlocks(8));
	ASSERT_EQ(2U, xor_3_of_3.getNumberOfBlocks(8));
	ASSERT_EQ(3U, xor_p_of_3.getNumberOfBlocks(8));

	ASSERT_EQ(4U, xor_1_of_3.getNumberOfBlocks(12));
	ASSERT_EQ(4U, xor_2_of_3.getNumberOfBlocks(12));
	ASSERT_EQ(4U, xor_3_of_3.getNumberOfBlocks(12));
	ASSERT_EQ(4U, xor_p_of_3.getNumberOfBlocks(12));


	ASSERT_EQ(MFSBLOCKSINCHUNK / 1, int(standard.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 2, int(xor_1_of_2.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 2, int(xor_2_of_2.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 2, int(xor_p_of_2.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 3, int(xor_3_of_3.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 4, int(xor_1_of_4.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 7, int(xor_7_of_7.getNumberOfBlocks(MFSBLOCKSINCHUNK)));

	ASSERT_EQ((MFSBLOCKSINCHUNK + 2)/ 3, int(xor_1_of_3.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ((MFSBLOCKSINCHUNK + 1)/ 3, int(xor_2_of_3.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
	ASSERT_EQ((MFSBLOCKSINCHUNK + 2)/ 3, int(xor_p_of_3.getNumberOfBlocks(MFSBLOCKSINCHUNK)));
}*/
