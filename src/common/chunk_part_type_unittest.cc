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
#include "common/chunk_part_type.h"

#include <vector>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "common/slice_traits.h"
#include "unittests/chunk_type_constants.h"

TEST(ChunkTypeTests, SerializeDeserialize) {
	// Create array with all chunk types
	std::vector<ChunkPartType> allChunkTypes = { slice_traits::standard::ChunkPartType() };
	for (int level = slice_traits::xors::kMinXorLevel; level <= slice_traits::xors::kMaxXorLevel; ++level) {
		for (int part = 1; part <= level; ++part) {
			allChunkTypes.push_back(slice_traits::xors::ChunkPartType(level, part));
		}
		allChunkTypes.push_back(slice_traits::xors::ChunkPartType(level, slice_traits::xors::kXorParityPart));
	}

	std::vector<uint8_t> buffer;
	ChunkPartType deserializedChunkType = slice_traits::xors::ChunkPartType(2, slice_traits::xors::kXorParityPart);
	for (ChunkPartType chunkType : allChunkTypes) {
		buffer.clear();
		serialize(buffer, chunkType);
		deserialize(buffer, deserializedChunkType);
		EXPECT_EQ(chunkType, deserializedChunkType);
	}
}

TEST(ChunkTypeTests, validChunkTypeIDTest) {
	std::vector<bool> chunkIDValidity(1 << 16, false);
	chunkIDValidity[slice_traits::standard::ChunkPartType().getId()] = true;
	chunkIDValidity[slice_traits::tape::ChunkPartType().getId()] = true;
	for (int xorLevel = slice_traits::xors::kMinXorLevel;
	     xorLevel <= slice_traits::xors::kMaxXorLevel; ++xorLevel) {
		chunkIDValidity[slice_traits::xors::ChunkPartType(
		                        xorLevel, slice_traits::xors::kXorParityPart).getId()] =
		        true;
		for (int xorPart = 1; xorPart <= xorLevel; ++xorPart) {
			chunkIDValidity[slice_traits::xors::ChunkPartType(xorLevel, xorPart)
			                        .getId()] = true;
		}
	}
	for (int ec_data_count = slice_traits::ec::kMinDataCount; ec_data_count <= slice_traits::ec::kMaxDataCount; ++ec_data_count) {
	for (int ec_parity_count = slice_traits::ec::kMinParityCount; ec_parity_count <= slice_traits::ec::kMaxParityCount; ++ec_parity_count) {
	for (int ec_part = 0; ec_part < (ec_data_count + ec_parity_count); ++ec_part) {
		ChunkPartType test = slice_traits::ec::ChunkPartType(ec_data_count, ec_parity_count, ec_part);
		EXPECT_EQ(test.getSliceType(), slice_traits::ec::getSliceType(ec_data_count, ec_parity_count));
		EXPECT_EQ(test.getSlicePart(), ec_part);
		chunkIDValidity[slice_traits::ec::ChunkPartType(ec_data_count, ec_parity_count, ec_part).getId()] = true;
	}
	}
	}

	for (int id = 0; id < (1 << 16); ++id) {
		SCOPED_TRACE("ID: " + std::to_string(id));

		ChunkPartType cpt(id);

		EXPECT_EQ(chunkIDValidity[id], cpt.isValid())
		        << "invalid chunk id = " << (int)cpt.getId() << "(" << (int)cpt.getSliceType() << "," << cpt.getSlicePart() << ")";
	}
}

#define CHECK_CHUNK_TYPE_LENGTH(chunkType, expectedChunkTypeLen, wholeChunkLen) \
	EXPECT_EQ(expectedChunkTypeLen, \
			slice_traits::chunkLengthToChunkPartLength(chunkType, wholeChunkLen))

TEST(ChunkTypeTests, chunkTypeLengthTest) {
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 2 * MFSBLOCKSIZE    , 4 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 2 * MFSBLOCKSIZE + 1, 4 * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 3 * MFSBLOCKSIZE    , 5 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_p_of_2, 3 * MFSBLOCKSIZE    , 5 * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 2 * MFSBLOCKSIZE    , 4 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 2 * MFSBLOCKSIZE + 1, 4 * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 3 * MFSBLOCKSIZE    , 5 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_1_of_2, 3 * MFSBLOCKSIZE    , 5 * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2 * MFSBLOCKSIZE    , 4 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2 * MFSBLOCKSIZE    , 4 * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2 * MFSBLOCKSIZE    , 5 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(xor_2_of_2, 2 * MFSBLOCKSIZE + 1, 5 * MFSBLOCKSIZE + 1);

	CHECK_CHUNK_TYPE_LENGTH(standard,   4 * MFSBLOCKSIZE    , 4 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(standard,   4 * MFSBLOCKSIZE + 1, 4 * MFSBLOCKSIZE + 1);
	CHECK_CHUNK_TYPE_LENGTH(standard,   5 * MFSBLOCKSIZE    , 5 * MFSBLOCKSIZE);
	CHECK_CHUNK_TYPE_LENGTH(standard,   5 * MFSBLOCKSIZE + 1, 5 * MFSBLOCKSIZE + 1);
}

TEST(ChunkTypeTests, GetNumberOfBlocks) {
	ASSERT_EQ(1, slice_traits::getNumberOfBlocks(standard, 1));

	ASSERT_EQ(1, slice_traits::getNumberOfBlocks(xor_1_of_2, 1));
	ASSERT_EQ(0, slice_traits::getNumberOfBlocks(xor_2_of_2, 1));
	ASSERT_EQ(1, slice_traits::getNumberOfBlocks(xor_p_of_2, 1));

	ASSERT_EQ(1, slice_traits::getNumberOfBlocks(xor_1_of_2, 2));
	ASSERT_EQ(1, slice_traits::getNumberOfBlocks(xor_2_of_2, 2));
	ASSERT_EQ(1, slice_traits::getNumberOfBlocks(xor_p_of_2, 2));

	ASSERT_EQ(3, slice_traits::getNumberOfBlocks(xor_1_of_3, 8));
	ASSERT_EQ(3, slice_traits::getNumberOfBlocks(xor_2_of_3, 8));
	ASSERT_EQ(2, slice_traits::getNumberOfBlocks(xor_3_of_3, 8));
	ASSERT_EQ(3, slice_traits::getNumberOfBlocks(xor_p_of_3, 8));

	ASSERT_EQ(4, slice_traits::getNumberOfBlocks(xor_1_of_3, 12));
	ASSERT_EQ(4, slice_traits::getNumberOfBlocks(xor_2_of_3, 12));
	ASSERT_EQ(4, slice_traits::getNumberOfBlocks(xor_3_of_3, 12));
	ASSERT_EQ(4, slice_traits::getNumberOfBlocks(xor_p_of_3, 12));


	ASSERT_EQ(MFSBLOCKSINCHUNK / 1, int(slice_traits::getNumberOfBlocks(standard, MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 2, int(slice_traits::getNumberOfBlocks(xor_1_of_2, MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 2, int(slice_traits::getNumberOfBlocks(xor_2_of_2, MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 2, int(slice_traits::getNumberOfBlocks(xor_p_of_2, MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 3, int(slice_traits::getNumberOfBlocks(xor_3_of_3, MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 4, int(slice_traits::getNumberOfBlocks(xor_1_of_4, MFSBLOCKSINCHUNK)));
	ASSERT_EQ(MFSBLOCKSINCHUNK / 7, int(slice_traits::getNumberOfBlocks(xor_7_of_7, MFSBLOCKSINCHUNK)));

	ASSERT_EQ((MFSBLOCKSINCHUNK + 2)/ 3, int(slice_traits::getNumberOfBlocks(xor_1_of_3, MFSBLOCKSINCHUNK)));
	ASSERT_EQ((MFSBLOCKSINCHUNK + 1)/ 3, int(slice_traits::getNumberOfBlocks(xor_2_of_3, MFSBLOCKSINCHUNK)));
	ASSERT_EQ((MFSBLOCKSINCHUNK + 2)/ 3, int(slice_traits::getNumberOfBlocks(xor_p_of_3, MFSBLOCKSINCHUNK)));
}
