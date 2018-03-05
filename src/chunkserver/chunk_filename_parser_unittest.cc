/*
   Copyright 2013-2016 Skytechnology sp. z o.o.

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
#include "common/slice_traits.h"
#include "chunkserver/chunk_filename_parser.h"

#include <gtest/gtest.h>


TEST(ChunkFilenameParser, ParseStandardChunkFilename) {
	ChunkFilenameParser filenameParser("chunk_0000000000550A00_00000001.liz");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(ChunkFormat::INTERLEAVED, filenameParser.chunkFormat());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000001U, filenameParser.chunkVersion());
	EXPECT_EQ(slice_traits::standard::ChunkPartType(), filenameParser.chunkType());

	// hashes unparsable by stoll(), but parsable by stoull()
	EXPECT_EQ(ChunkFilenameParser::OK,
			ChunkFilenameParser("chunk_9999999999999999_00000001.liz").parse());
}

TEST(ChunkFilenameParser, ParseXorChunkFilename) {
	ChunkFilenameParser filenameParser("chunk_xor_1_of_3_0000000000550A00_00000002.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(ChunkFormat::MOOSEFS, filenameParser.chunkFormat());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000002U, filenameParser.chunkVersion());
	EXPECT_EQ(slice_traits::xors::ChunkPartType(3, 1), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseXorChunkFilenameMaxLevel) {
	ChunkFilenameParser filenameParser("chunk_xor_9_of_9_0000000000550A00_00000002.liz");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(ChunkFormat::INTERLEAVED, filenameParser.chunkFormat());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000002U, filenameParser.chunkVersion());
	EXPECT_EQ(slice_traits::xors::ChunkPartType(9, 9), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseXorParityFilename) {
	ChunkFilenameParser filenameParser("chunk_xor_parity_of_3_0000000000550A00_00000003.liz");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(ChunkFormat::INTERLEAVED, filenameParser.chunkFormat());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000003U, filenameParser.chunkVersion());
	EXPECT_EQ(slice_traits::xors::ChunkPartType(3, slice_traits::xors::kXorParityPart), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseXorParityFilenameMaxLevel) {
	ChunkFilenameParser filenameParser("chunk_xor_parity_of_9_0000000000550A00_00000003.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(ChunkFormat::MOOSEFS, filenameParser.chunkFormat());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000003U, filenameParser.chunkVersion());
	EXPECT_EQ(slice_traits::xors::ChunkPartType(9, slice_traits::xors::kXorParityPart), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseECChunkFilename) {
	ChunkFilenameParser filenameParser("chunk_ec2_3_of_2_4_0000000000550A00_00000002.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(ChunkFormat::MOOSEFS, filenameParser.chunkFormat());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000002U, filenameParser.chunkVersion());
	EXPECT_EQ(slice_traits::ec::ChunkPartType(2, 4, 2), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseECChunkFilenameMaxLevel) {
	ChunkFilenameParser filenameParser("chunk_ec2_1_of_3_7_0000000000550A00_00000002.liz");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(ChunkFormat::INTERLEAVED, filenameParser.chunkFormat());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000002U, filenameParser.chunkVersion());
	EXPECT_EQ(slice_traits::ec::ChunkPartType(3, 7, 0), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseWrongFilenames) {
	// leading whitespace
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser(" chunk_0000000000550A00_00000001.liz").parse());

	// lowercase letters in chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550a00_00000003.liz").parse());

	// lowercase letters in chunk version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_0000000d.liz").parse());

	// 15 digits of chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_000000000550A00_00000003.liz").parse());

	// 17 digits of chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_00000000000550A00_00000003.liz").parse());

	// 7 digits of version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_0000003.liz").parse());

	// 9 digits of version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_000000003.liz").parse());

	// trailing characters
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_00000001.lizABC").parse());

	// trailing whitespace
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_00000001.liz ").parse());

	// leading 0's in xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_04_of_5_0000000000550A00_00000001.liz").parse());

	// wrong xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_3_of_2_0000000000550A00_00000001.liz").parse());

	// wrong xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_0_of_2_0000000000550A00_00000001.liz").parse());

	// leading 0's in xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_4_of_05_0000000000550A00_00000001.liz").parse());

	// wrong xor level
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_4_of_20_0000000000550A00_00000001.liz").parse());

	// wrong xor level
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_4_of_A_0000000000550A00_00000001.liz").parse());

	// wrong xor level
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_1_of_1_0000000000550A00_00000001.liz").parse());

	// wrong xor level in parity chunk
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_parity_of_1_0000000000550A00_00000001.liz").parse());

	// wrong xor level in parity chunk
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_parity_of_0_0000000000550A00_00000001.liz").parse());

	// wrong xor level in parity chunk
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_parity_of_30_0000000000550A00_00000001.liz").parse());

	// missing some parts
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_0000000000550A00_00000001.liz").parse());

	// some wrong characters inside chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_00000#0000550A00_00000001.liz").parse());

	// some wrong characters inside chunk version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_000#0001.liz").parse());

	// leading 0's in ec part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_ec2_04_of_3_5_0000000000550A00_00000001.liz").parse());

	// wrong ec part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_ec2_4_of_2_1_0000000000550A00_00000001.liz").parse());

	// wrong ec part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_ec2_0_of_2_1_0000000000550A00_00000001.liz").parse());

	// leading 0's in data part count
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_ec2_4_of_05_10_0000000000550A00_00000001.liz").parse());

	// leading 0's in parity part count
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_ec2_4_of_05_10_0000000000550A00_00000001.liz").parse());

	// wrong ec number of parity parts
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_ec2_4_of_7_33_0000000000550A00_00000001.liz").parse());

	// wrong ec number of data parts
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_ec2_4_of_39_8_0000000000550A00_00000001.liz").parse());

	// wrong ec number of data parts
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_1_of_1_3_0000000000550A00_00000001.liz").parse());
}
