#include "mfschunkserver/chunk_filename_parser.h"

#include <gtest/gtest.h>

TEST(ChunkFilenameParser, ParseStandardChunkFilename) {
	ChunkFilenameParser filenameParser("chunk_0000000000550A00_00000001.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000001U, filenameParser.chunkVersion());
	EXPECT_EQ(ChunkType::getStandardChunkType(), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseXorChunkFilename) {
	ChunkFilenameParser filenameParser("chunk_xor_1_of_3_0000000000550A00_00000002.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000002U, filenameParser.chunkVersion());
	EXPECT_EQ(ChunkType::getXorChunkType(3, 1), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseXorChunkFilenameMaxLevel) {
	ChunkFilenameParser filenameParser("chunk_xor_10_of_10_0000000000550A00_00000002.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000002U, filenameParser.chunkVersion());
	EXPECT_EQ(ChunkType::getXorChunkType(10, 10), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseXorParityFilename) {
	ChunkFilenameParser filenameParser("chunk_xor_parity_of_3_0000000000550A00_00000003.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000003U, filenameParser.chunkVersion());
	EXPECT_EQ(ChunkType::getXorParityChunkType(3), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseXorParityFilenameMaxLevel) {
	ChunkFilenameParser filenameParser("chunk_xor_parity_of_10_0000000000550A00_00000003.mfs");
	ASSERT_EQ(ChunkFilenameParser::OK, filenameParser.parse());
	EXPECT_EQ(0x550A00U, filenameParser.chunkId());
	EXPECT_EQ(0x000003U, filenameParser.chunkVersion());
	EXPECT_EQ(ChunkType::getXorParityChunkType(10), filenameParser.chunkType());
}

TEST(ChunkFilenameParser, ParseWrongFilenames) {
	// leading whitespace
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser(" chunk_0000000000550A00_00000001.mfs").parse());

	// lowercase letters in chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550a00_00000003.mfs").parse());

	// lowercase letters in chunk version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_0000000d.mfs").parse());

	// 15 digits of chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_000000000550A00_00000003.mfs").parse());

	// 17 digits of chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_00000000000550A00_00000003.mfs").parse());

	// 7 digits of version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_0000003.mfs").parse());

	// 9 digits of version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_000000003.mfs").parse());

	// trailing characters
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_00000001.mfsABC").parse());

	// trailing whitespace
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_00000001.mfs ").parse());

	// leading 0's in xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_04_of_5_0000000000550A00_00000001.mfs").parse());

	// wrong xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_3_of_2_0000000000550A00_00000001.mfs").parse());

	// wrong xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_0_of_2_0000000000550A00_00000001.mfs").parse());

	// leading 0's in xor part
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_4_of_05_0000000000550A00_00000001.mfs").parse());

	// wrong xor level
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_4_of_20_0000000000550A00_00000001.mfs").parse());

	// wrong xor level
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_4_of_A_0000000000550A00_00000001.mfs").parse());

	// wrong xor level
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_1_of_1_0000000000550A00_00000001.mfs").parse());

	// wrong xor level in parity chunk
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_parity_of_1_0000000000550A00_00000001.mfs").parse());

	// wrong xor level in parity chunk
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_parity_of_0_0000000000550A00_00000001.mfs").parse());

	// wrong xor level in parity chunk
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_parity_of_30_0000000000550A00_00000001.mfs").parse());

	// missing some parts
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_xor_0000000000550A00_00000001.mfs").parse());

	// some wrong characters inside chunk id
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_00000#0000550A00_00000001.mfs").parse());

	// some wrong characters inside chunk version
	EXPECT_EQ(ChunkFilenameParser::ERROR_INVALID_FILENAME,
			ChunkFilenameParser("chunk_0000000000550A00_000#0001.mfs").parse());
}
