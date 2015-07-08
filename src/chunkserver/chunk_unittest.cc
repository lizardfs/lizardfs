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
#include "chunkserver/chunk.h"

#include <gtest/gtest.h>

class ChunkTests : public testing::Test {
public:
	ChunkTests()
			: standardChunk(1, ChunkType::getStandardChunkType(), CH_AVAIL),
			  chunk_1_of_2(1, ChunkType::getXorChunkType(2, 1), CH_AVAIL),
			  chunk_2_of_2(1, ChunkType::getXorChunkType(2, 2), CH_AVAIL),
			  chunk_p_of_2(1, ChunkType::getXorParityChunkType(2), CH_AVAIL),
			  chunk_1_of_3(1, ChunkType::getXorChunkType(3, 1), CH_AVAIL),
			  chunk_3_of_3(1, ChunkType::getXorChunkType(3, 3), CH_AVAIL),
			  chunk_p_of_3(1, ChunkType::getXorParityChunkType(3), CH_AVAIL) {
	}

protected:
	MooseFSChunk standardChunk;
	MooseFSChunk chunk_1_of_2, chunk_2_of_2, chunk_p_of_2;
	InterleavedChunk chunk_1_of_3, chunk_3_of_3, chunk_p_of_3;
};

TEST_F(ChunkTests, MaxBlocksInFile) {
	EXPECT_EQ(1024U, standardChunk.maxBlocksInFile());
	EXPECT_EQ(512U, chunk_1_of_2.maxBlocksInFile());
	EXPECT_EQ(512U, chunk_2_of_2.maxBlocksInFile());
	EXPECT_EQ(512U, chunk_p_of_2.maxBlocksInFile());
	EXPECT_EQ(342U, chunk_1_of_3.maxBlocksInFile());
	EXPECT_EQ(342U, chunk_3_of_3.maxBlocksInFile());
	EXPECT_EQ(342U, chunk_p_of_3.maxBlocksInFile());
}

TEST_F(ChunkTests, GetFileName) {
	folder f;
	std::vector<char> folderPath = { '/', 'm', 'n', 't', '/', 0 };
	f.path = folderPath.data();

	standardChunk.chunkid = 0x123456;
	standardChunk.owner = &f;
	EXPECT_EQ("/mnt/chunks12/chunk_0000000000123456_0000ABCD.mfs",
			standardChunk.generateFilenameForVersion(0xabcd));

	chunk_1_of_3.chunkid = 0x8765430d;
	chunk_1_of_3.owner = &f;
	EXPECT_EQ("/mnt/chunks65/chunk_xor_1_of_3_000000008765430D_00654321.liz",
			chunk_1_of_3.generateFilenameForVersion(0x654321));

	chunk_p_of_3.chunkid = 0x1234567890abcdef;
	chunk_p_of_3.owner = &f;
	EXPECT_EQ("/mnt/chunksAB/chunk_xor_parity_of_3_1234567890ABCDEF_12345678.liz",
			chunk_p_of_3.generateFilenameForVersion(0x12345678));
}

TEST_F(ChunkTests, GetSubfolderName) {
	EXPECT_EQ("chunks00", Chunk::getSubfolderNameGivenNumber(0x00));
	EXPECT_EQ("chunksAB", Chunk::getSubfolderNameGivenNumber(0xAB));
	EXPECT_EQ("chunksFF", Chunk::getSubfolderNameGivenNumber(0xFF));
	EXPECT_EQ("chunks00", Chunk::getSubfolderNameGivenChunkId(0x1234512345003456LL));
	EXPECT_EQ("chunksAD", Chunk::getSubfolderNameGivenChunkId(0x1234512345AD3456LL));
	EXPECT_EQ("chunksFF", Chunk::getSubfolderNameGivenChunkId(0x1234512345FF3456LL));
}
