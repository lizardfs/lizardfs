#include "config.h"
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
	Chunk standardChunk;
	Chunk chunk_1_of_2, chunk_2_of_2, chunk_p_of_2;
	Chunk chunk_1_of_3, chunk_3_of_3, chunk_p_of_3;
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

TEST_F(ChunkTests, GetHeaderSize) {
	EXPECT_EQ(5 * 1024U, standardChunk.getHeaderSize());
	EXPECT_EQ(4 * 1024U, chunk_1_of_2.getHeaderSize());
	EXPECT_EQ(4 * 1024U, chunk_2_of_2.getHeaderSize());
	EXPECT_EQ(4 * 1024U, chunk_p_of_2.getHeaderSize());
	EXPECT_EQ(4 * 1024U, chunk_1_of_3.getHeaderSize());
	EXPECT_EQ(4 * 1024U, chunk_3_of_3.getHeaderSize());
	EXPECT_EQ(4 * 1024U, chunk_p_of_3.getHeaderSize());
}

TEST_F(ChunkTests, GetDataBlockOffset) {
	EXPECT_EQ(5 * 1024U, standardChunk.getDataBlockOffset(0));
	EXPECT_EQ(5 * 1024U + MFSBLOCKSIZE, standardChunk.getDataBlockOffset(1));
	EXPECT_EQ(4 * 1024U, chunk_1_of_2.getDataBlockOffset(0));
	EXPECT_EQ(4 * 1024U + MFSBLOCKSIZE, chunk_1_of_2.getDataBlockOffset(1));
	EXPECT_EQ(4 * 1024U, chunk_1_of_3.getDataBlockOffset(0));
	EXPECT_EQ(4 * 1024U + MFSBLOCKSIZE, chunk_1_of_3.getDataBlockOffset(1));
}

TEST_F(ChunkTests, IsFileSizeValid) {
	EXPECT_FALSE(standardChunk.isFileSizeValid(4 * 1024));
	EXPECT_FALSE(standardChunk.isFileSizeValid(5 * 1024 + 100));
	EXPECT_FALSE(standardChunk.isFileSizeValid(5 * 1024 + 10000));
	EXPECT_FALSE(standardChunk.isFileSizeValid(70 * 1025 * 1024));
	EXPECT_TRUE(standardChunk.isFileSizeValid(5 * 1024));
	EXPECT_TRUE(standardChunk.isFileSizeValid(5 * 1024 + MFSBLOCKSIZE));
	EXPECT_TRUE(standardChunk.isFileSizeValid(5 * 1024 + 1024 * MFSBLOCKSIZE));
	EXPECT_FALSE(standardChunk.isFileSizeValid(5 * 1024 + 1024 * MFSBLOCKSIZE + 1));
	EXPECT_FALSE(standardChunk.isFileSizeValid(5 * 1024 + 1025 * MFSBLOCKSIZE));

	for (const Chunk& chunk : { chunk_1_of_2, chunk_2_of_2, chunk_p_of_2 }) {
		EXPECT_FALSE(chunk.isFileSizeValid(5 * 1024));
		EXPECT_FALSE(chunk.isFileSizeValid(4 * 1024 + 100));
		EXPECT_FALSE(chunk.isFileSizeValid(4 * 1024 + 10000));
		EXPECT_FALSE(chunk.isFileSizeValid(70 * 1025 * 1024));
		EXPECT_TRUE(chunk.isFileSizeValid(4 * 1024));
		EXPECT_TRUE(chunk.isFileSizeValid(4 * 1024 + MFSBLOCKSIZE));
		EXPECT_TRUE(chunk.isFileSizeValid(4 * 1024 + 512 * MFSBLOCKSIZE));
		EXPECT_FALSE(chunk.isFileSizeValid(4 * 1024 + 512 * MFSBLOCKSIZE + 1));
		EXPECT_FALSE(chunk.isFileSizeValid(5 * 1024 + 513 * MFSBLOCKSIZE));
	}

	for (const Chunk& chunk : { chunk_1_of_3, chunk_3_of_3, chunk_p_of_3 }) {
		EXPECT_FALSE(chunk.isFileSizeValid(5 * 1024));
		EXPECT_FALSE(chunk.isFileSizeValid(4 * 1024 + 100));
		EXPECT_FALSE(chunk.isFileSizeValid(4 * 1024 + 10000));
		EXPECT_FALSE(chunk.isFileSizeValid(70 * 1025 * 1024));
		EXPECT_TRUE(chunk.isFileSizeValid(4 * 1024));
		EXPECT_TRUE(chunk.isFileSizeValid(4 * 1024 + MFSBLOCKSIZE));
		EXPECT_TRUE(chunk.isFileSizeValid(4 * 1024 + 342 * MFSBLOCKSIZE));
		EXPECT_FALSE(chunk.isFileSizeValid(4 * 1024 + 342 * MFSBLOCKSIZE + 1));
		EXPECT_FALSE(chunk.isFileSizeValid(5 * 1024 + 343 * MFSBLOCKSIZE));
	}
}

TEST_F(ChunkTests, SetBlockCountFromFizeSize) {
	standardChunk.setBlockCountFromFizeSize(5 * 1024);
	EXPECT_EQ(0, standardChunk.blocks);
	standardChunk.setBlockCountFromFizeSize(5 * 1024 + MFSBLOCKSIZE);
	EXPECT_EQ(1, standardChunk.blocks);
	standardChunk.setBlockCountFromFizeSize(5 * 1024 + 1024 * MFSBLOCKSIZE);
	EXPECT_EQ(1024, standardChunk.blocks);

	std::vector<Chunk> chunks_2 { chunk_1_of_2, chunk_2_of_2, chunk_p_of_2 };
	std::vector<Chunk> chunks_3 { chunk_1_of_3, chunk_3_of_3, chunk_p_of_3 };

	for (Chunk& chunk : chunks_2) {
		chunk.setBlockCountFromFizeSize(4 * 1024);
		EXPECT_EQ(0, chunk.blocks);
		chunk.setBlockCountFromFizeSize(4 * 1024 + 1 * MFSBLOCKSIZE);
		EXPECT_EQ(1, chunk.blocks);
		chunk.setBlockCountFromFizeSize(4 * 1024 + 512 * MFSBLOCKSIZE);
		EXPECT_EQ(512, chunk.blocks);
	}

	for (Chunk& chunk : chunks_3) {
		chunk.setBlockCountFromFizeSize(4 * 1024);
		EXPECT_EQ(0, chunk.blocks);
		chunk.setBlockCountFromFizeSize(4 * 1024 + 1 * MFSBLOCKSIZE);
		EXPECT_EQ(1, chunk.blocks);
		chunk.setBlockCountFromFizeSize(4 * 1024 + 342 * MFSBLOCKSIZE);
		EXPECT_EQ(342, chunk.blocks);
	}
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
	EXPECT_EQ("/mnt/chunks65/chunk_xor_1_of_3_000000008765430D_00654321.mfs",
			chunk_1_of_3.generateFilenameForVersion(0x654321));

	chunk_p_of_3.chunkid = 0x1234567890abcdef;
	chunk_p_of_3.owner = &f;
	EXPECT_EQ("/mnt/chunksAB/chunk_xor_parity_of_3_1234567890ABCDEF_12345678.mfs",
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
