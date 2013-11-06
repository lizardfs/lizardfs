#include "common/cltoma_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"

TEST(CltomaCommunicationTests, FuseReadChunk) {
	uint32_t outMessageId, inMessageId = 512;
	uint32_t outInode, inInode = 112;
	uint32_t outDataBlockNumber, inDataBlockNumber = 1;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseReadChunk::serialize(buffer, inMessageId, inInode, inDataBlockNumber));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_READ_CHUNK);
	verifyVersion(buffer, 0U);

	ASSERT_NO_THROW(cltoma::fuseReadChunk::deserialize(
			removeHeader(buffer), outMessageId,  outInode, outDataBlockNumber));
	EXPECT_EQ(inMessageId, outMessageId);
	EXPECT_EQ(inInode, outInode);
	EXPECT_EQ(inDataBlockNumber, outDataBlockNumber);
}

TEST(CltomaCommunicationTests, FuseWriteChunk) {
	uint32_t outMessageId, inMessageId = 512;
	uint32_t outInode, inInode = 112;
	uint32_t outChunkIndex, inChunkIndex = 1583;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseWriteChunk::serialize(buffer, inMessageId, inInode, inChunkIndex));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_WRITE_CHUNK);
	verifyVersion(buffer, 0U);

	ASSERT_NO_THROW(cltoma::fuseWriteChunk::deserialize(
			removeHeader(buffer), outMessageId,  outInode, outChunkIndex));
	EXPECT_EQ(inMessageId, outMessageId);
	EXPECT_EQ(inInode, outInode);
	EXPECT_EQ(inChunkIndex, outChunkIndex);
}

TEST(CltomaCommunicationTests, FuseWriteChunkEnd) {
	uint32_t outMessageId, inMessageId = 512;
	uint64_t outChunkId, inChunkId = 4254;
	uint32_t outInode, inInode = 112;
	uint64_t outFileLength, inFileLength = 1583;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseWriteChunkEnd::serialize(buffer, inMessageId,
			inChunkId, inInode, inFileLength));

	verifyHeader(buffer, CLTOMA_FUSE_WRITE_CHUNK_END);
	verifyVersion(buffer, 0U);

	ASSERT_NO_THROW(cltoma::fuseWriteChunkEnd::deserialize(
			removeHeader(buffer), outMessageId,  outChunkId, outInode, outFileLength));
	EXPECT_EQ(inMessageId, outMessageId);
	EXPECT_EQ(inChunkId, outChunkId);
	EXPECT_EQ(inInode, outInode);
	EXPECT_EQ(inFileLength, outFileLength);
}
