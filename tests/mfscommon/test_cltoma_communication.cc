#include "common/cltoma_communication.h"

#include <gtest/gtest.h>

#include "tests/common/packet.h"

TEST(CltomaCommunicationTests, FuseReadChunkData) {
	uint32_t outMessageId, inMessageId = 512;
	uint32_t outInode, inInode = 112;
	uint32_t outDataBlockNumber, inDataBlockNumber = 1;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseReadChunk::serialize(buffer, inMessageId, inInode, inDataBlockNumber));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_CLTOMA_FUSE_READ_CHUNK, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	ASSERT_NO_THROW(cltoma::fuseReadChunk::deserialize(
			removeHeader(buffer), outMessageId,  outInode, outDataBlockNumber));
	EXPECT_EQ(inMessageId, outMessageId);
	EXPECT_EQ(inInode, outInode);
	EXPECT_EQ(inDataBlockNumber, outDataBlockNumber);
}
