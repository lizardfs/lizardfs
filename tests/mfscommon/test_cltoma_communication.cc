#include <gtest/gtest.h>

#include "tests/common/packet.h"
#include "mfscommon/cltoma_communication.h"

TEST(CltomaCommunicationTests, fuseReadChunkDataTest) {
	uint32_t outInode, inInode = 112;
	uint32_t outDataBlockNumber, inDataBlockNumber = 1;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseReadChunk::serialize(buffer, inInode, inDataBlockNumber));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_CLTOMA_FUSE_READ_CHUNK, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	ASSERT_NO_THROW(cltoma::fuseReadChunk::deserialize(
			removeHeader(buffer), outInode, outDataBlockNumber));
	EXPECT_EQ(inInode, outInode);
	EXPECT_EQ(inDataBlockNumber, outDataBlockNumber);
}
