#include "mfscommon/cltocs_communication.h"

#include <gtest/gtest.h>

TEST(CltocsCommunicationTests, Read) {
	std::vector<uint8_t> buffer;
	uint64_t chunkIdIn = 0x0123456789ABCDEF, chunkIdOut = 0;
	uint32_t chunkVersionIn = 0x01234567, chunkVersionOut = 0;
	ChunkType chunkTypeIn = ChunkType::getXorParityChunkType(8),
			chunkTypeOut = ChunkType::getStandardChunkType();
	uint32_t readOffsetIn = 2 * MFSBLOCKSIZE, readOffsetOut = 0;
	uint32_t readSizeIn = 5 * MFSBLOCKSIZE, readSizeOut = 0;

	ASSERT_NO_THROW(cltocs::read::serialize(buffer, chunkIdIn, chunkVersionIn, chunkTypeIn,
			readOffsetIn, readSizeIn));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_CLTOCS_READ, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	std::vector<uint8_t> bufferWithoutHeader(
			buffer.begin() + serializedSize(header), buffer.end());
	ASSERT_NO_THROW(cltocs::read::deserialize(
			bufferWithoutHeader.data(), bufferWithoutHeader.size(),
			chunkIdOut, chunkVersionOut, chunkTypeOut, readOffsetOut, readSizeOut));
	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(chunkVersionIn, chunkVersionOut);
	EXPECT_EQ(chunkTypeIn, chunkTypeOut);
	EXPECT_EQ(readOffsetIn, readOffsetOut);
	EXPECT_EQ(readSizeIn, readSizeOut);
}
