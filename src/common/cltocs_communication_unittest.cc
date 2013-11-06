#include "common/cltocs_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"

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

	verifyHeader(buffer, LIZ_CLTOCS_READ);
	verifyVersion(buffer, 0U);

	std::vector<uint8_t> bufferWithoutHeader = removeHeader(buffer);
	ASSERT_NO_THROW(cltocs::read::deserialize(
			bufferWithoutHeader.data(), bufferWithoutHeader.size(),
			chunkIdOut, chunkVersionOut, chunkTypeOut, readOffsetOut, readSizeOut));
	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(chunkVersionIn, chunkVersionOut);
	EXPECT_EQ(chunkTypeIn, chunkTypeOut);
	EXPECT_EQ(readOffsetIn, readOffsetOut);
	EXPECT_EQ(readSizeIn, readSizeOut);
}
