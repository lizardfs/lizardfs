#include "common/matocs_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"

TEST(MatocsCommunicationTests, SetVersion) {
	uint64_t chunkIdOut, chunkIdIn = 62443697;
	uint32_t chunkVersionOut, chunkVersionIn = 436457;
	ChunkType chunkTypeOut = ChunkType::getStandardChunkType();
	ChunkType chunkTypeIn = ChunkType::getXorChunkType(3, 2);
	uint32_t newVersionOut, newVersionIn = 173333;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocs::setVersion::serialize(buffer,
			chunkIdIn, chunkVersionIn, chunkTypeIn, newVersionIn));

	verifyHeader(buffer, LIZ_MATOCS_SET_VERSION);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(matocs::setVersion::deserialize(buffer,
			chunkIdOut, chunkVersionOut, chunkTypeOut, newVersionOut));

	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(chunkVersionIn, chunkVersionOut);
	EXPECT_EQ(chunkTypeIn, chunkTypeOut);
	EXPECT_EQ(newVersionIn, newVersionOut);
}

TEST(MatocsCommunicationTests, DeleteChunk) {
	uint64_t chunkIdOut, chunkIdIn = 62443697;
	uint32_t chunkVersionOut, chunkVersionIn = 436457;
	ChunkType chunkTypeOut = ChunkType::getStandardChunkType();
	ChunkType chunkTypeIn = ChunkType::getXorChunkType(3, 2);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocs::deleteChunk::serialize(buffer,
			chunkIdIn, chunkVersionIn, chunkTypeIn));

	verifyHeader(buffer, LIZ_MATOCS_DELETE_CHUNK);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(matocs::deleteChunk::deserialize(buffer,
			chunkIdOut, chunkVersionOut, chunkTypeOut));

	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(chunkVersionIn, chunkVersionOut);
	EXPECT_EQ(chunkTypeIn, chunkTypeOut);
}
