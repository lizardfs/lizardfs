#include "common/matocs_communication.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"
#include "unittests/operators.h"
#include "unittests/packet.h"

TEST(MatocsCommunicationTests, SetVersion) {
	uint64_t chunkIdOut, chunkIdIn = 62443697;
	uint32_t chunkVersionOut, chunkVersionIn = 436457;
	ChunkType chunkTypeOut = ChunkType::getStandardChunkType();
	ChunkType chunkTypeIn = ChunkType::getXorChunkType(3, 2);
	uint32_t newVersionOut, newVersionIn = 173333;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocs::setVersion::serialize(buffer,
			chunkIdIn, chunkTypeIn, chunkVersionIn, newVersionIn));

	verifyHeader(buffer, LIZ_MATOCS_SET_VERSION);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(matocs::setVersion::deserialize(buffer,
			chunkIdOut, chunkTypeOut, chunkVersionOut, newVersionOut));

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
			chunkIdIn, chunkTypeIn, chunkVersionIn));

	verifyHeader(buffer, LIZ_MATOCS_DELETE_CHUNK);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(matocs::deleteChunk::deserialize(buffer,
			chunkIdOut, chunkTypeOut, chunkVersionOut));

	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(chunkVersionIn, chunkVersionOut);
	EXPECT_EQ(chunkTypeIn, chunkTypeOut);
}

TEST(MatocsCommunicationTests, Replicate) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId,      87,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 52,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, ChunkType::getXorParityChunkType(3), ChunkType::getStandardChunkType());
	LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(ChunkTypeWithAddress, serverList) = {
		ChunkTypeWithAddress(NetworkAddress(0xC0A80001, 8080), ChunkType::getStandardChunkType()),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80002, 8081), ChunkType::getXorParityChunkType(5)),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80003, 8082), ChunkType::getXorChunkType(5, 1)),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80004, 8084), ChunkType::getXorChunkType(5, 5)),
	};

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocs::replicate::serialize(buffer,
			chunkIdIn, chunkVersionIn, chunkTypeIn, serverListIn));

	verifyHeader(buffer, LIZ_MATOCS_REPLICATE);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocs::replicate::deserialize(buffer,
			chunkIdOut, chunkVersionOut, chunkTypeOut, serverListOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
	LIZARDFS_VERIFY_INOUT_PAIR(serverList);
}
