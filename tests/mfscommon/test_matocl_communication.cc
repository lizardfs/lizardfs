#include <gtest/gtest.h>

#include "tests/common/packet.h"
#include "mfscommon/matocl_communication.h"

TEST(MatoclCommunicationTests, fuseReadChunkDataTest) {
	uint64_t outChunkId, inChunkId = 87;
	uint32_t outChunkVersion, inChunkVersion = 52;
	uint64_t outFileLength, inFileLength = 1024;
	std::vector<ChunkserverHoldingPartOfChunk> outServerList;
	std::vector<ChunkserverHoldingPartOfChunk> inServerList {
		ChunkserverHoldingPartOfChunk(127001, 8080, ChunkType::getStandardChunkType()),
		ChunkserverHoldingPartOfChunk(127002, 8081, ChunkType::getXorParityChunkType(5)),
		ChunkserverHoldingPartOfChunk(127003, 8082, ChunkType::getXorChunkType(5, 1)),
		ChunkserverHoldingPartOfChunk(127004, 8084, ChunkType::getXorChunkType(5, 5)),
	};
	ChunkserverHoldingPartOfChunk outServer;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseReadChunkData::serialize(buffer, inChunkId, inChunkVersion,
			inFileLength,inServerList));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_MATOCL_FUSE_READ_CHUNK_DATA, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	ASSERT_NO_THROW(matocl::fuseReadChunkData::deserialize(
			removeHeader(buffer), outChunkId, outChunkVersion, outFileLength, outServerList));

	EXPECT_EQ(inChunkId, outChunkId);
	EXPECT_EQ(inChunkVersion, outChunkVersion);
	EXPECT_EQ(inFileLength, outFileLength);

	for (uint i = 0; i < outServerList.size(); ++i){
		SCOPED_TRACE("Server number: " + std::to_string(i));
		EXPECT_EQ(inServerList[i].ip, outServerList[i].ip);
		EXPECT_EQ(inServerList[i].port, outServerList[i].port);
		EXPECT_EQ(inServerList[i].chunkType, outServerList[i].chunkType);
	}
}

TEST(MatoclCommunicationTests, fuseReadChunkStatusTest) {
	uint8_t outStatus, inStatus = 0;
	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseReadChunkStatus::serialize(buffer, inStatus));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_MATOCL_FUSE_READ_CHUNK_STATUS, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	ASSERT_NO_THROW(matocl::fuseReadChunkStatus::deserialize(removeHeader(buffer), outStatus));
	EXPECT_EQ(inStatus, outStatus);
}
