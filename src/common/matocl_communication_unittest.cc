#include "common/matocl_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"

TEST(MatoclCommunicationTests, FuseReadChunkData) {
	uint32_t /*outMessageId, */inMessageId = 512; //TODO(alek) check messageID
	uint64_t outChunkId, inChunkId = 87;
	uint32_t outChunkVersion, inChunkVersion = 52;
	uint64_t outFileLength, inFileLength = 1024;
	std::vector<ChunkTypeWithAddress> outServerList;
	std::vector<ChunkTypeWithAddress> inServerList {
		ChunkTypeWithAddress(NetworkAddress(0xC0A80001, 8080), ChunkType::getStandardChunkType()),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80002, 8081), ChunkType::getXorParityChunkType(5)),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80003, 8082), ChunkType::getXorChunkType(5, 1)),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80004, 8084), ChunkType::getXorChunkType(5, 5)),
	};
	ChunkTypeWithAddress outServer;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseReadChunk::serialize(buffer, inMessageId, inFileLength, inChunkId,
			inChunkVersion, inServerList));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_MATOCL_FUSE_READ_CHUNK, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 0;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(1U, version);

	ASSERT_NO_THROW(matocl::fuseReadChunk::deserialize(removeHeader(buffer), outFileLength,
			outChunkId, outChunkVersion, outServerList));

	EXPECT_EQ(inChunkId, outChunkId);
	EXPECT_EQ(inChunkVersion, outChunkVersion);
	EXPECT_EQ(inFileLength, outFileLength);

	for (uint i = 0; i < outServerList.size(); ++i){
		SCOPED_TRACE("Server number: " + std::to_string(i));
		EXPECT_EQ(inServerList[i].address.ip, outServerList[i].address.ip);
		EXPECT_EQ(inServerList[i].address.port, outServerList[i].address.port);
		EXPECT_EQ(inServerList[i].chunkType, outServerList[i].chunkType);
	}
}

TEST(MatoclCommunicationTests, FuseReadChunkStatus) {
	uint32_t /*outMessageId, */inMessageId = 512; //TODO(alek) check messageID
	uint8_t outStatus, inStatus = 0;
	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseReadChunk::serialize(buffer, inMessageId, inStatus));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_MATOCL_FUSE_READ_CHUNK, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	ASSERT_NO_THROW(matocl::fuseReadChunk::deserialize(removeHeader(buffer), outStatus));
	EXPECT_EQ(inStatus, outStatus);
}
