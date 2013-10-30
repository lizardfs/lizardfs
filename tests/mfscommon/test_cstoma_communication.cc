#include "common/cstoma_communication.h"

#include <gtest/gtest.h>

#include "common/crc.h"
#include "common/strerr.h"
#include "tests/common/packet.h"

TEST(CstomaCommunicationTests, RegisterHost) {
	uint32_t outIp, inIp = 127001;
	uint16_t outPort, inPort = 8080;
	uint16_t outTimeout, inTimeout = 1;
	uint32_t outCSVersion, inCSVersion = VERSHEX;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::registerHost::serialize(buffer,
			inIp, inPort, inTimeout, inCSVersion));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_CSTOMA_REGISTER_HOST, header.type);
	EXPECT_EQ(buffer.size() - PacketHeader::kSize, header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0u, version);

	ASSERT_NO_THROW(cstoma::registerHost::deserialize(removeHeader(buffer),
			outIp, outPort, outTimeout, outCSVersion));
	EXPECT_EQ(inIp, outIp);
	EXPECT_EQ(inPort, outPort);
	EXPECT_EQ(inTimeout, outTimeout);
	EXPECT_EQ(inCSVersion, outCSVersion);
}

TEST(CstomaCommunicationTests, registerChunksTest) {
	std::vector<ChunkWithVersionAndType> outChunks {
		ChunkWithVersionAndType(0, 1000, ChunkType::getXorChunkType(3, 1)),
		ChunkWithVersionAndType(1, 1001, ChunkType::getXorChunkType(7, 7)),
		ChunkWithVersionAndType(2, 1002, ChunkType::getXorParityChunkType(9)),
		ChunkWithVersionAndType(3, 1003, ChunkType::getStandardChunkType())
	};

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::registerChunks::serialize(buffer, outChunks));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_CSTOMA_REGISTER_CHUNKS, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header),
			header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	std::vector<ChunkWithVersionAndType> inChunks;
	ASSERT_NO_THROW(cstoma::registerChunks::deserialize(
			removeHeader(buffer), inChunks));
	EXPECT_EQ(inChunks.size(), outChunks.size());
	for (size_t i = 0; i < outChunks.size(); ++i) {
		SCOPED_TRACE(std::string("Checking chunk number ") + std::to_string(i));
		EXPECT_EQ(outChunks[i].id, inChunks[i].id);
		EXPECT_EQ(outChunks[i].version, inChunks[i].version);
		EXPECT_EQ(outChunks[i].type, inChunks[i].type);
	}
}

TEST(CstomaCommunicationTests, registerSpaceTest) {
	std::vector<uint8_t> buffer;

	uint64_t usedSpace[2] {1, 2};
	uint64_t totalSpace[2] {3, 4};
	uint32_t chunksNumber[2] {5, 6};
	uint64_t tdUsedSpace[2] {7, 8};
	uint64_t toDeleteTotalSpace[2] {9, 10};
	uint32_t toDeleteChunksNumber[2] {11, 12};
	ASSERT_NO_THROW(cstoma::registerSpace::serialize(buffer,
			usedSpace[0], totalSpace[0], chunksNumber[0], tdUsedSpace[0], toDeleteTotalSpace[0],
			toDeleteChunksNumber[0]));

	ASSERT_NO_THROW(cstoma::registerSpace::deserialize(removeHeader(buffer),
			usedSpace[1], totalSpace[1], chunksNumber[1], tdUsedSpace[1], toDeleteTotalSpace[1],
			toDeleteChunksNumber[1]));

	EXPECT_EQ(usedSpace[0], usedSpace[1]);
	EXPECT_EQ(totalSpace[0], totalSpace[1]);
	EXPECT_EQ(chunksNumber[0], chunksNumber[1]);
	EXPECT_EQ(tdUsedSpace[0], tdUsedSpace[1]);
	EXPECT_EQ(toDeleteTotalSpace[0], toDeleteTotalSpace[1]);
	EXPECT_EQ(toDeleteChunksNumber[0], toDeleteChunksNumber[1]);
}
