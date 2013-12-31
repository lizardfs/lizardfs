#include "common/cstoma_communication.h"

#include <gtest/gtest.h>

#include "common/crc.h"
#include "common/strerr.h"
#include "unittests/inout_pair.h"
#include "unittests/operators.h"
#include "unittests/packet.h"

TEST(CstomaCommunicationTests, OverwriteStatusField) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 0xFFFFFFFFFFFFFFFF, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, ChunkType::getXorParityChunkType(3), ChunkType::getStandardChunkType());
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t, status, 2, 2);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::serializeStatus(buffer, LIZ_CSTOMA_SET_VERSION, chunkIdIn, chunkTypeIn, statusIn));
	statusIn = ERROR_WRONGOFFSET;
	cstoma::overwriteStatusField(buffer, statusIn);

	verifyHeader(buffer, LIZ_CSTOMA_SET_VERSION);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstoma::setVersion::deserialize(buffer,
				chunkIdOut, chunkTypeOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(CstomaCommunicationTests, RegisterHost) {
	uint32_t outIp, inIp = 127001;
	uint16_t outPort, inPort = 8080;
	uint16_t outTimeout, inTimeout = 1;
	uint32_t outCSVersion, inCSVersion = VERSHEX;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::registerHost::serialize(buffer,
			inIp, inPort, inTimeout, inCSVersion));

	verifyHeader(buffer, LIZ_CSTOMA_REGISTER_HOST);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstoma::registerHost::deserialize(buffer,
			outIp, outPort, outTimeout, outCSVersion));

	EXPECT_EQ(inIp, outIp);
	EXPECT_EQ(inPort, outPort);
	EXPECT_EQ(inTimeout, outTimeout);
	EXPECT_EQ(inCSVersion, outCSVersion);
}

TEST(CstomaCommunicationTests, RegisterChunks) {
	std::vector<ChunkWithVersionAndType> outChunks {
		ChunkWithVersionAndType(0, 1000, ChunkType::getXorChunkType(3, 1)),
		ChunkWithVersionAndType(1, 1001, ChunkType::getXorChunkType(7, 7)),
		ChunkWithVersionAndType(2, 1002, ChunkType::getXorParityChunkType(9)),
		ChunkWithVersionAndType(3, 1003, ChunkType::getStandardChunkType())
	};
	std::vector<ChunkWithVersionAndType> inChunks;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::registerChunks::serialize(buffer, outChunks));

	verifyHeader(buffer, LIZ_CSTOMA_REGISTER_CHUNKS);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstoma::registerChunks::deserialize(buffer, inChunks));

	EXPECT_EQ(inChunks.size(), outChunks.size());
	for (size_t i = 0; i < outChunks.size(); ++i) {
		SCOPED_TRACE(std::string("Checking chunk number ") + std::to_string(i));
		EXPECT_EQ(outChunks[i].id, inChunks[i].id);
		EXPECT_EQ(outChunks[i].version, inChunks[i].version);
		EXPECT_EQ(outChunks[i].type, inChunks[i].type);
	}
}

TEST(CstomaCommunicationTests, RegisterSpace) {
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

	verifyHeader(buffer, LIZ_CSTOMA_REGISTER_SPACE);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstoma::registerSpace::deserialize(buffer,
			usedSpace[1], totalSpace[1], chunksNumber[1], tdUsedSpace[1], toDeleteTotalSpace[1],
			toDeleteChunksNumber[1]));

	EXPECT_EQ(usedSpace[0], usedSpace[1]);
	EXPECT_EQ(totalSpace[0], totalSpace[1]);
	EXPECT_EQ(chunksNumber[0], chunksNumber[1]);
	EXPECT_EQ(tdUsedSpace[0], tdUsedSpace[1]);
	EXPECT_EQ(toDeleteTotalSpace[0], toDeleteTotalSpace[1]);
	EXPECT_EQ(toDeleteChunksNumber[0], toDeleteChunksNumber[1]);
}

TEST(CstomaCommunicationTests, SetVersion) {
	uint64_t chunkIdOut, chunkIdIn = 62443697;
	ChunkType chunkTypeOut = ChunkType::getStandardChunkType();
	ChunkType chunkTypeIn = ChunkType::getXorChunkType(3, 2);
	uint8_t statusOut, statusIn = 17;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::setVersion::serialize(buffer, chunkIdIn, chunkTypeIn, statusIn));

	verifyHeader(buffer, LIZ_CSTOMA_SET_VERSION);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstoma::setVersion::deserialize(buffer, chunkIdOut, chunkTypeOut, statusOut));

	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(chunkTypeIn, chunkTypeOut);
	EXPECT_EQ(statusIn, statusOut);
}

TEST(CstomaCommunicationTests, DeleteChunk) {
	uint64_t chunkIdOut, chunkIdIn = 62443697;
	ChunkType chunkTypeOut = ChunkType::getStandardChunkType();
	ChunkType chunkTypeIn = ChunkType::getXorChunkType(3, 2);
	uint8_t statusOut, statusIn = 17;

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::deleteChunk::serialize(buffer, chunkIdIn, chunkTypeIn, statusIn));

	verifyHeader(buffer, LIZ_CSTOMA_DELETE_CHUNK);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstoma::deleteChunk::deserialize(buffer, chunkIdOut, chunkTypeOut, statusOut));

	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(chunkTypeIn, chunkTypeOut);
	EXPECT_EQ(statusIn, statusOut);
}

TEST(CstomaCommunicationTests, Replicate) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 0xFFFFFFFFFFFFFFFF, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 0x87654321, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, ChunkType::getXorParityChunkType(3), ChunkType::getStandardChunkType());
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t, status, 2, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstoma::replicate::serialize(buffer,
			chunkIdIn, chunkVersionIn, chunkTypeIn, statusIn));

	verifyHeader(buffer, LIZ_CSTOMA_REPLICATE);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cstoma::replicate::deserialize(buffer,
			chunkIdOut, chunkVersionOut, chunkTypeOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}
