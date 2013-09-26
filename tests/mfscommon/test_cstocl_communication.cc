#include "mfscommon/cstocl_communication.h"

#include <gtest/gtest.h>

#include "mfscommon/crc.h"

TEST(CltocsCommunicationTests, ReadData) {
	std::vector<uint8_t> buffer;
	uint64_t chunkIdOut = 0, chunkIdIn = 0x0123456789ABCDEF;
	uint32_t readOffsetOut = 0, readOffsetIn = 2 * MFSBLOCKSIZE;
	uint32_t readSizeOut = 0, readSizeIn = 5 * MFSBLOCKSIZE;
	std::vector<uint8_t> dataOut, dataIn = {0x10, 0x20, 0x30, 0x40};
	uint32_t crcOut = 0, crcIn = mycrc32(0, dataIn.data(), dataIn.size());

	ASSERT_NO_THROW(cstocl::readData::serialize(buffer, chunkIdIn, readOffsetIn, readSizeIn, crcIn,
			dataIn));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_CSTOCL_READ_DATA, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	std::vector<uint8_t> bufferWithoutHeader(
			buffer.begin() + serializedSize(header), buffer.end());
	ASSERT_NO_THROW(cstocl::readData::deserialize(bufferWithoutHeader, chunkIdOut, readOffsetOut,
			readSizeOut, crcOut, dataOut));
	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(readOffsetIn, readOffsetOut);
	EXPECT_EQ(readSizeIn, readSizeOut);
	EXPECT_EQ(crcIn, crcOut);

	for (uint i = 0; i < dataIn.size(); ++i) {
		EXPECT_EQ(dataIn[i], dataOut[i]);
	}
}

TEST(CltocsCommunicationTests, ReadStatus) {
	std::vector<uint8_t> buffer;
	uint64_t chunkIdOut = 0, chunkIdIn = 0x0123456789ABCDEF;
	uint8_t statusOut = 0, statusIn = 0xAB;

	ASSERT_NO_THROW(cstocl::readStatus::serialize(buffer, chunkIdIn, statusIn));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(LIZ_CSTOCL_READ_STATUS, header.type);
	EXPECT_EQ(buffer.size() - serializedSize(header), header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(0U, version);

	std::vector<uint8_t> bufferWithoutHeader(
			buffer.begin() + serializedSize(header), buffer.end());
	ASSERT_NO_THROW(cstocl::readStatus::deserialize(bufferWithoutHeader, chunkIdOut, statusOut));
	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(statusIn, statusOut);
}
