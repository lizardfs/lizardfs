#include "common/cstocl_communication.h"

#include <gtest/gtest.h>

#include "common/crc.h"
#include "unittests/inout_pair.h"
#include "unittests/packet.h"

TEST(CltocsCommunicationTests, ReadData) {
	std::vector<uint8_t> buffer;
	uint64_t chunkIdOut = 0, chunkIdIn = 0x0123456789ABCDEF;
	uint32_t readOffsetOut = 0, readOffsetIn = 2 * MFSBLOCKSIZE;
	uint32_t readSizeOut = 0, readSizeIn = 5 * MFSBLOCKSIZE;
	std::vector<uint8_t> dataOut, dataIn = {0x10, 0x20, 0x30, 0x40};
	uint32_t crcOut = 0, crcIn = mycrc32(0, dataIn.data(), dataIn.size());

	ASSERT_NO_THROW(cstocl::readData::serialize(buffer, chunkIdIn, readOffsetIn, readSizeIn, crcIn,
			dataIn));

	verifyHeader(buffer, LIZ_CSTOCL_READ_DATA);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstocl::readData::deserialize(buffer, chunkIdOut, readOffsetOut,
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

	verifyHeader(buffer, LIZ_CSTOCL_READ_STATUS);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(cstocl::readStatus::deserialize(buffer, chunkIdOut, statusOut));

	EXPECT_EQ(chunkIdIn, chunkIdOut);
	EXPECT_EQ(statusIn, statusOut);
}

TEST(CltocsCommunicationTests, WriteStatus) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId,  0x987654321, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, writeId,  0x12345,     0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t,  status,   12,          0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstocl::writeStatus::serialize(buffer, chunkIdIn, writeIdIn, statusIn));

	verifyHeader(buffer, LIZ_CSTOCL_WRITE_STATUS);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cstocl::writeStatus::deserialize(buffer, chunkIdOut, writeIdOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(writeId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);

}
