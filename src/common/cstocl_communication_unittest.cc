#include "common/platform.h"
#include "common/cstocl_communication.h"

#include <gtest/gtest.h>

#include "common/crc.h"
#include "unittests/inout_pair.h"
#include "unittests/packet.h"

TEST(CltocsCommunicationTests, ReadData) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 0x0123456789ABCDEF, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, readOffset, 2 * MFSBLOCKSIZE, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, readSize, MFSBLOCKSIZE, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, crc, 0x89ABCDEF, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstocl::readData::serializePrefix(buffer,
			chunkIdIn, readOffsetIn, readSizeIn));
	uint32_t prefixSize = buffer.size();
	buffer.resize(prefixSize + serializedSize(crcIn) + MFSBLOCKSIZE);
	uint8_t* ptr = buffer.data() + prefixSize;
	ASSERT_NO_THROW(serialize(&ptr, crcIn));

	verifyHeader(buffer, LIZ_CSTOCL_READ_DATA);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cstocl::readData::deserializePrefix(buffer,
			chunkIdOut, readOffsetOut, readSizeOut, crcOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(readOffset);
	LIZARDFS_VERIFY_INOUT_PAIR(readSize);
	LIZARDFS_VERIFY_INOUT_PAIR(crc);
}

TEST(CltocsCommunicationTests, ReadStatus) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId,  0x0123456789ABCDEF, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t,  status,   12,                 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstocl::readStatus::serialize(buffer, chunkIdIn, statusIn));

	verifyHeader(buffer, LIZ_CSTOCL_READ_STATUS);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cstocl::readStatus::deserialize(buffer, chunkIdOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(CltocsCommunicationTests, WriteStatus) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId,  0x0123456789ABCDEF, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, writeId,  0x12345678,         0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t,  status,   12,                 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstocl::writeStatus::serialize(buffer, chunkIdIn, writeIdIn, statusIn));

	verifyHeader(buffer, LIZ_CSTOCL_WRITE_STATUS);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cstocl::writeStatus::deserialize(buffer, chunkIdOut, writeIdOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(writeId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}
