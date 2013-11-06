#include "common/crc.h"

#include <string>
#include <gtest/gtest.h>

#include "common/MFSCommunication.h"

TEST(CrcTests, MyCrc32) {
	std::vector<std::pair<std::string, uint32_t>> data {
		// calculated by http://www.miniwebtool.com/crc32-checksum-calculator
		std::make_pair("a", 0xE8B7BE43),
		std::make_pair("aa", 0x78A19D7),
		std::make_pair("aaaa", 0xAD98E545),
		std::make_pair("aaaaaaaa", 0xBF848046),
		std::make_pair("aaaaaaaaaaaaaaaa", 0xCFD668D5),
		std::make_pair("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0xCAB11777),
		std::make_pair("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0x89B46555),
	};
	for (auto& pair : data) {
		EXPECT_EQ(pair.second, mycrc32(0, (const uint8_t*)pair.first.data(), pair.first.size()));
	}
}

TEST(CrcTests, MfsCrcEmpty) {
	std::vector<uint8_t>data(MFSBLOCKSIZE);
	EXPECT_EQ(MFSCRCEMPTY, mycrc32(0, data.data(), MFSBLOCKSIZE));
}

TEST(CrcTests, MfsCrc32Zeroblock) {
	EXPECT_EQ(MFSCRCEMPTY, mycrc32_zeroblock(0, MFSBLOCKSIZE));
}

TEST(CrcTests, MyCrc32Combine) {
	std::vector<uint8_t> data(MFSBLOCKSIZE);
	for (size_t i = 0; i < data.size(); ++i) {
		data[i] = i;
	}
	uint32_t crc = mycrc32(0, data.data(), data.size());
	for (size_t length : {1, 50, 100, 1024, 1025, 32767, 32768, 32769, 65534, 65535}) {
		SCOPED_TRACE("Testing combine for length=" + std::to_string(length));
		uint32_t crc1 = mycrc32(0, data.data(), data.size() - length);
		uint32_t crc2 = mycrc32(0, data.data() + data.size() - length, length);
		uint32_t combined = mycrc32_combine(crc1, crc2, length);
		EXPECT_EQ(crc, combined);
	}
}
