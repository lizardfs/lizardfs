#include "common/platform.h"
#include "common/crc.h"

#include <string>
#include <gtest/gtest.h>

#include "common/LFSCommunication.h"

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

TEST(CrcTests, LfsCrcEmpty) {
	std::vector<uint8_t>data(LFSBLOCKSIZE);
	EXPECT_EQ(LFSCRCEMPTY, mycrc32(0, data.data(), LFSBLOCKSIZE));
}

TEST(CrcTests, LfsCrc32Zeroblock) {
	EXPECT_EQ(LFSCRCEMPTY, mycrc32_zeroblock(0, LFSBLOCKSIZE));
}

TEST(CrcTests, MyCrc32Combine) {
	std::vector<uint8_t> data(LFSBLOCKSIZE);
	for (size_t i = 0; i < data.size(); ++i) {
		data[i] = i;
	}
	uint32_t crc = mycrc32(0, data.data(), data.size());
	for (size_t length = 2; length < LFSBLOCKSIZE; length *= 2) {
		for (int8_t offset : {-1, 0, 1}) { // (1, 2, 3), (3, 4, 5), (7, 8, 9), (15, 16, 17)...
			SCOPED_TRACE("LFSBLOCKSIZE = " + std::to_string(LFSBLOCKSIZE) + ". Testing combine for length=" + std::to_string(length + offset));
			uint32_t crc1 = mycrc32(0, data.data(), data.size() - (length + offset));
			uint32_t crc2 = mycrc32(0, data.data() + data.size() - (length + offset), (length + offset));
			uint32_t combined = mycrc32_combine(crc1, crc2, (length + offset));
			EXPECT_EQ(crc, combined);
		}
	}
}
