/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "common/crc.h"

#include <string>
#include <gtest/gtest.h>

#include "protocol/MFSCommunication.h"

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

TEST(CrcTests, MfsCrc32Zeroblock) {
	std::vector<uint8_t>data(MFSBLOCKSIZE);
	EXPECT_EQ(mycrc32(0, data.data(), MFSBLOCKSIZE), mycrc32_zeroblock(0, MFSBLOCKSIZE));
}

TEST(CrcTests, MyCrc32Combine) {
	std::vector<uint8_t> data(MFSBLOCKSIZE);
	for (size_t i = 0; i < data.size(); ++i) {
		data[i] = i;
	}
	uint32_t crc = mycrc32(0, data.data(), data.size());
	for (size_t length = 2; length < MFSBLOCKSIZE; length *= 2) {
		for (int8_t offset : {-1, 0, 1}) { // (1, 2, 3), (3, 4, 5), (7, 8, 9), (15, 16, 17)...
			SCOPED_TRACE("MFSBLOCKSIZE = " + std::to_string(MFSBLOCKSIZE) + ". Testing combine for length=" + std::to_string(length + offset));
			uint32_t crc1 = mycrc32(0, data.data(), data.size() - (length + offset));
			uint32_t crc2 = mycrc32(0, data.data() + data.size() - (length + offset), (length + offset));
			uint32_t combined = mycrc32_combine(crc1, crc2, (length + offset));
			EXPECT_EQ(crc, combined);
		}
	}
}
