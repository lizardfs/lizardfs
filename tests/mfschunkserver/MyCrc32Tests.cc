/*
 * MyCrc32Tests.cc
 *
 *  Created on: 03-07-2013
 *      Author: Marcin Sulikowski
 */

#include <cstdlib>
#include <string>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>
#include <gtest/gtest.h>

#include "common/crc.h"
#include "tests/common/TemporaryDirectory.h"

TEST(MyCrc32Tests, MyCrc32Combine) {
	std::vector<std::vector<uint8_t>> buffers;
	std::vector<uint8_t> joinedBuffer;

	// Create some buffers of random length and one buffer which contains all the data joined together
	buffers.resize(20);
	for (unsigned i = 0; i < buffers.size(); ++i) {
		buffers[i].resize(1 + rand() % 10000);
		for (unsigned j = 0; j < buffers[i].size(); ++j) {
			buffers[i][j] = rand();
		}
		joinedBuffer.insert(joinedBuffer.end(), buffers[i].begin(), buffers[i].end());
	}

	mycrc32_init();

	// Compute CRC32 of all the data combining checksums of individual blocks
	uint32_t crc = 0;
	for (unsigned i = 0; i < buffers.size(); ++i) {
		uint32_t blockCrc = mycrc32(0, &buffers[i][0], buffers[i].size());
		crc = mycrc32_combine(crc, blockCrc, buffers[i].size());
	}

	// Compute CRC32 of all the data using one mycrc32 call
	uint32_t joinedCrc = mycrc32(0, &joinedBuffer[0], joinedBuffer.size());

	// Check if checksums computed in both ways are equal
    ASSERT_EQ(joinedCrc, crc);
}
