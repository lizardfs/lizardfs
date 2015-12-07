/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include "chunkserver/chunk_signature.h"

#include <fcntl.h>
#include <unistd.h>
#include <fstream>
#include <string>
#include <gtest/gtest.h>

#include "common/chunk_part_type.h"
#include "common/slice_traits.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"
#include "unittests/TemporaryDirectory.h"

TEST(ChunkSignatureTests, ReadingOldSignatureFromFile) {
	const size_t signatureOffset = 5;
	const uint64_t chunkId = 0x0102030405060708;
	const uint32_t version = 0x04030201;
	const uint8_t chunkTypeId = legacy::ChunkPartType(slice_traits::xors::getSliceType(4), 1).getId();

	// For the data listed above the contents should look like this:
	std::vector<uint8_t> chunkFileContents = {
		0, 1, 2, 3, 4,                          // 5 (headerOffset) bytes of garbage
		'L', 'I', 'Z', 'C', ' ', '1', '.', '0', // signature = LIZC 1.0
		1, 2, 3, 4, 5, 6, 7, 8,                 // id        = 0x0102030405060708
		4, 3, 2, 1,                             // version   = 0x04030201
		chunkTypeId,                            // legacy type ID
		0                                       // padding
	};

	// Create a file
	TemporaryDirectory temp("/tmp", this->test_info_->name());
	std::string chunkFileName(temp.name() + "/" + "chunk");
	std::ofstream file(chunkFileName);
	ASSERT_TRUE((bool)file) << "Cannot create a file " << chunkFileName;
	file.write(reinterpret_cast<const char*>(chunkFileContents.data()), chunkFileContents.size());
	file.close();
	ASSERT_TRUE((bool)file) << "Cannot write data to file " << chunkFileName;

	// Open file and read header contents
	int fd = open(chunkFileName.c_str(), O_RDONLY);
	ASSERT_NE(fd, -1) << "Cannot open file " << chunkFileName << " after creating it";

	ChunkSignature chunkSignature;
	ASSERT_TRUE(chunkSignature.readFromDescriptor(fd, signatureOffset)) << "Cannot read signature";
	ASSERT_TRUE(chunkSignature.hasValidSignatureId());
	ASSERT_EQ(chunkId, chunkSignature.chunkId());
	ASSERT_EQ(version, chunkSignature.chunkVersion());
	ASSERT_EQ(slice_traits::xors::ChunkPartType(4, 1), chunkSignature.chunkType());
}


TEST(ChunkSignatureTests, ReadingFromFile) {
	const size_t signatureOffset = 5;
	const uint64_t chunkId = 0x0102030405060708;
	const uint32_t version = 0x04030201;
	const uint16_t chunkTypeId = slice_traits::xors::ChunkPartType(3, 1).getId();

	// For the data listed above the contents should look like this:
	std::vector<uint8_t> chunkFileContents = {
		0, 1, 2, 3, 4,                          // 5 (headerOffset) bytes of garbage
		'L', 'I', 'Z', 'C', ' ', '1', '.', '1', // signature = LIZC 1.1
		1, 2, 3, 4, 5, 6, 7, 8,                 // id        = 0x0102030405060708
		4, 3, 2, 1,                             // version   = 0x04030201
		(uint8_t)(chunkTypeId >> 8),            // type ID (hi byte)
		(uint8_t)(chunkTypeId & 0xFF)           // type ID (lo byte)
	};

	// Create a file
	TemporaryDirectory temp("/tmp", this->test_info_->name());
	std::string chunkFileName(temp.name() + "/" + "chunk");
	std::ofstream file(chunkFileName);
	ASSERT_TRUE((bool)file) << "Cannot create a file " << chunkFileName;
	file.write(reinterpret_cast<const char*>(chunkFileContents.data()), chunkFileContents.size());
	file.close();
	ASSERT_TRUE((bool)file) << "Cannot write data to file " << chunkFileName;

	// Open file and read header contents
	int fd = open(chunkFileName.c_str(), O_RDONLY);
	ASSERT_NE(fd, -1) << "Cannot open file " << chunkFileName << " after creating it";

	ChunkSignature chunkSignature;
	ASSERT_TRUE(chunkSignature.readFromDescriptor(fd, signatureOffset)) << "Cannot read signature";
	ASSERT_TRUE(chunkSignature.hasValidSignatureId());
	ASSERT_EQ(chunkId, chunkSignature.chunkId());
	ASSERT_EQ(version, chunkSignature.chunkVersion());
	ASSERT_EQ(chunkTypeId, chunkSignature.chunkType().getId());
}

// This test verifies if signature has proper size, because existing chunks
// created by previous versions of LizardFS have 21-byte signatures.
TEST(ChunkSignatureTests, SerializedSize) {
	ASSERT_EQ(22U, ChunkSignature(0x0102030405060708, 0x04030201, xor_1_of_3).serializedSize());
}

// This test verifies if serialized signature has proper content, because existing chunks
// created by previous versions of LizardFS already have signatures in this format
TEST(ChunkSignatureTests, Serialize) {
	// Serialize some signature
	std::vector<uint8_t> data;
	serialize(data, ChunkSignature(0x0102030405060708, 0x04030201, xor_1_of_3));

	// And test if it looks like it should look like
	std::vector<uint8_t> expectedData = {
			'L', 'I', 'Z', 'C', ' ', '1', '.', '1', // signature = LIZC 1.0
			1, 2, 3, 4, 5, 6, 7, 8,                 // id        = 0x0102030405060708
			4, 3, 2, 1,                             // version   = 0x04030201
			(uint8_t)(xor_1_of_3.getId() >> 8),     // type ID (hi byte)
			(uint8_t)(xor_1_of_3.getId() & 0xFF)    // type ID (lo byte)
	};
	ASSERT_EQ(expectedData, data);
}
