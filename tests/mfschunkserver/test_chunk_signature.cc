#include "chunkserver/chunk_signature.h"

#include <fcntl.h>
#include <unistd.h>
#include <fstream>
#include <string>
#include <gtest/gtest.h>

#include "common/chunk_type.h"
#include "tests/common/TemporaryDirectory.h"

TEST(ChunkSignatureTests, ReadingFromFile) {
	const size_t signatureOffset = 5;
	const uint64_t chunkId = 0x0102030405060708;
	const uint32_t version = 0x04030201;
	const uint8_t chunkTypeId = ChunkType::getXorChunkType(3, 1).chunkTypeId();

	// For the data listed above the contents should look like this:
	std::vector<uint8_t> chunkFileContents = {
		0, 1, 2, 3, 4,                          // 5 (headerOffset) bytes of garbage
		'L', 'I', 'Z', 'C', ' ', '1', '.', '0', // signature = LIZC 1.0
		1, 2, 3, 4, 5, 6, 7, 8,                 // id        = 0x0102030405060708
		4, 3, 2, 1,                             // version   = 0x04030201
		chunkTypeId,                            // type ID
	};

	// Create a file
	TemporaryDirectory temp("/tmp", this->test_info_->name());
	std::string chunkFileName(temp.name() + "/" + "chunk");
	std::ofstream file(chunkFileName);
	ASSERT_TRUE(file) << "Cannot create a file " << chunkFileName;
	file.write(reinterpret_cast<const char*>(chunkFileContents.data()), chunkFileContents.size());
	file.close();
	ASSERT_TRUE(file) << "Cannot write data to file " << chunkFileName;

	// Open file and read header contents
	int fd = open(chunkFileName.c_str(), O_RDONLY);
	ASSERT_NE(fd, -1) << "Cannot open file " << chunkFileName << " after creating it";

	ChunkSignature chunkSignature;
	ASSERT_TRUE(chunkSignature.readFromDescriptor(fd, signatureOffset)) << "Cannot read signature";
	ASSERT_TRUE(chunkSignature.hasValidSignatureId());
	ASSERT_EQ(chunkId, chunkSignature.chunkId());
	ASSERT_EQ(version, chunkSignature.chunkVersion());
	ASSERT_EQ(chunkTypeId, chunkSignature.chunkTypeId());
}
