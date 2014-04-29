#include "config.h"
#include "common/matocs_communication.h"

#include <gtest/gtest.h>

#include "unittests/chunk_type_constants.h"
#include "unittests/inout_pair.h"
#include "unittests/operators.h"
#include "unittests/packet.h"

TEST(MatocsCommunicationTests, SetVersion) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 87,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 52,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, xor_p_of_3, standard);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, newVersion, 53,  0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocs::setVersion::serialize(buffer,
			chunkIdIn, chunkTypeIn, chunkVersionIn, newVersionIn));

	verifyHeader(buffer, LIZ_MATOCS_SET_VERSION);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, 0U);
	ASSERT_NO_THROW(matocs::setVersion::deserialize(buffer,
			chunkIdOut, chunkTypeOut, chunkVersionOut, newVersionOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
	LIZARDFS_VERIFY_INOUT_PAIR(newVersion);
}

TEST(MatocsCommunicationTests, DeleteChunk) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 87,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 52,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, xor_p_of_3, standard);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocs::deleteChunk::serialize(buffer,
			chunkIdIn, chunkTypeIn, chunkVersionIn));

	verifyHeader(buffer, LIZ_MATOCS_DELETE_CHUNK);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocs::deleteChunk::deserialize(buffer,
			chunkIdOut, chunkTypeOut, chunkVersionOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
}

TEST(MatocsCommunicationTests, Replicate) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 87,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 52,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, xor_p_of_3, standard);
	LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(ChunkTypeWithAddress, serverList) = {
		ChunkTypeWithAddress(NetworkAddress(0xC0A80001, 8080), standard),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80002, 8081), xor_p_of_6),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80003, 8082), xor_1_of_6),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80004, 8084), xor_5_of_7),
	};

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocs::replicate::serialize(buffer,
			chunkIdIn, chunkVersionIn, chunkTypeIn, serverListIn));

	verifyHeader(buffer, LIZ_MATOCS_REPLICATE);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocs::replicate::deserialize(buffer,
			chunkIdOut, chunkVersionOut, chunkTypeOut, serverListOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
	LIZARDFS_VERIFY_INOUT_PAIR(serverList);
}
