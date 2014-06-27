#include "common/platform.h"
#include "common/cstocs_communication.h"

#include <gtest/gtest.h>

#include "unittests/chunk_type_constants.h"
#include "unittests/inout_pair.h"
#include "unittests/operators.h"
#include "unittests/packet.h"

TEST(CstocsCommunicationTests, GetChunkBlocks) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 0x0123456789ABCDEF, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 0x01234567, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, xor_2_of_6, standard);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstocs::getChunkBlocks::serialize(buffer,
			chunkIdIn, chunkVersionIn, chunkTypeIn));

	verifyHeader(buffer, LIZ_CSTOCS_GET_CHUNK_BLOCKS);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cstocs::getChunkBlocks::deserialize(buffer.data(), buffer.size(),
			chunkIdOut, chunkVersionOut, chunkTypeOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
}

TEST(CstocsCommunicationTests, GetChunkBlocksStatus) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 0x0123456789ABCDEF, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 0x01234567, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(ChunkType, chunkType, xor_p_of_7, standard);
	LIZARDFS_DEFINE_INOUT_PAIR(uint16_t, blocks, 0xFEED, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t, status, 123, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cstocs::getChunkBlocksStatus::serialize(buffer,
			chunkIdIn, chunkVersionIn, chunkTypeIn, blocksIn, statusIn));

	verifyHeader(buffer, LIZ_CSTOCS_GET_CHUNK_BLOCKS_STATUS);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cstocs::getChunkBlocksStatus::deserialize(buffer,
			chunkIdOut, chunkVersionOut, chunkTypeOut, blocksOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkType);
	LIZARDFS_VERIFY_INOUT_PAIR(blocks);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}
