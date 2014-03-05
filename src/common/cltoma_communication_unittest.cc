#include "common/cltoma_communication.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"
#include "unittests/packet.h"

TEST(CltomaCommunicationTests, FuseReadChunk) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, inode, 112, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, index, 1583, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseReadChunk::serialize(buffer, messageIdIn, inodeIn, indexIn));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_READ_CHUNK);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::fuseReadChunk::deserialize(buffer, messageIdOut, inodeOut, indexOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(inode);
	LIZARDFS_VERIFY_INOUT_PAIR(index);
}

TEST(CltomaCommunicationTests, FuseWriteChunk) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, inode, 112, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, index, 1583, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, lockId, 986589, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseWriteChunk::serialize(buffer,
			messageIdIn, inodeIn, indexIn, lockIdIn));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_WRITE_CHUNK);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::fuseWriteChunk::deserialize(buffer,
			messageIdOut, inodeOut, indexOut, lockIdOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(inode);
	LIZARDFS_VERIFY_INOUT_PAIR(index);
	LIZARDFS_VERIFY_INOUT_PAIR(lockId);
}

TEST(CltomaCommunicationTests, FuseWriteChunkEnd) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId, 4254, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, lockId, 986589, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, inode, 112, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, fileLength, 1583, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseWriteChunkEnd::serialize(buffer,
			messageIdIn, chunkIdIn, lockIdIn, inodeIn, fileLengthIn));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_WRITE_CHUNK_END);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::fuseWriteChunkEnd::deserialize(buffer,
			messageIdOut, chunkIdOut, lockIdOut, inodeOut, fileLengthOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(inode);
	LIZARDFS_VERIFY_INOUT_PAIR(fileLength);
	LIZARDFS_VERIFY_INOUT_PAIR(lockId);
}

TEST(CltomaCommunicationTests, XorChunksHealth) {
	LIZARDFS_DEFINE_INOUT_PAIR(bool, regular, true, false);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::xorChunksHealth::serialize(buffer, regularIn));

	verifyHeader(buffer, LIZ_CLTOMA_CHUNKS_HEALTH);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::xorChunksHealth::deserialize(buffer, regularOut));

	LIZARDFS_VERIFY_INOUT_PAIR(regular);
}

TEST(CltomaCommunicationTests, IoLimitNeeds) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::string, group       , "group 123", "");
	LIZARDFS_DEFINE_INOUT_PAIR(bool       , wantMore    , true       , false);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t   , currentLimit, 1234       , 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t   , recentUsage , 5678       , 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::iolimit::serialize(buffer, groupIn, wantMoreIn,
			currentLimitIn, recentUsageIn));

	verifyHeader(buffer, LIZ_CLTOMA_IOLIMIT);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::iolimit::deserialize(buffer.data(), buffer.size(),
			groupOut, wantMoreOut, currentLimitOut, recentUsageOut));

	LIZARDFS_VERIFY_INOUT_PAIR(group);
	LIZARDFS_VERIFY_INOUT_PAIR(wantMore);
	LIZARDFS_VERIFY_INOUT_PAIR(currentLimit);
	LIZARDFS_VERIFY_INOUT_PAIR(recentUsage);
}
