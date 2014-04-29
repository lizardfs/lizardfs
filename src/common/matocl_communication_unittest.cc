#include "common/matocl_communication.h"

#include <gtest/gtest.h>

#include "unittests/chunk_type_constants.h"
#include "unittests/inout_pair.h"
#include "unittests/operators.h"
#include "unittests/packet.h"

TEST(MatoclCommunicationTests, FuseReadChunkData) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId,    512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId,      87,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 52,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, fileLength,   124, 0);
	LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(ChunkTypeWithAddress, serverList) = {
		ChunkTypeWithAddress(NetworkAddress(0xC0A80001, 8080), standard),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80002, 8081), xor_p_of_6),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80003, 8082), xor_1_of_6),
		ChunkTypeWithAddress(NetworkAddress(0xC0A80004, 8084), xor_5_of_7),
	};

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseReadChunk::serialize(buffer,
			messageIdIn, fileLengthIn, chunkIdIn, chunkVersionIn, serverListIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_READ_CHUNK);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, matocl::fuseReadChunk::kResponsePacketVersion);
	ASSERT_NO_THROW(deserializePacketDataNoHeader(buffer, messageIdOut));
	ASSERT_NO_THROW(matocl::fuseReadChunk::deserialize(buffer,
			fileLengthOut, chunkIdOut, chunkVersionOut, serverListOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(fileLength);
	LIZARDFS_VERIFY_INOUT_PAIR(serverList);
}

TEST(MatoclCommunicationTests, FuseReadChunkStatus) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t,  status,    10,  0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseReadChunk::serialize(buffer, messageIdIn, statusIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_READ_CHUNK);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, matocl::fuseReadChunk::kStatusPacketVersion);
	ASSERT_NO_THROW(deserializePacketDataNoHeader(buffer, messageIdOut));
	ASSERT_NO_THROW(matocl::fuseReadChunk::deserialize(buffer, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(MatoclCommunicationTests, FuseWriteChunkData) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId,    512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, chunkId,      87,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, chunkVersion, 52,  0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, fileLength,   124, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, lockId,       225, 0);
	LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(ChunkTypeWithAddress, serverList) = {
			ChunkTypeWithAddress(NetworkAddress(0xC0A80001, 8080), standard),
			ChunkTypeWithAddress(NetworkAddress(0xC0A80002, 8081), xor_p_of_6),
			ChunkTypeWithAddress(NetworkAddress(0xC0A80003, 8082), xor_1_of_6),
			ChunkTypeWithAddress(NetworkAddress(0xC0A80004, 8084), xor_5_of_7),
	};

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseWriteChunk::serialize(buffer,
			messageIdIn, fileLengthIn, chunkIdIn, chunkVersionIn, lockIdIn, serverListIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_WRITE_CHUNK);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, matocl::fuseWriteChunk::kResponsePacketVersion);
	ASSERT_NO_THROW(deserializePacketDataNoHeader(buffer, messageIdOut));
	ASSERT_NO_THROW(matocl::fuseWriteChunk::deserialize(buffer,
			fileLengthOut, chunkIdOut, chunkVersionOut, lockIdOut, serverListOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkId);
	LIZARDFS_VERIFY_INOUT_PAIR(chunkVersion);
	LIZARDFS_VERIFY_INOUT_PAIR(fileLength);
	LIZARDFS_VERIFY_INOUT_PAIR(serverList);
}

TEST(MatoclCommunicationTests, FuseWriteChunkStatus) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t,  status,    10,  0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseWriteChunk::serialize(buffer, messageIdIn, statusIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_WRITE_CHUNK);
	removeHeaderInPlace(buffer);
	verifyVersion(buffer, matocl::fuseReadChunk::kStatusPacketVersion);
	ASSERT_NO_THROW(deserializePacketDataNoHeader(buffer, messageIdOut));
	ASSERT_NO_THROW(matocl::fuseWriteChunk::deserialize(buffer, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(MatoclCommunicationTests, FuseWriteChunkEnd) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 512, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t,  status,    10,  0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseWriteChunkEnd::serialize(buffer, messageIdIn, statusIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_WRITE_CHUNK_END);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(deserializePacketDataNoHeader(buffer, messageIdOut));
	ASSERT_NO_THROW(matocl::fuseWriteChunkEnd::deserialize(buffer, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(MatoclCommunicationTests, XorChunksHealth) {
	LIZARDFS_DEFINE_INOUT_PAIR(bool, regular, true, false);
	ChunksAvailabilityState availIn, availOut;
	ChunksReplicationState replIn, replOut;
	std::vector<uint8_t> goals = {0};
	for (uint8_t i = kMinOrdinaryGoal; i <= kMaxOrdinaryGoal; ++i) {
		goals.push_back(i);
	}
	for (ChunkType::XorLevel level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
		goals.push_back(xorLevelToGoal(level));
	}

	availIn.addChunk(0, ChunksAvailabilityState::kSafe);
	availIn.addChunk(1, ChunksAvailabilityState::kEndangered);
	availIn.addChunk(xorLevelToGoal(2), ChunksAvailabilityState::kEndangered);
	availIn.addChunk(xorLevelToGoal(3), ChunksAvailabilityState::kLost);
	availIn.addChunk(xorLevelToGoal(4), ChunksAvailabilityState::kSafe);

	replIn.addChunk(0, 0, 1);
	replIn.addChunk(2, 1, 0);
	replIn.addChunk(xorLevelToGoal(2), 2, 10);
	replIn.addChunk(xorLevelToGoal(3), 15, 5);
	replIn.addChunk(xorLevelToGoal(4), 12, 13);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::xorChunksHealth::serialize(buffer, regularIn, availIn, replIn));

	verifyHeader(buffer, LIZ_MATOCL_CHUNKS_HEALTH);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::xorChunksHealth::deserialize(buffer, regularOut, availOut, replOut));

	LIZARDFS_VERIFY_INOUT_PAIR(regular);
	for (uint8_t goal : goals) {
		EXPECT_EQ(availIn.safeChunks(goal), availOut.safeChunks(goal));
		EXPECT_EQ(availIn.endangeredChunks(goal), availOut.endangeredChunks(goal));
		EXPECT_EQ(availIn.lostChunks(goal), availOut.lostChunks(goal));

		for (uint32_t part = 0; part <= ChunksReplicationState::kMaxPartsCount; ++part) {
			EXPECT_EQ(replIn.chunksToReplicate(goal, part), replOut.chunksToReplicate(goal, part));
			EXPECT_EQ(replIn.chunksToDelete(goal, part), replOut.chunksToDelete(goal, part));
		}
	}
}

TEST(MatoclCommunicationTests, FuseDeleteAcl) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 123, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t, status, ERROR_EPERM, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseDeleteAcl::serialize(buffer, messageIdIn, statusIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_DELETE_ACL);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::fuseDeleteAcl::deserialize(buffer.data(), buffer.size(),
			messageIdOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(MatoclCommunicationTests, FuseGetAclStatus) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 123, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t, status, ERROR_EPERM, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseGetAcl::serialize(buffer, messageIdIn, statusIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_GET_ACL);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::fuseGetAcl::deserialize(buffer.data(), buffer.size(),
			messageIdOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(MatoclCommunicationTests, FuseGetAclResponse) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 123, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(AccessControlList, acl, 0750, 0000);
	aclIn.extendedAcl.reset(new ExtendedAcl(5));
	aclIn.extendedAcl->addNamedGroup(123, 7);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseGetAcl::serialize(buffer, messageIdIn, aclIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_GET_ACL);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::fuseGetAcl::deserialize(buffer.data(), buffer.size(),
			messageIdOut, aclOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	EXPECT_EQ(aclIn.mode, aclOut.mode);
	EXPECT_EQ(aclIn.extendedAcl->owningGroupMask(), aclOut.extendedAcl->owningGroupMask());
	EXPECT_EQ(aclIn.extendedAcl->list(), aclOut.extendedAcl->list());
}

TEST(MatoclCommunicationTests, FuseSetAcl) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 123, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t, status, ERROR_EPERM, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::fuseSetAcl::serialize(buffer, messageIdIn, statusIn));

	verifyHeader(buffer, LIZ_MATOCL_FUSE_SET_ACL);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::fuseSetAcl::deserialize(buffer.data(), buffer.size(),
			messageIdOut, statusOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(status);
}

TEST(MatoclCommunicationTests, IoLimitsConfig) {
	std::vector<std::string> groups_tmp{"group 1", "group 20", "group 300"};

	LIZARDFS_DEFINE_INOUT_PAIR(std::string             , subsystem, "cgroups_something", "");
	LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(std::string      , groups) = groups_tmp;
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t                , frequency, 100                , 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::iolimits_config::serialize(buffer, subsystemIn, groupsIn,
			frequencyIn));

	verifyHeader(buffer, LIZ_MATOCL_IOLIMITS_CONFIG);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::iolimits_config::deserialize(buffer.data(), buffer.size(),
			subsystemOut, groupsOut, frequencyOut));

	LIZARDFS_VERIFY_INOUT_PAIR(subsystem);
	LIZARDFS_VERIFY_INOUT_PAIR(groups);
	LIZARDFS_VERIFY_INOUT_PAIR(frequency);
}

TEST(MatoclCommunicationTests, IoLimitAllocated) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::string, group, "group 123", "");
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, limit, 1234, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::iolimit::serialize(buffer, groupIn, limitIn));

	verifyHeader(buffer, LIZ_MATOCL_IOLIMIT);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::iolimit::deserialize(buffer.data(), buffer.size(),
			groupOut, limitOut));

	LIZARDFS_VERIFY_INOUT_PAIR(limit);
	LIZARDFS_VERIFY_INOUT_PAIR(group);
}
