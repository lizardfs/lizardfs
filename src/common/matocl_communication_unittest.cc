#include "common/matocl_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"
#include "unittests/inout_pair.h"

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
