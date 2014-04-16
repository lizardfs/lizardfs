#include "common/cltoma_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"
#include "unittests/inout_pair.h"

TEST(CltomaCommunicationTests, FuseDeleteAcl) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 123, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, inode, 456, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, uid, 789, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, gid, 1011, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(AclType, type, AclType::kDefault, AclType::kAccess);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseDeleteAcl::serialize(buffer,
			messageIdIn, inodeIn, uidIn, gidIn, typeIn));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_DELETE_ACL);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::fuseDeleteAcl::deserialize(buffer.data(), buffer.size(),
			messageIdOut, inodeOut, uidOut, gidOut, typeOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(inode);
	LIZARDFS_VERIFY_INOUT_PAIR(uid);
	LIZARDFS_VERIFY_INOUT_PAIR(gid);
	LIZARDFS_VERIFY_INOUT_PAIR(type);
}

TEST(CltomaCommunicationTests, FuseGetAcl) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 123, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, inode, 456, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, uid, 789, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, gid, 1011, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(AclType, type, AclType::kDefault, AclType::kAccess);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseGetAcl::serialize(buffer,
			messageIdIn, inodeIn, uidIn, gidIn, typeIn));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_GET_ACL);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::fuseGetAcl::deserialize(buffer.data(), buffer.size(),
			messageIdOut, inodeOut, uidOut, gidOut, typeOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(inode);
	LIZARDFS_VERIFY_INOUT_PAIR(uid);
	LIZARDFS_VERIFY_INOUT_PAIR(gid);
	LIZARDFS_VERIFY_INOUT_PAIR(type);
}

TEST(CltomaCommunicationTests, FuseSetAcl) {
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 123, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, inode, 456, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, uid, 789, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, gid, 1011, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(AclType, type, AclType::kDefault, AclType::kAccess);
	LIZARDFS_DEFINE_INOUT_PAIR(AccessControlList, acl, 0750, 0000);
	aclIn.extendedAcl.reset(new ExtendedAcl(5));
	aclIn.extendedAcl->addNamedGroup(123, 7);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::fuseSetAcl::serialize(buffer,
			messageIdIn, inodeIn, uidIn, gidIn, typeIn, aclIn));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_SET_ACL);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::fuseSetAcl::deserialize(buffer.data(), buffer.size(),
			messageIdOut, inodeOut, uidOut, gidOut, typeOut, aclOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(inode);
	LIZARDFS_VERIFY_INOUT_PAIR(uid);
	LIZARDFS_VERIFY_INOUT_PAIR(gid);
	LIZARDFS_VERIFY_INOUT_PAIR(type);
	EXPECT_EQ(aclIn.mode, aclOut.mode);
	EXPECT_EQ(aclIn.extendedAcl->owningGroupMask(), aclOut.extendedAcl->owningGroupMask());
	EXPECT_EQ(aclIn.extendedAcl->list(), aclOut.extendedAcl->list());
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
