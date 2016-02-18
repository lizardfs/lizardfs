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
#include "protocol/cltoma.h"

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
	ASSERT_NO_THROW(cltoma::chunksHealth::serialize(buffer, regularIn));

	verifyHeader(buffer, LIZ_CLTOMA_CHUNKS_HEALTH);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::chunksHealth::deserialize(buffer, regularOut));

	LIZARDFS_VERIFY_INOUT_PAIR(regular);
}

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
	AccessControlList aclIn, aclOut;
	aclIn.setMode(0750);
	aclIn.setEntry(AccessControlList::kNamedGroup, 123, 7);

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
	EXPECT_EQ(aclIn, aclOut);
}
