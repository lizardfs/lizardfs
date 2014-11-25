#include "common/platform.h"
#include "common/serialization_macros.h"

#include <tuple>
#include <gtest/gtest.h>

#include "common/lizardfs_string.h"
#include "unittests/inout_pair.h"
#include "unittests/packet.h"


class Base {};
SERIALIZABLE_CLASS_BEGIN(SomeClass : public Base)
SERIALIZABLE_CLASS_BODY(
	SomeClass,
	int   , fieldA,
	short , fieldB,
	long  , fieldC
)
	void myMethod() {
		fieldA = 5;
	};
SERIALIZABLE_CLASS_END;
TEST(SerializableClassTests, SimpleClass) {
	SomeClass a;
	(void) a.fieldA;
	(void) a.fieldB;
	(void) a.fieldC;
	a.myMethod();
}

SERIALIZABLE_CLASS_BEGIN(Class)
SERIALIZABLE_CLASS_BODY(
	Class,
	int   , A,
	short , B,
	long  , C,
	std::string             , D,
	std::vector<std::string>, E
)
	bool operator==(const Class& o) const {
		return std::make_tuple(A, B, C) == std::make_tuple(o.A, o.B, o.C);
	}
	bool operator!=(const Class& o) const {
		return !(*this == o);
	}
SERIALIZABLE_CLASS_END;
TEST(SerializableClassTests, Serialize) {
	std::vector<std::string> tmpVector {"kogo", "ma", "ala", "?"};
	Class tmpC {1, 20, 300, "ala ma kota", tmpVector};

	LIZARDFS_DEFINE_INOUT_PAIR(Class, c, tmpC, Class());
	ASSERT_NE(cIn, cOut);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, cIn));
	ASSERT_NO_THROW(deserialize(buffer, cOut));
	LIZARDFS_VERIFY_INOUT_PAIR(c);
}

LIZARDFS_DEFINE_PACKET_VERSION(somebodyToSomebodyElse, communicate, kSomeVersion, 3210)
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		somebodyToSomebodyElse, communicate, LIZ_CLTOMA_FUSE_MKNOD, kSomeVersion,
		uint32_t, messageId,
		uint32_t, inode,
		LizardFsString<uint8_t>, name,
		uint8_t, nodeType,
		uint16_t, mode,
		uint16_t, umask,
		uint32_t, uid,
		uint32_t, gid,
		uint32_t, rdev)

TEST(PacketSerializationTests, SerializeAndDeserialize) {
	ASSERT_EQ(3210U, somebodyToSomebodyElse::communicate::kSomeVersion);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, messageId, 65432, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, inode, 36, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(LizardFsString<uint8_t>, name, "kobyla ma maly bok", "");
	LIZARDFS_DEFINE_INOUT_PAIR(uint8_t, nodeType, 0xF1, 0x00);
	LIZARDFS_DEFINE_INOUT_PAIR(uint16_t, mode, 0725, 0000);
	LIZARDFS_DEFINE_INOUT_PAIR(uint16_t, umask, 0351, 0000);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, uid, 1235, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, gid, 531, 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t, rdev, 398, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(somebodyToSomebodyElse::communicate::serialize(buffer, messageIdIn, inodeIn,
			nameIn, nodeTypeIn, modeIn, umaskIn, uidIn, gidIn, rdevIn));

	verifyHeader(buffer, LIZ_CLTOMA_FUSE_MKNOD);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(somebodyToSomebodyElse::communicate::deserialize(buffer.data(), buffer.size(),
			messageIdOut, inodeOut, nameOut, nodeTypeOut, modeOut, umaskOut, uidOut, gidOut,
			rdevOut));

	LIZARDFS_VERIFY_INOUT_PAIR(messageId);
	LIZARDFS_VERIFY_INOUT_PAIR(inode);
	LIZARDFS_VERIFY_INOUT_PAIR(name);
	LIZARDFS_VERIFY_INOUT_PAIR(nodeType);
	LIZARDFS_VERIFY_INOUT_PAIR(mode);
	LIZARDFS_VERIFY_INOUT_PAIR(umask);
	LIZARDFS_VERIFY_INOUT_PAIR(uid);
	LIZARDFS_VERIFY_INOUT_PAIR(gid);
	LIZARDFS_VERIFY_INOUT_PAIR(rdev);
}

LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(TestEnum, value0, value1, value2)

TEST(EnumClassSerializationTests, SerializeAndDeserialize) {
	LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(TestEnum, enums);
	enumsIn  = {TestEnum::value0, TestEnum::value1, TestEnum::value2};
	enumsOut = {TestEnum::value1, TestEnum::value2, TestEnum::value0};

	for (uint8_t i = 0; i < enumsIn.size(); ++i) {
		EXPECT_EQ(i, static_cast<uint8_t>(enumsIn[i]));
		std::vector<uint8_t> buffer;
		ASSERT_NO_THROW(serialize(buffer, enumsIn[i]));
		ASSERT_NO_THROW(deserialize(buffer, enumsOut[i]));
	}
	LIZARDFS_VERIFY_INOUT_PAIR(enums);
}

TEST(EnumClassSerializationTests, DeserializeImproperValue) {
	std::vector<uint8_t> buffer;
	uint8_t in = 1 + static_cast<uint8_t>(TestEnum::value2);
	ASSERT_NO_THROW(serialize(buffer, in));
	TestEnum out;
	ASSERT_THROW(deserialize(buffer, out), IncorrectDeserializationException);
}
