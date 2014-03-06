#include "common/serialization.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"

template<class T>
void serializeTest(const T& toBeSerialized) {
	LIZARDFS_DEFINE_INOUT_PAIR(T, toBeTested, toBeSerialized, T());

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, toBeTestedIn));
	ASSERT_NO_THROW(deserialize(buffer, toBeTestedOut));

	LIZARDFS_VERIFY_INOUT_PAIR(toBeTested);
}

TEST(SerializationTest, SerializeString) {
	serializeTest<std::string>("jajeczniczka ze szczypiorkiem");
}

TEST(SerializationTest, SerializeUint32Vector) {
	serializeTest<std::vector<uint32_t>>(std::vector<uint32_t>{1, 2, 3, 4});
}

TEST(SerializationTest, DeserializeStringNonEmptyVariable) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::string, stringVariable, "good!", "BAD!");

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, stringVariableIn));
	ASSERT_ANY_THROW(deserialize(buffer, stringVariableOut));
}
