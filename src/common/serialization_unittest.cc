#include "common/platform.h"
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

TEST(SerializationTests, SerializeString) {
	serializeTest<std::string>("jajeczniczka ze szczypiorkiem");
}

TEST(SerializationTests, SerializeUint32Vector) {
	serializeTest<std::vector<uint32_t>>(std::vector<uint32_t>{1, 2, 3, 4});
}

TEST(SerializationTests, SerializeStringVector) {
	serializeTest<std::vector<std::string>>(
			std::vector<std::string>{"jajeczniczka", "ze", "szczypiorkiem"});
}

TEST(SerializationTests, DeserializeStringNonEmptyVariable) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::string, stringVariable, "good!", "BAD!");

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, stringVariableIn));
	ASSERT_ANY_THROW(deserialize(buffer, stringVariableOut));
}

TEST(SerializationTests, SerializeUniquePtr) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::unique_ptr<std::string>, ptr,
			new std::string("wyrob czekoladopodobny"), nullptr);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, ptrIn));
	ASSERT_NO_THROW(deserialize(buffer, ptrOut));
	ASSERT_FALSE(!ptrOut);
	ASSERT_EQ(*ptrIn, *ptrOut);
}

TEST(SerializationTests, SerializeEmptyUniquePtr) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::unique_ptr<std::string>, emptyPtr, nullptr, nullptr);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, emptyPtrIn));
	ASSERT_NO_THROW(deserialize(buffer, emptyPtrOut));
	ASSERT_TRUE(!emptyPtrOut);
}

TEST(SerializationTests, SerializeStringArray) {
	std::string arrayIn[3] = {"ala", "ma", "xxxxx"};
	std::string arrayOut[3];

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, arrayIn));
	ASSERT_NO_THROW(deserialize(buffer, arrayOut));

	for (auto i = 0; i < 3; ++i) {
		ASSERT_EQ(arrayIn[i], arrayOut[i]);
	}
}
