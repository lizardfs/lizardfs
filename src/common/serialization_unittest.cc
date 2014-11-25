#include "common/platform.h"
#include "common/serialization.h"

#include <gtest/gtest.h>

#include "unittests/serialization.h"

TEST(SerializationTests, SerializeString) {
	serializeTest<std::string>("jajeczniczka ze szczypiorkiem");
}

TEST(SerializationTests, SerializeUint32Vector) {
	serializeTest(std::vector<uint32_t>{1, 2, 3, 4});
}

TEST(SerializationTests, SerializeStringVector) {
	serializeTest(std::vector<std::string>{"jajeczniczka", "ze", "szczypiorkiem"});
}

TEST(SerializationTests, SerializeMapOfMapsOfSets) {
	std::map<uint32_t, std::map<int, std::set<uint32_t>>> mapOfMapsOfSets;
	mapOfMapsOfSets[3][4] = {5, 6, 7};
	mapOfMapsOfSets[3][5] = {9, 6};
	mapOfMapsOfSets[1][4] = {};
	mapOfMapsOfSets[2];
	serializeTest(mapOfMapsOfSets);
}

struct MyStringAllocator : public std::allocator<std::string> {
	template<class ... Args>
	MyStringAllocator(Args... args) : std::allocator<std::string>(args...) {
	}
};
TEST(SerializationTests, SerializeVectorWithCustomAllocator) {
	serializeTest<std::vector<std::string, MyStringAllocator>>(
			std::vector<std::string, MyStringAllocator>{"dwa", "jajka", "na", "kielbasie"});
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

TEST(SerializationTests, SerializeSet) {
	serializeTest<std::set<std::string>>(
			std::set<std::string>{"lubie", "dajmy", "na", "to", "-", "placuszki"});
}

TEST(SerializationTests, SerializeMap) {
	serializeTest<std::map<std::string, std::string>>(
			std::map<std::string, std::string>{
					{"lubie", "dajmy"}, {"na", "to"}, {"-", "placuszki"}});
}
