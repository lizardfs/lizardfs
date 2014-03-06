#include "common/vector_serialization_wrapper.h"

#include <gtest/gtest.h>
#include <string>

TEST(VectorSerializationWrapperTest, SerializeStringVector) {
	const std::vector<std::string> in{"ukradli", "mi", "zloto"};
	std::vector<std::string> out;
	auto outWrapper = makeSerializationWrapper(out);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, makeSerializationWrapper(in)));
	ASSERT_NO_THROW(deserialize(buffer, outWrapper));

	EXPECT_EQ(in, out);
}

TEST(VectorSerializationWrapperTest, DeserializeToNonEmptyVector) {
	const std::vector<std::string> in{"ukradli", "mi", "zloto"};
	std::vector<std::string> out{"zaraz", "bedzie", "ciemno"};
	auto outWrapper = makeSerializationWrapper(out);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, makeSerializationWrapper(in)));
	ASSERT_ANY_THROW(deserialize(buffer, outWrapper));
}
