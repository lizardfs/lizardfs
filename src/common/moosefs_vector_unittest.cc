#include "common/moosefs_vector.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"

TEST(MooseFSVectorTest, GeneralBehaviour) {
	MooseFSVector<int>    vec1_A;
	std::vector<int>      vec1_B;
	EXPECT_EQ(vec1_A, vec1_B);

	MooseFSVector<double> vec2_A(5, 1.0);
	std::vector<double>   vec2_B(5, 1.0);
	EXPECT_EQ(vec2_A, vec2_B);

	MooseFSVector<double> vec3_A(vec2_B);
	std::vector<double>   vec3_B(vec2_A);
	EXPECT_EQ(vec2_B, vec3_A);
	EXPECT_EQ(vec2_A, vec3_B);

	vec1_A.push_back(5);
	EXPECT_NE(vec1_A, vec1_B);
	vec2_A[0] = 2.0;
	EXPECT_NE(vec2_A, vec2_B);
}

TEST(MooseFSVectorTest, Serialization) {
	typedef std::vector<uint16_t>   OrdinaryVec;
	typedef MooseFSVector<uint16_t> MooseFsVec;

	OrdinaryVec o1 {1, 20, 300, 400};
	OrdinaryVec o2 (o1);
	MooseFsVec  m1 (o1);
	MooseFsVec  m2 (o1);

	std::vector<uint8_t> o_buffers[2];
	serialize(o_buffers[0], o1);
	serialize(o_buffers[1], o2);

	std::vector<uint8_t> m_buffers[2];
	serialize(m_buffers[0], m1);
	serialize(m_buffers[1], m2);

	// Check if MooseFSVector is serialized differently then std::vector:
	ASSERT_NE(o_buffers[0], m_buffers[0]);

	// Check if MooseFSVectors is always serialized the same way:
	ASSERT_EQ(m_buffers[0], m_buffers[1]);
	// Same about std::vector:
	ASSERT_EQ(o_buffers[0], o_buffers[1]);

	MooseFsVec m1_deserialized;
	ASSERT_NE(m1, m1_deserialized);
	deserialize(m_buffers[0], m1_deserialized);
	ASSERT_EQ(m1, m1_deserialized);
}
