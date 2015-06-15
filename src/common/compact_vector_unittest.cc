#include "common/platform.h"
#include "common/compact_vector.h"

#include <algorithm>
#include <gtest/gtest.h>

template <typename T>
bool operator==(const compact_vector<T> &a, const std::vector<T> &b) {
	return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template <typename T>
bool operator!=(const compact_vector<T> &a, const std::vector<T> &b) {
	return !(a == b);
}

TEST(CompactVectorTest, GeneralBehaviour) {
	compact_vector<int> vec1_A;
	std::vector<int> vec1_B;
	EXPECT_EQ(vec1_A, vec1_B);

	compact_vector<double> vec2_A(5, 1.0);
	std::vector<double> vec2_B(5, 1.0);
	EXPECT_EQ(vec2_A, vec2_B);

	compact_vector<double> vec3_A(vec2_A);
	std::vector<double> vec3_B(vec2_B);
	EXPECT_EQ(vec2_A, vec3_A);
	EXPECT_EQ(vec3_A, vec3_B);

	vec3_A = std::move(vec2_A);
	vec3_B = std::move(vec2_B);

	EXPECT_EQ(vec3_A, vec3_B);
}

TEST(CompactVectorTest, InsertTest) {
	std::vector<int> range(100);

	std::iota(range.begin(), range.end(), 1);

	compact_vector<int> vec_A;
	std::vector<int> vec_B;

	vec_A.resize(100, 3);
	vec_B.resize(100, 3);

	vec_A.insert(vec_A.begin() + 30, range.begin(), range.end());
	vec_B.insert(vec_B.begin() + 30, range.begin(), range.end());

	EXPECT_EQ(vec_A, vec_B);

	vec_A.insert(vec_A.end(), 189);
	vec_B.insert(vec_B.end(), 189);

	EXPECT_EQ(vec_A, vec_B);
}

TEST(CompactVectorTest, EraseTest) {
	std::vector<int> range(200);

	std::iota(range.begin(), range.end(), 1);

	compact_vector<int> vec_A;
	std::vector<int> vec_B;

	vec_A.insert(vec_A.begin(), range.begin(), range.end());
	vec_B.insert(vec_B.begin(), range.begin(), range.end());

	vec_A.erase(vec_A.begin() + 40);
	vec_B.erase(vec_B.begin() + 40);

	EXPECT_EQ(vec_A, vec_B);

	vec_A.erase(vec_A.begin() + 76, vec_A.begin() + 100);
	vec_B.erase(vec_B.begin() + 76, vec_B.begin() + 100);

	EXPECT_EQ(vec_A, vec_B);

	vec_A.erase(vec_A.begin(), vec_A.end());
	vec_B.erase(vec_B.begin(), vec_B.end());

	EXPECT_EQ(vec_A, vec_B);
}

TEST(CompactVectorTest, IteratorTest) {
	compact_vector<int> vec(200);

	std::iota(vec.begin(), vec.end(), 0);

	compact_vector<int>::iterator it;
	compact_vector<int>::const_iterator cit;
	int i;

	it = vec.begin();
	for (i = 0; i < (int)vec.size(); i++) {
		EXPECT_EQ(i, *(vec.begin() + i));
		EXPECT_EQ(*it, vec[i]);

		++it;
	}

	EXPECT_EQ(it, vec.end());

	it--;
	i--;
	for (; i >= 0; i--) {
		EXPECT_EQ(*it, vec[i]);
		it--;
	}

	it = vec.begin() + 10;
	cit = it;

	EXPECT_EQ(it, cit);
	EXPECT_EQ(*it, *cit);
}
