/*
   Copyright 2015 Skytechnology sp. z o.o.

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
#include "common/small_vector.h"

#include <algorithm>
#include <gtest/gtest.h>

template <typename T, std::size_t N>
bool operator==(const small_vector<T,N> &a, const std::vector<T> &b) {
	return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template <typename T, std::size_t N>
bool operator!=(const small_vector<T,N> &a, const std::vector<T> &b) {
	return !(a == b);
}

TEST(SmallVectorTest, GeneralBehaviour) {
	small_vector<int, 5> vec1_A;
	std::vector<int> vec1_B;
	EXPECT_EQ(vec1_A, vec1_B);

	small_vector<double, 3> vec2_A(5, 1.0);
	std::vector<double> vec2_B(5, 1.0);
	EXPECT_EQ(vec2_A, vec2_B);

	small_vector<double, 3> vec3_A(vec2_A);
	std::vector<double> vec3_B(vec2_B);
	EXPECT_EQ(vec2_A, vec3_A);
	EXPECT_EQ(vec3_A, vec3_B);

	vec3_A = std::move(vec2_A);
	vec3_B = std::move(vec2_B);

	EXPECT_EQ(vec3_A, vec3_B);
}

TEST(SmallVectorTest, InsertTest) {
	std::vector<int> range(100);

	std::iota(range.begin(), range.end(), 1);

	small_vector<int, 3> vec_A;
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

TEST(SmallVectorTest, EraseTest) {
	std::vector<int16_t> range(200);

	std::iota(range.begin(), range.end(), 1);

	small_vector<int16_t, 7> vec_A;
	std::vector<int16_t> vec_B;

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

TEST(SmallVectorTest, IteratorTest) {
	small_vector<int, 33> vec(200);

	std::iota(vec.begin(), vec.end(), 0);

	small_vector<int, 33>::iterator it;
	small_vector<int, 33>::const_iterator cit;
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

TEST(SmallVectorTest, InitializerList) {
	small_vector<double, 16> vec{1.0, 2.0, 4.0, 8.0};

	for (int i = 0; i < (int)vec.size(); i++) {
		EXPECT_EQ(1 << i, (int)vec[i]);
	}

	small_vector<small_vector<double, 16>, 24> vecvec{vec, vec, vec, vec, vec};

	for (int i = 0; i < (int)vecvec.size(); ++i) {
		EXPECT_EQ(vec, vecvec[i]);
	}
}
