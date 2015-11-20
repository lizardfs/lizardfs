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
#include "common/vector_range.h"

#include <algorithm>
#include <list>
#include <gtest/gtest.h>

TEST(VectorRange, BasicOperations) {
	std::vector<int> vec = {1, 2, 3, 4, 5, 6, 7, 8, 9};
	small_vector<int, 5> small_vec;
	small_vec.insert(small_vec.begin(), vec.begin(), vec.end());
	size_t size = 5;

	// Check constructors
	vector_range<std::vector<int>> vec_range(vec, 1, size);
	vector_range<small_vector<int, 5>> small_vec_range(small_vec, 0, size);

	vector_range<std::vector<int>> vec_copy = vec_range;
	vector_range<std::vector<int>> vec_moved = std::move(vec_copy);
	vec_copy = std::move(vec_moved);
	vec_moved = std::move(vec_copy);

	ASSERT_EQ(vec_moved.size(), vec_range.size());

	int i = 0;
	for (auto &x : vec_moved) {
		ASSERT_EQ(x, vec_range[i]);
		ASSERT_EQ(x, vec[i + 1]);
		i++;
	}

	// Reverse a vector - ranges should not be invalidated
	std::reverse(vec.begin(), vec.end());

	i = 0;
	for (auto &x : vec_moved) {
		ASSERT_EQ(x, vec_range[i]);
		ASSERT_EQ(x, vec[i + 1]);
		i++;
	}

	// Change vector range by assigning values to it and pushing one back
	vec_moved = {0, 1, 0, 1, 0, 0};
	vec_moved.pop_back();
	vec_moved.push_back(2);

	i = 0;
	for (auto &x : vec_moved) {
		ASSERT_EQ(x, vec_range[i]);
		ASSERT_EQ(x, vec[i + 1]);
		i++;
	}

	// Erase some values
	vec_range.erase(vec_range.begin() + 1);
	vec_moved.erase(vec_moved.begin(), vec_moved.begin() + 2);

	i = 0;
	for (auto &x : vec_moved) {
		ASSERT_EQ(x, vec_range[i]);
		ASSERT_EQ(x, vec[i + 1]);
		i++;
	}

	// Restore initial value of vector range
	vec_range.clear();
	vec_moved = {8, 6, 5, 4};
	vec_range.insert(vec_range.begin() + 1, 7);

	// Check that original vector has the same values after restoring its range
	i = vec.size() - 1;
	for (auto &x : vec) {
		ASSERT_EQ(x, small_vec[i]);
		i--;
	}
}

TEST(VectorRange, Assign) {
	size_t size = 3;
	std::vector<int> vec = {1, 2, 3, 4, 5, 6, 7, 8, 9};
	vector_range<std::vector<int>> vec_range(vec, 4, size);
	std::list<int> l = {0, 0, 0, 9, 9, 7};

	vec_range.assign({1});
	ASSERT_EQ(size, 1U);
	ASSERT_EQ(vec.size(), 9 - 3 + size);

	vec_range.assign(l.begin(), l.end());
	ASSERT_EQ(size, l.size());
	ASSERT_EQ(vec.size(), 9 - 3 + size);

	vec_range.assign(11, 6);
	ASSERT_EQ(size, 11U);
	ASSERT_EQ(vec.size(), 9 - 3 + size);
}

TEST(VectorRange, ConstVectorRange) {
	std::vector<int> vec = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
	vector_range<const std::vector<int>> vec_range(vec, 3, 3);

	EXPECT_EQ(vec_range.size(), 3U);
	EXPECT_EQ(vec_range[0], 3);
	EXPECT_EQ(vec_range[1], 4);
	EXPECT_EQ(vec_range[2], 5);
}
