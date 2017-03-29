/*
   Copyright 2017 Skytechnology sp. z o.o.

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

#include "common/counting_sort.h"
#include "common/platform.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <numeric>

TEST(CountingSort, SimpleSort) {
	std::vector<int> data, data_count;

	for (int i = 0; i < 1000; ++i) {
		data.push_back(rand() % 1000);
	}

	data_count.resize(data.size());
	counting_sort_copy(data.begin(), data.end(), data_count.begin(), [](int v) { return v; });

	std::sort(data.begin(), data.end());

	EXPECT_EQ(data, data_count);
}

TEST(CountingSort, StableSort) {
	std::vector<std::pair<int, int>> data, data_count;

	for (int i = 0; i < 1000; ++i) {
		data.push_back({rand() % 1000, rand() % 100});
	}

	std::stable_sort(data.begin(), data.end(),
	                 [](const std::pair<int, int> &v1, const std::pair<int, int> &v2) {
		                 return v1.second < v2.second;
		         });

	data_count.resize(data.size());
	counting_sort_copy(data.begin(), data.end(), data_count.begin(),
	                   [](const std::pair<int, int> &value) { return value.first; });

	std::stable_sort(data.begin(), data.end(),
	                 [](const std::pair<int, int> &v1, const std::pair<int, int> &v2) {
		                 return v1.first < v2.first;
		         });

	EXPECT_EQ(data, data_count);
}
