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
#include "common/linear_assignment_optimizer.h"

#include <array>
#include <gtest/gtest.h>

#include "common/random.h"

// testing all possible combinations of assignment
template <typename M, std::size_t N>
int getMaximumAssignmentValue(M &value_matrix, std::array<int, N> &assignment, int index, int size) {
	int max_value = std::numeric_limits<int>::lowest();

	for (int i = 0; i < size; ++i) {
		auto it = std::find(assignment.begin(), assignment.begin() + index, i);
		if (it != (assignment.begin() + index)) {
			continue;
		}

		assignment[index] = i;
		int v = value_matrix[index][i];
		if (index < (size - 1)) {
			v += getMaximumAssignmentValue(value_matrix, assignment, index + 1, size);
		}

		max_value = std::max(max_value, v);
	}

	return max_value;
}

TEST(LinearAssignmentProblem, TestSize1) {
	int value_matrix[1][1] = {{3}};
	std::array<int, 1> assignment;

	linear_assignment::auctionOptimization(value_matrix, assignment, 1);

	std::array<int, 1> expected_result = {0};

	EXPECT_EQ(expected_result, assignment);
}

TEST(LinearAssignmentProblem, TestRandom) {
	int value_matrix[10][10];
	std::array<int, 10> assignment;

	for (int size = 2; size <= 10; ++size) {
		for (int i = 0; i < size; ++i) {
			for (int j = 0; j < size; ++j) {
				value_matrix[i][j] = rnd_ranged(1000);
			}
		}

		// evaluate all possible assignments
		int max_value = getMaximumAssignmentValue(value_matrix, assignment, 0, size);

		linear_assignment::auctionOptimization(value_matrix, assignment, size);

		int auction_value = 0;
		for (int i = 0; i < size; ++i) {
			auction_value += value_matrix[i][assignment[i]] / (size + 1);
		}

		// check if values are different
		int max_element = -1;
		for (int i = 0; i < size; ++i) {
			for (int j = i + 1; j < size; ++j) {
				EXPECT_NE(assignment[i], assignment[j]);
			}
			max_element = std::max(max_element, assignment[i]);
		}

		EXPECT_EQ(size - 1, max_element) << "size = " << size;
		EXPECT_EQ(max_value, auction_value) << "size = " << size;
	}
}
