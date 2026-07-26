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

#pragma once

#include "common/platform.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <limits>

namespace linear_assignment {

namespace detail {

template <typename M, typename A, typename P, typename V>
void auctionStep(M &value_matrix, A &assignment, A &object_assignment, P &prices, const V &eps,
		int size) {
	std::fill(assignment.begin(), assignment.begin() + size, -1);
	std::fill(object_assignment.begin(), object_assignment.begin() + size, -1);

	assert(size > 1);

	int unassigned_idx = 0;
	int assigned_count = 0;
	while (assigned_count < size) {
		while (assignment[unassigned_idx] >= 0) {
			++unassigned_idx;
			if (unassigned_idx >= size) {
				unassigned_idx = 0;
			}
		}

		V w = std::numeric_limits<V>::lowest();  // second highest (or highest if there is
		                                         // more than one element with highest
		                                         // value)
		V v = std::numeric_limits<V>::lowest();  // highest value
		int v_idx = -1;
		for (int i = 0; i < size; ++i) {
			V c = value_matrix[unassigned_idx][i] - prices[i];
			assert(c > std::numeric_limits<V>::lowest());
			if (c > v) {
				w = v;
				v = c;
				v_idx = i;
			} else {
				w = std::max(c, w);
			}
		}

		assert((v - w + eps) > V());

		prices[v_idx] += v - w + eps;

		int idx = object_assignment[v_idx];
		if (idx >= 0) {
			assignment[idx] = -1;
			assigned_count--;
		}

		object_assignment[v_idx] = unassigned_idx;
		assignment[unassigned_idx] = v_idx;
		assigned_count++;
	}
}

} // detail

/*! \brief Implementation of Bertsekas auction algorithm.
 *
 * The function implements Bertsekas [1] auction algorithm for integer value/benefit matrix
 * with epsilon scaling.
 *
 * [1] Bertsekas, D.P. 1998. Network Optimization Continuous and Discrete Models.
 *
 * \param cost value matrix with integer values - matrix dimension should be (size,size).
 *             Values in matrix should be considerably lower than MAX_INT.
 * \param assignment return vector with optimal assignment (maximizing value).
 *                   Each value in vector represent index of object that is assigned to that
 *                   person.
 * \param object_assignment return vector with optimal assignment of object-person pairs.
 *                          Value at index i represents index of person to which this object is
 *                          assigned.
 * \param size problem size (number of person-object pairs to match).
 */
template <typename M, std::size_t N>
void auctionOptimization(M &value_matrix, std::array<int, N> &assignment,
			std::array<int, N> &object_assignment, int size) {
	std::array<int, N> prices;

	assert(size <= (int)N);

	if (size <= 0) {
		return;
	}
	if (size == 1) {
		assignment[0] = 0;
		object_assignment[0] = 0;
		return;
	}

	std::fill(prices.begin(), prices.begin() + size, 0);

	// heuristic for setting epsilon
	int max_a = std::numeric_limits<int>::lowest();
	for (int i = 0; i < size; ++i) {
		for (int j = 0; j < size; ++j) {
			value_matrix[i][j] *= (size + 1); // we scale by (size + 1) so that epsilon=1
			                                  // guarantees optimal solution
			max_a = std::max(value_matrix[i][j], max_a);
		}
	}
	int eps = (max_a + 12) / 25;

	while (eps > 1) {
		detail::auctionStep(value_matrix, assignment, object_assignment, prices, eps, size);
		eps = (eps + 2) / 5;
	}

	eps = 1;
	detail::auctionStep(value_matrix, assignment, object_assignment, prices, eps, size);
}

/*! \brief Implementation of Bertsekas auction algorithm.
 *
 * \param cost value matrix with integer values - matrix dimension should be (size,size).
 *             Values in matrix should be considerably lower than MAX_INT.
 * \param assignment return vector with optimal assignment (maximizing value).
 *                   Each value in vector represent index of object that is assigned to that
 *                   person.
 * \param size problem size (number of person-object pairs to match).
 */
template <typename M, std::size_t N>
void auctionOptimization(M &value_matrix, std::array<int, N> &assignment, int size) {
	std::array<int, N> object_assignment;
	auctionOptimization(value_matrix, assignment, object_assignment, size);
}

}  // linear_assignment
