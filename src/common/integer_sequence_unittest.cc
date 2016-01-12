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
#include "common/integer_sequence.h"

#include <numeric>

#include <gtest/gtest.h>

template <typename T>
void convert_to_vector(std::vector<T> &) {
}

template <typename T, typename... Is>
void convert_to_vector(std::vector<T> &result, T v, Is... sequence_postfix) {
	result.push_back(v);
	convert_to_vector(result, sequence_postfix...);
}

template <typename T, T... Is>
void convert_to_vector(std::vector<T> &result, integer_sequence<T, Is...>) {
	convert_to_vector(result, Is...);
}

TEST(IntegerSequence, IntegerTest) {
	std::vector<int> seq1(100), seq2;

	std::iota(seq1.begin(), seq1.end(), 0);
	convert_to_vector(seq2, make_integer_sequence<int, 100>());

	EXPECT_EQ(seq1, seq2);
}

TEST(IntegerSequence, IndexTest) {
	std::vector<std::size_t> seq1(101), seq2;

	std::iota(seq1.begin(), seq1.end(), 0);
	convert_to_vector(seq2, make_index_sequence<101>());

	EXPECT_EQ(seq1, seq2);
}
