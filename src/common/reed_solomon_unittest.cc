/*
   Copyright 2016 Skytechnology sp. z o.o.

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

#include <cassert>
#include <gtest/gtest.h>
#include <iostream>

#include "common/reed_solomon.h"
#include "common/slice_traits.h"
#include "common/time_utils.h"

#define SMALL_TEST_DATA_SIZE (64 * 1024)
#define BIG_TEST_DATA_SIZE (64 * 1024 * 1024)

static void generate_random_data(std::vector<std::vector<uint8_t>> &data, int n, int size) {
	unsigned seed = rand();
	data.resize(n);
	for (int i = 0; i < n; ++i) {
		data[i].resize(size);
		for (int j = 0; j < size; ++j) {
			data[i][j] = seed % 256;
			seed += 997;
		}
	}
}

static void benchmark_encoding(std::vector<std::vector<uint8_t>> &input, int m, int repeat_count) {
	std::vector<std::vector<uint8_t>> output;
	int size = input[0].size();

	output.resize(m);
	for (int i = 0; i < m; ++i) {
		output[i].resize(size);
	}

	ReedSolomon<32, 32> rs(input.size(), m);
	ReedSolomon<32, 32>::ConstFragmentMap data_fragments{{0}};
	ReedSolomon<32, 32>::FragmentMap parity_fragments{{0}};

	for (int i = 0; i < (int)input.size(); ++i) {
		data_fragments[i] = input[i].data();
	}
	for (int i = 0; i < m; ++i) {
		parity_fragments[i] = output[i].data();
	}

	Timer time;
	for (int i = 0; i < repeat_count; ++i) {
		rs.encode(data_fragments, parity_fragments, size);
	}

	int64_t speed =
	    (int64_t)input.size() * (int64_t)size * (int64_t)repeat_count / (int64_t)time.elapsed_us();

	std::cout << "Encoding (" << input.size() << "," << m << ") = " << speed << "MB/s\n";
}

static void encode_parity(std::vector<std::vector<uint8_t>> &output,
		const std::vector<std::vector<uint8_t>> &input, int m) {
	int size = input[0].size();

	output.resize(m);
	for (int i = 0; i < m; ++i) {
		output[i].assign(size, 0xFF);
	}

	ReedSolomon<32, 32> rs(input.size(), m);
	ReedSolomon<32, 32>::ConstFragmentMap data_fragments{{0}};
	ReedSolomon<32, 32>::FragmentMap parity_fragments{{0}};

	for (int i = 0; i < (int)input.size(); ++i) {
		data_fragments[i] = input[i].data();
	}
	for (int i = 0; i < m; ++i) {
		parity_fragments[i] = output[i].data();
	}

	rs.encode(data_fragments, parity_fragments, size);
}

static void recover_parts(std::vector<std::vector<uint8_t>> &output,
		const ReedSolomon<32, 32>::ErasedMap erased,
		const ReedSolomon<32, 32>::ErasedMap zero_input,
		const std::vector<std::vector<uint8_t>> &data,
		const std::vector<std::vector<uint8_t>> &parity) {
	ReedSolomon<32, 32> rs(data.size(), parity.size());
	ReedSolomon<32, 32>::ConstFragmentMap input_fragments{{0}};
	ReedSolomon<32, 32>::FragmentMap output_fragments{{0}};
	int size = data[0].size();
	int parts_count = data.size() + parity.size();

	output.resize(erased.count());
	for (int i = 0; i < (int)output.size(); ++i) {
		output[i].assign(size, 0xFF);
	}

	for (int i = 0; i < (int)data.size(); ++i) {
		if (!zero_input[i]) {
			input_fragments[i] = data[i].data();
		}
	}
	for (int i = 0; i < (int)parity.size(); ++i) {
		if (!zero_input[data.size() + i]) {
			input_fragments[data.size() + i] = parity[i].data();
		}
	}

	int output_index = 0;
	for (int i = 0; i < parts_count; ++i) {
		if (erased[i]) {
			output_fragments[i] = output[output_index].data();
			++output_index;
		}
	}

	rs.recover(input_fragments, erased, output_fragments, size);
}

TEST(ReedSolomon, TestRecovery) {
	std::vector<std::vector<uint8_t>> data, parity, recovered;

	generate_random_data(data, 4, SMALL_TEST_DATA_SIZE);
	encode_parity(parity, data, 2);

	ReedSolomon<32, 32>::ErasedMap erased, zero_input;

	erased.set(0);
	erased.set(2);
	recover_parts(recovered, erased, zero_input, data, parity);

	EXPECT_EQ(data[0], recovered[0]);
	EXPECT_EQ(data[2], recovered[1]);

	erased.reset();
	erased.set(0);
	erased.set(5);
	recover_parts(recovered, erased, zero_input, data, parity);

	EXPECT_EQ(data[0], recovered[0]);
	EXPECT_EQ(parity[1], recovered[1]);

	erased.reset();
	erased.set(4);
	erased.set(5);
	recover_parts(recovered, erased, zero_input, data, parity);

	EXPECT_EQ(parity[0], recovered[0]);
	EXPECT_EQ(parity[1], recovered[1]);
}

TEST(ReedSolomon, TestRecoveryWithZeroData) {
	std::vector<std::vector<uint8_t>> data, parity, recovered;

	generate_random_data(data, 8, SMALL_TEST_DATA_SIZE);
	data[0].assign(SMALL_TEST_DATA_SIZE, 0);
	data[3].assign(SMALL_TEST_DATA_SIZE, 0);
	encode_parity(parity, data, 2);

	ReedSolomon<32, 32>::ErasedMap erased, zero_input;

	zero_input.set(0);

	erased.set(1);
	erased.set(8);
	recover_parts(recovered, erased, zero_input, data, parity);

	EXPECT_EQ(data[1], recovered[0]);
	EXPECT_EQ(parity[0], recovered[1]);

	zero_input.reset();
	erased.reset();

	zero_input.set(0);
	zero_input.set(3);

	erased.set(2);
	erased.set(9);
	recover_parts(recovered, erased, zero_input, data, parity);

	EXPECT_EQ(data[2], recovered[0]);
	EXPECT_EQ(parity[1], recovered[1]);
}

TEST(ReedSolomon, EncodeBenchmarkSmall) {
	std::vector<std::vector<uint8_t>> data;

	generate_random_data(data, 4, SMALL_TEST_DATA_SIZE);
	benchmark_encoding(data, 2, 1000);

	generate_random_data(data, 8, SMALL_TEST_DATA_SIZE);
	benchmark_encoding(data, 2, 1000);

	generate_random_data(data, 32, SMALL_TEST_DATA_SIZE);
	benchmark_encoding(data, 32, 100);
}

TEST(ReedSolomon, EncodeBenchmarkBig) {
	std::vector<std::vector<uint8_t>> data;

	generate_random_data(data, 4, BIG_TEST_DATA_SIZE/4);
	benchmark_encoding(data, 2, 5);
	generate_random_data(data, 8, BIG_TEST_DATA_SIZE/4);
	benchmark_encoding(data, 2, 5);
	generate_random_data(data, 32, BIG_TEST_DATA_SIZE/32);
	benchmark_encoding(data, 4, 5);
}

template<std::size_t N>
void select_rows(uint8_t *output_matrix, const uint8_t *input_matrix, int s1, int s2,
	                const std::bitset<N> &required_rows) {
	for (int i = 0; i < s1; ++i) {
		if (!required_rows[i]) {
			input_matrix += s2;
			continue;
		}

		for (int j = 0; j < s2; ++j) {
			output_matrix[j] = input_matrix[j];
		}

		output_matrix += s2;
		input_matrix += s2;
	}
}

template<std::size_t M, std::size_t N>
bool test_matrix(std::array<uint8_t, M> &rs_matrix, const std::bitset<N> &erased, int k, int m) {
	std::array<uint8_t, M> tmp_matrix,result_matrix;
	assert((int)erased.count() == m);
	select_rows(tmp_matrix.data(), rs_matrix.data(), k + m, k, ~erased);

	return gf_invert_matrix(tmp_matrix.data(), result_matrix.data(), k) == 0;
}

TEST(ReedSolomon, TestMatrix) {
	static const int max_part_count = 32 + 8;
	typedef std::bitset<2 * max_part_count> ErasedMap;
	typedef std::array<uint8_t, max_part_count * max_part_count> MatrixContainer;

	MatrixContainer rs_matrix;
	for(int k = slice_traits::ec::kMinDataCount; k <= slice_traits::ec::kMaxDataCount; ++k) {
		int m;

		m = 1; gf_gen_rs_matrix(rs_matrix.data(), k + m, k);
		for(int a = 0; a < k + m; ++a) {
			ErasedMap erased;
			erased.set(a);

			EXPECT_TRUE(test_matrix(rs_matrix, erased, k, m))
			<< "K,M = " << k << "," << m << " set=" << erased.to_string();
		}

		m = 2; gf_gen_rs_matrix(rs_matrix.data(), k + m, k);
		for(int a = 0; a < k + m; ++a) {
		for(int b = a + 1; b < k + m; ++b) {
			ErasedMap erased;
			erased.set(a);
			erased.set(b);

			EXPECT_TRUE(test_matrix(rs_matrix, erased, k, m))
			<< "K,M = " << k << "," << m << " set=" << erased.to_string();
		}
		}

		m = 3; gf_gen_rs_matrix(rs_matrix.data(), k + m, k);
		for(int a = 0; a < k + m; ++a) {
		for(int b = a + 1; b < k + m; ++b) {
		for(int c = b + 1; c < k + m; ++c) {
			ErasedMap erased;
			erased.set(a);
			erased.set(b);
			erased.set(c);

			EXPECT_TRUE(test_matrix(rs_matrix, erased, k, m))
			<< "K,M = " << k << "," << m << " set=" << erased.to_string();
		}
		}
		}

		if (k > 20) {
			continue;
		}

		m = 4; gf_gen_rs_matrix(rs_matrix.data(), k + m, k);
		for(int a = 0; a < k + m; ++a) {
		for(int b = a + 1; b < k + m; ++b) {
		for(int c = b + 1; c < k + m; ++c) {
		for(int d = c + 1; d < k + m; ++d) {
			ErasedMap erased;
			erased.set(a);
			erased.set(b);
			erased.set(c);
			erased.set(d);

			EXPECT_TRUE(test_matrix(rs_matrix, erased, k, m))
			<< "K,M = " << k << "," << m << " set=" << erased.to_string();
		}
		}
		}
		}
	}
}
