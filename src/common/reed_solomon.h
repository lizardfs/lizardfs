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

#pragma once

#include "common/platform.h"

#include <array>
#include <bitset>
#include <cassert>

#ifdef LIZARDFS_HAVE_ISA_L_ERASURE_CODE_H
  #include <isa-l/erasure_code.h>
#else
  #include "common/galois_field.h"
#endif

/*! \brief Implementation of Reed-Solomon encoding/decoding.
 *
 * This class uses Intel Storage Acceleration Library for efficient
 * operations in field GF(2^8).
 *
 * MAXK - maximum number of data parts
 * MAXM - maximum number of parity parts
 */
template <int MAXK, int MAXM>
class ReedSolomon {
public:
	static const int kMaxDataCount = MAXK;
	static const int kMaxParityCount = MAXM;
	static const int kMaxPartCount = MAXK + MAXM;

	typedef std::array<uint8_t, kMaxDataCount * kMaxPartCount> MatrixContainer;
	typedef std::array<uint8_t, kMaxDataCount * kMaxParityCount * 32> GFTableContainer;
	typedef std::bitset<kMaxPartCount> ErasedMap;
	typedef std::array<uint8_t *, kMaxPartCount> FragmentMap;
	typedef std::array<const uint8_t *, kMaxPartCount> ConstFragmentMap;

public:
	ReedSolomon() : rs_k_(), rs_m_() {
	}

	/*! Constructor.
	 *
	 * Prepares internal data for encoding/decoding of Reed-Solomon code (k,m).
	 *
	 * \param k Number of data parts.
	 * \param m Number of parity parts.
	 */
	ReedSolomon(int k, int m) : rs_k_(), rs_m_() {
		assert(k >= 1 && k <= kMaxDataCount);
		assert(m >= 1 && m <= kMaxParityCount);

		createRSMatrix(k, m);
	}

	/*! \brief Recover missing parts.
	 *
	 * Input/Output fragments are indexed from 0. First we have k data parts,
	 * then we have m parity parts (so first parity part have index k and last (k + m - 1)).
	 *
	 * \param input_fragments Table with pointers to buffers with available parts. If the part
	 *                        is marked as available and the pointer to input buffer is NULL,
	 *                        then the part is treated as containing only 0 values.
	 * \param erased Bit-set with information about missing parts.
	 * \param output_fragments Table with pointers to buffers for storing recovered parts. If the
	 *                         pointer to output buffer for a missing part is NULL,
	 *                         then this parts is not reconstructed (which reduces
	 *                         number of required computations).
	 * \param data_size size of input/output parts (each part must have the same size).
	 */
	void recover(const ConstFragmentMap &input_fragments, const ErasedMap &erased,
	             FragmentMap &output_fragments, std::size_t data_size) {
		ErasedMap needed, non_zero_input;
		ConstFragmentMap in_parts;
		FragmentMap out_parts;
		int in_count = 0, out_count = 0, in_with_zero_count = 0;
		int data_part_count = 0, parity_part_count = 0;

		assert((int)erased.count() == rs_m_);

		for (int i = 0; i < (rs_k_ + rs_m_); ++i) {
			if (erased[i] && output_fragments[i]) {
				needed.set(i);
				out_parts[out_count++] = output_fragments[i];
				parity_part_count += i >= rs_k_;
			}
			if (!erased[i]) {
				if (input_fragments[i]) {
					in_parts[in_count++] = input_fragments[i];
					non_zero_input.set(in_with_zero_count);
				}
				++in_with_zero_count;
				data_part_count += i < rs_k_;
			}
		}

		if (data_part_count == rs_k_) {
			createEncodingMatrix(needed, erased, non_zero_input);
		} else {
			createRecoveryMatrix(needed, erased, non_zero_input, parity_part_count == 0);
		}

		ec_encode_data(data_size, in_count, out_count, gf_table_.data(),
		               const_cast<uint8_t **>(in_parts.data()), out_parts.data());
	}

	/*! \brief Compute parity parts.
	 *
	 * \param data_fragments Table with pointers to buffers with data parts. If the part
	 *                       is marked as available and the pointer to input buffer is NULL,
	 *                       then the part is treated as containing only 0 values.
	 *                       Buffers are indexed from 0.
	 * \param parity_fragments Table with pointers to buffers for storing parity parts.
	 *                         Each pointer to parity buffer must be available (not NULL).
	 *                         Buffers are indexed from 0.
	 * \param data_size size of input/output parts (each part must have the same size).
	 */
	void encode(const ConstFragmentMap &data_fragments, FragmentMap &parity_fragments,
	            std::size_t data_size) {
		ErasedMap needed, erased, non_zero_input;
		ConstFragmentMap in_parts;
		int in_count = 0;

		for (int i = 0; i < rs_k_; ++i) {
			if (data_fragments[i]) {
				non_zero_input.set(i);
				in_parts[in_count++] = data_fragments[i];
			}
		}
		for (int i = 0; i < rs_m_; ++i) {
			assert(parity_fragments[i]);
			needed.set(rs_k_ + i);
			erased.set(rs_k_ + i);
		}
		createEncodingMatrix(needed, erased, non_zero_input);

		ec_encode_data(data_size, in_count, rs_m_, gf_table_.data(),
		               const_cast<uint8_t **>(in_parts.data()), parity_fragments.data());
	}

protected:
	/*! \brief Create Vandermonde RS(k,m) matrix.
	 *
	 * \param k Number of data parts.
	 * \param m Number of parity parts.
	 */
	void createRSMatrix(int k, int m) {
		if (k == rs_k_ && m == rs_m_) {
			return;
		}

		if (m >= 5 || (m == 4 && k > 20)) {
			gf_gen_cauchy1_matrix(rs_matrix_.data(), k + m, k);
		} else {
			gf_gen_rs_matrix(rs_matrix_.data(), k + m, k);
		}

		rs_k_ = k;
		rs_m_ = m;
		needed_parts_.reset();
		erased_parts_.reset();
	}

	/*! \brief Create encoding matrix (for calculating all parity parts).
	 *
	 * Data parts start at index 0. Parity parts start at index k.
	 *
	 * \param needed Bit-set with information about needed parts.
	 * \param erased Bit-set with information about missing parts.
	 * \param non_zero_input Bit-set with positions of non-zero input parts. Parts with all zeros can
	 *                       be omitted from calculations.
	 */
	void createEncodingMatrix(const ErasedMap &needed, const ErasedMap &erased,
	                          const ErasedMap &non_zero_input) {
		assert((int)needed.count() <= rs_m_ && (int)erased.count() == rs_m_);
		assert(non_zero_input.count() > 0 && (int)non_zero_input_.count() <= rs_k_);

		// check if matrix is available in cache
		if (needed == needed_parts_ && erased == erased_parts_ &&
		    non_zero_input_ == non_zero_input) {
			return;
		}

		MatrixContainer encode_matrix;
		selectRows(encode_matrix.data(), rs_matrix_.data(), rs_k_ + rs_m_, rs_k_, needed);
		if ((int)non_zero_input.count() < rs_k_) {
			MatrixContainer reduced_matrix;

			// Remove columns that will be multiplied by 0s.
			selectColumns(reduced_matrix.data(), encode_matrix.data(), needed.count(), rs_k_,
			              non_zero_input);
			ec_init_tables(needed.count(), non_zero_input.count(), reduced_matrix.data(),
			               gf_table_.data());
		} else {
			ec_init_tables(needed.count(), rs_k_, encode_matrix.data(), gf_table_.data());
		}

		needed_parts_ = needed;
		erased_parts_ = erased;
		non_zero_input_ = non_zero_input;
	}

	/*! \brief Create recovery matrix (for calculating needed missing parts).
	 *
	 * Data parts start at index 0. Parity parts start at index k.
	 *
	 * \param needed Bit-set with information about needed parts.
	 * \param erased Bit-set with information about missing parts.
	 * \param non_zero_input Bit-set with positions of non-zero input parts. Parts with all zeros can
	 *                       be omitted from calculations.
	 * \param recover_only_data If true then only data parts are needed.
	 */
	void createRecoveryMatrix(const ErasedMap &needed, const ErasedMap &erased,
	                          const ErasedMap &non_zero_input, bool recover_only_data) {
		assert((int)needed.count() <= rs_m_ && (int)erased.count() == rs_m_);
		assert(non_zero_input.count() > 0 && (int)non_zero_input_.count() <= rs_k_);

		// check if matrix is available in cache
		if (needed == needed_parts_ && erased == erased_parts_ &&
		    non_zero_input == non_zero_input_) {
			return;
		}

		MatrixContainer tmp_matrix;
		MatrixContainer decode_matrix;
		MatrixContainer recover_matrix;

		// Create matrix that can compute available parts from data parts.
		selectRows(tmp_matrix.data(), rs_matrix_.data(), rs_k_ + rs_m_, rs_k_, ~erased);

		// Invert so we can compute data parts from available parts.
		int r = gf_invert_matrix(tmp_matrix.data(), decode_matrix.data(), rs_k_);
		if (r != 0) {
			std::runtime_error("Reed-Solomon:Failed to invert decode matrix");
		}

		if (!recover_only_data) {
			// Create matrix that can calculate needed parts from all data parts.
			selectRows(tmp_matrix.data(), rs_matrix_.data(), rs_k_ + rs_m_, rs_k_, needed);

			// Multiply this matrix by decode matrix. Resulting matrix computes only needed parts
			// from available parts.
			matrixMultiply(recover_matrix.data(), needed.count(), rs_k_, rs_k_, tmp_matrix.data(),
			               decode_matrix.data());
		} else {
			// Select rows corresponding to needed data parts.
			selectRows(recover_matrix.data(), decode_matrix.data(), rs_k_, rs_k_, needed);
		}

		if ((int)non_zero_input.count() < rs_k_) {
			MatrixContainer reduced_matrix;

			// Remove columns that will be multiplied by 0s.
			selectColumns(reduced_matrix.data(), recover_matrix.data(), needed.count(), rs_k_,
			              non_zero_input);
			ec_init_tables(needed.count(), non_zero_input.count(), reduced_matrix.data(),
			               gf_table_.data());
		} else {
			ec_init_tables(needed.count(), rs_k_, recover_matrix.data(), gf_table_.data());
		}

		needed_parts_ = needed;
		erased_parts_ = erased;
		non_zero_input_ = non_zero_input;
	}

	/*! \brief Select rows from matrix.
	 *
	 * \param output_matrix Pointer to buffer for storing output matrix. Output matrix
	 *                      has size (required_rows.count(), s2).
	 * \param input_matrix Pointer to buffer with input matrix.
	 * \param s1 Number of rows in input matrix.
	 * \param s2 Number of columns in input matrix.
	 * \param required_rows Bit-set with rows that should be copied.
	 */
	void selectRows(uint8_t *output_matrix, const uint8_t *input_matrix, int s1, int s2,
	                const ErasedMap &required_rows) {
		for (int i = 0; i < s1; ++i) {
			if (!required_rows[i]) {
				input_matrix += s2;
				continue;
			}

#ifdef _OPENMP
			#pragma omp simd
#endif
			for (int j = 0; j < s2; ++j) {
				output_matrix[j] = input_matrix[j];
			}

			output_matrix += s2;
			input_matrix += s2;
		}
	}

	/*! \brief Select columns from matrix.
	 *
	 * \param output_matrix Pointer to buffer for storing output matrix. Output matrix
	 *                      has size (s1, required_columns.count()).
	 * \param input_matrix Pointer to buffer with input matrix.
	 * \param s1 Number of rows in input matrix.
	 * \param s2 Number of columns in input matrix.
	 * \param required_columns Bit-set with columns that should be copied.
	 */
	void selectColumns(uint8_t *output_matrix, const uint8_t *input_matrix, int s1, int s2,
	                   const ErasedMap &required_columns) {
		for (int i = 0; i < s1; ++i) {
			for (int j = 0; j < s2; ++j) {
				if (required_columns[j]) {
					*output_matrix = input_matrix[j];
					++output_matrix;
				}
			}
			input_matrix += s2;
		}
	}

	/*! \brief Matrix multiplication.
	 *
	 * \param output_matrix Pointer to buffer for storing output matrix. Output matrix
	 *                      has size (s1, s3).
	 * \param s1 Number of rows in matrix A.
	 * \param s2 Number of columns in matrix A (and number rows in matrix B).
	 * \param s3 Number of columns in matrix B.
	 * \param a_matrix Pointer to matrix A.
	 * \param b_matrix Pointer to matrix B.
	 */
	void matrixMultiply(uint8_t *output_matrix, int s1, int s2, int s3, const uint8_t *a_matrix,
	                    const uint8_t *b_matrix) {
		FragmentMap output_rows, input_rows;

		ec_init_tables(s3, s1, (uint8_t*)a_matrix, gf_table_.data());

		for (int i = 0; i < s1; ++i) {
			output_rows[i] = output_matrix + i * s2;
		}
		for (int i = 0; i < s3; ++i) {
			input_rows[i] = (uint8_t*)b_matrix + i * s2;
		}

		ec_encode_data(s2, s3, s1, gf_table_.data(), input_rows.data(), output_rows.data());
	}

protected:
	GFTableContainer gf_table_; /*!< Cached recovery matrix (encoded for ISA-L).
	                                 This must be first variable to preserve alignment. */
	MatrixContainer rs_matrix_; /*!< Vandermonde matrix for RS(rs_k_, rs_m_). */
	ErasedMap erased_parts_;    /*!< Erased parts for cached recovery matrix. */
	ErasedMap needed_parts_;    /*!< Needed parts for cached recovery matrix. */
	ErasedMap non_zero_input_;  /*!< Non zero inputs for cached recovery matrix. */
	int rs_k_;                  /*!< Number of data parts. */
	int rs_m_;                  /*!< Number of parity parts. */
}
#if defined(__GCC__)
__attribute__ ((aligned(32)))
#endif
;
