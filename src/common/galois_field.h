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

#include <cstdint>

/*! \brief Create Vandermonde encoding matrix for Reed-Solomon.
 *
 * Matrix dimension is (m, k)
 *
 * Top of the matrix is identity matrix.
 * Next (m-k) rows compute parity parts.
 *
 * \param a Pointer to buffer for storing matrix
 * \param m Sum of parity and data parts .
 * \param k Number of data parts.
 */
void gf_gen_rs_matrix(uint8_t *a, int m, int k);

/*! Compute matrix inversion.
 *
 * Input and output matrix have dimension (k, k).
 *
 * \param in_mat Pointer to buffer with matrix to be inverted.
 * \param out_mat Pointer to buffer
 * \param k Number of data parts.
 */
int gf_invert_matrix(uint8_t *in_mat, uint8_t *out_mat, const int n);

/*! \brief Initialize tables for fast Erasure Code encode and decode.
 *
 * Generates the expanded tables needed for fast encode or decode for erasure
 * codes on blocks of data. 32 bytes is generated for each input coefficient.
 *
 * \param k      Number of rows in the generator matrix.
 * \param rows   Number of columns in the generator matrix.
 * \param a      Pointer to generator matrix.
 * \param gftbls Pointer to start of buffer for concatenated output tables
 *               generated from input coefficients. Must be of size 32*k*rows.
 */
void ec_init_tables(int k, int rows, uint8_t *a, uint8_t *g_tbls);

/*! \brief Generate or decode erasure codes on blocks of data.
 *
 * Given a list of source data blocks, generate one or multiple blocks of
 * encoded data as specified by a matrix of GF(2^8) coefficients. When given a
 * suitable set of coefficients, this function will perform the fast generation
 * or decoding of Reed-Solomon type erasure codes.
 *
 * \param len    Length of each block of data (vector) of source or dest data.
 * \param k      The number of vector sources or rows in the generator matrix for coding.
 * \param rows   The number of output vectors to concurrently encode/decode.
 * \param gftbls Pointer to array of input tables generated from coding
 *               coefficients in ec_init_tables(). Must be of size 32*k*rows
 * \param data   Array of pointers to source input buffers.
 * \param coding Array of pointers to coded output buffers.
 */
void ec_encode_data(int len, int srcs, int dests, uint8_t *v, uint8_t **src, uint8_t **dest);
