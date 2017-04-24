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

#pragma once

#include "common/platform.h"

#include <array>

#include <common/integer_sequence.h>

namespace detail {

/*! \brief Multiply by 2 in GF(2^8). */
constexpr uint8_t gf_mul2(uint8_t x) {
	return (x << 1) ^ ((x & 0x80) ? 0x1d : 0);
}

/*! \brief Compute base 2 logarithm in GF(2^8).
 * \param x Logarithm argument (must be different than 0).
 * \param pow2n Internal variable used for recursion.
 * \param n Internal variable used for recursion.
 * \return log(x)
 */
constexpr uint8_t gf_log(uint8_t x, uint8_t pow2n = 2, unsigned n = 1) {
	return x == pow2n ? n : gf_log(x, gf_mul2(pow2n), n + 1);
}

/*! \brief Compute base 2 exponent in GF(2^8).
 * \param x Logarithm argument (must be >= 1).
 * \param pow2n Internal variable used for recursion.
 * \param n Internal variable used for recursion.
 * \return exp(x)
 */
constexpr uint8_t gf_exp(uint8_t x, uint8_t pow2n = 2, unsigned n = 1) {
	return x == n ? pow2n : gf_exp(x, gf_mul2(pow2n), n + 1);
}

/*! \brief Returns lookup table for base 2 logarithm in GF(2^8). */
template <std::size_t... Is>
constexpr std::array<uint8_t, sizeof...(Is) + 1> get_gf_log_table(index_sequence<Is...>) {
	return std::array<uint8_t, sizeof...(Is) + 1>{{0, gf_log(Is + 1)...}};
}

/*! \brief Returns lookup table for base 2 exponent in GF(2^8). */
template <std::size_t... Is>
constexpr std::array<uint8_t, sizeof...(Is) + 1> get_gf_exp_table(index_sequence<Is...>) {
	return std::array<uint8_t, sizeof...(Is) + 1>{{1, gf_exp(Is + 1)...}};
}

}  // detail

constexpr std::array<uint8_t, 256> gf_log_table =
	detail::get_gf_log_table(make_index_sequence<255>());
constexpr std::array<uint8_t, 256> gf_exp_table =
	detail::get_gf_exp_table(make_index_sequence<255>());
