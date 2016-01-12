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

#include <cstddef>

template <typename T, T... Is>
struct integer_sequence {
	typedef integer_sequence<T, Is...> type;
	static constexpr std::size_t size() {
		return sizeof...(Is);
	}
};

template <typename T, class S1, class S2>
struct merge_integer_sequence;
template <typename T, T... I1, T... I2>
struct merge_integer_sequence<T, integer_sequence<T, I1...>, integer_sequence<T, I2...>>
	: integer_sequence<T, I1..., (sizeof...(I1) + I2)...> {};

template <typename T, std::size_t N>
struct make_integer_sequence
	: merge_integer_sequence<T, typename make_integer_sequence<T, N / 2>::type,
	                            typename make_integer_sequence<T, N - N / 2>::type> {};

template <typename T>
struct make_integer_sequence<T, 0> : integer_sequence<T> {};
template <typename T>
struct make_integer_sequence<T, 1> : integer_sequence<T, 0> {};

template <std::size_t... Is>
struct index_sequence : integer_sequence<std::size_t, Is...> {
	typedef index_sequence<Is...> type;
	static constexpr std::size_t size() {
		return sizeof...(Is);
	}
};

template <class S1, class S2>
struct merge_index_sequence;
template <std::size_t... I1, std::size_t... I2>
struct merge_index_sequence<index_sequence<I1...>, index_sequence<I2...>>
	: index_sequence<I1..., (sizeof...(I1) + I2)...> {};

template <std::size_t N>
struct make_index_sequence : merge_index_sequence<typename make_index_sequence<N / 2>::type,
	typename make_index_sequence<N - N / 2>::type> {};

template <>
struct make_index_sequence<0> : index_sequence<> {};
template <>
struct make_index_sequence<1> : index_sequence<0> {};
