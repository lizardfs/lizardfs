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
#include "common/small_vector.h"

template <typename InputIterator, typename RandomIterator, typename KeyAccessor,
	class ElementIndex = small_vector<std::size_t, 16>>
void counting_sort_copy(InputIterator first, InputIterator last, RandomIterator output,
	                KeyAccessor get_key) {
	ElementIndex element_index;

	for (auto it = first; it != last; ++it) {
		std::size_t key = get_key(*it);
		if ((key + 1) >= element_index.size()) {
			element_index.resize(key + 2, 0);
		}
		++element_index[key + 1];
	}

	for (size_t i = 1; i < element_index.size(); ++i) {
		element_index[i] += element_index[i - 1];
	}

	for (auto it = first; it != last; ++it) {
		std::size_t key = get_key(*it);
		std::size_t index = element_index[key]++;
		output[index] = std::move(*it);
	}
}

template <typename OutputIterator, typename KeyAccessor,
	class ElementIndex = small_vector<std::size_t, 16>>
void counting_sort(OutputIterator first, OutputIterator last, KeyAccessor get_key) {
	std::vector<typename std::iterator_traits<OutputIterator>::value_type> result(
	        std::distance(first, last));
	counting_sort_copy<ElementIndex>(first, last, result.begin(), std::move(get_key));
	std::copy(first, last, result.begin());
}

template <typename DataContainer, typename KeyAccessor,
	class ElementIndex = small_vector<std::size_t, 16>>
void counting_sort(DataContainer &data, KeyAccessor get_key) {
	DataContainer result(data.size());
	counting_sort_copy<typename DataContainer::iterator, typename DataContainer::iterator,
	                   KeyAccessor, ElementIndex>(data.begin(), data.end(), result.begin(),
	                                              std::move(get_key));
	data = std::move(result);
}
