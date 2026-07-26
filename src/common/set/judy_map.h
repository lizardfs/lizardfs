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

#pragma once

#include <Judy.h>
#include <array>
#include <cassert>
#include <functional>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

template <typename T1, typename T2>
class judy_map;

namespace detail {

/*! \brief Replacement of std::pair for map like interface to Judy array.
 *
 * The biggest problem with creating map like interface to Judy array is
 * that map's value_type (key + mapped value) isn't stored in Judy array.
 * In some cases the key value is not stored in memory at all, but
 * it's recalculated based on node position in Judy array. This leads to challenge
 * with returning reference to value_type.
 * judy_pair was created to overcome this problem. The key value is stored locally
 * in first_data array and variable first is a reference to this local storage.
 * Variable second is referencing mapped value in Judy array.
 *
 * Note: Key value cannot be stored as Key type because this would cause constructor/destructor
 * problems (value is still virtually stored in Judy array but we call it's destructor here).
 */
template <typename T1, typename T2>
class judy_pair {
public:
	typedef T1 first_type;
	typedef T2 second_type;

private:
	std::array<uint8_t, sizeof(Word_t)> first_data;

public:
	const first_type &first;
	second_type &second;

	judy_pair(const judy_pair &other)
	    : first_data(other.first_data),
	      first(*reinterpret_cast<first_type *>(first_data.data())),
	      second(other.second) {
	}

	judy_pair(Word_t a, second_type &b)
	    : first(*reinterpret_cast<first_type *>(first_data.data())), second(b) {
		*reinterpret_cast<Word_t *>(first_data.data()) = a;
	}

	judy_pair& operator=(const judy_pair& other) = delete;

	bool operator==(const judy_pair &other) const {
		return first == other.first && second == other.second;
	}

	bool operator!=(const judy_pair &other) const {
		return first != other.first || second != other.second;
	}

	explicit operator std::pair<T1, typename std::remove_const<T2>::type>() const {
		return std::pair<T1, typename std::remove_const<T2>::type>(first, second);
	}

	Word_t getIndex() const {
		return *(Word_t *)first_data.data();
	}
};

/*! \brief Forward iterator for judy_map. */
template <typename T1, typename T2, bool IsConst>
class judy_iterator {
public:
	typedef std::forward_iterator_tag iterator_category;
	typedef std::pair<T1, T2> value_type;
	typedef std::ptrdiff_t difference_type;
	typedef judy_pair<T1, typename std::conditional<IsConst, const T2, T2>::type> reference;
	typedef void pointer;

public:
	judy_iterator() : data_(), index_(), pvalue_() {
	}

	judy_iterator(const Pvoid_t *data, Word_t i, T2 *v) : data_(data), index_(i), pvalue_(v) {
	}

	/*!
	 * Can create const iterator from non-const iterator but not the other way around
	 * (const-ness cannot be relaxed).
	 */
	template <bool _IsConst,
	          typename = typename std::enable_if<IsConst || (IsConst == _IsConst)>::type>
	judy_iterator(const judy_iterator<T1, T2, _IsConst> &iter)
	    : data_(iter.data_), index_(iter.index_), pvalue_(iter.pvalue_) {
	}

	judy_iterator &operator++() {
		if (!data_) {
			return *this;
		}

		pvalue_ = (T2 *)JudyLNext(*data_, &index_, nullptr);
		if (!pvalue_) {
			data_ = nullptr;
			index_ = 0;
		}

		return *this;
	}

	judy_iterator operator++(int) {
		judy_iterator it(*this);
		operator++();
		return it;
	}

	reference operator*() {
		assert(pvalue_);
		return reference(index_, *pvalue_);
	}

	/*!
	 * Can copy-assign non-const iterator to const iterator but not the other way around
	 * (const-ness cannot be relaxed).
	 */
	template <bool _IsConst,
	          typename = typename std::enable_if<IsConst || (IsConst == _IsConst)>::type>
	judy_iterator &operator=(const judy_iterator<T1, T2, _IsConst> &other) {
		data_ = other.data_;
		index_ = other.index_;
		pvalue_ = other.pvalue_;
		return *this;
	}

	bool operator==(const judy_iterator &it) const {
		return data_ == it.data_ && (!data_ || index_ == it.index_);
	}

	bool operator!=(const judy_iterator &it) const {
		return !(*this == it);
	}

	/*! \brief Reload pointer to mapped value.
	 *
	 * After removing element from judy_map all iterators are partially invalidated.
	 * Calling reload on such an iterator makes it valid again.
	 */
	void reload() {
		if (data_) {
			pvalue_ = (T2 *)JudyLFirst(*data_, &index_, nullptr);
			if (!pvalue_) {
				data_ = nullptr;
				index_ = 0;
			}
		}
	}

protected:
	const Pvoid_t *data_;
	Word_t index_;
	T2 *pvalue_;

	friend class ::judy_map<T1, T2>;
	friend class judy_iterator<T1, T2, !IsConst>;
};

template <typename T1, typename T2>
bool operator==(const judy_pair<T1, T2> &a, const ::std::pair<T1, T2> &b) {
	return a.first == b.first && a.second == b.second;
}

template <typename T1, typename T2>
bool operator==(const judy_pair<T1, const T2> &a, const ::std::pair<T1, T2> &b) {
	return a.first == b.first && a.second == b.second;
}

template <typename T1, typename T2>
bool operator==(const ::std::pair<T1, T2> &a, const judy_pair<T1, T2> &b) {
	return a.first == b.first && a.second == b.second;
}

template <typename T1, typename T2>
bool operator==(const ::std::pair<T1, T2> &a, const judy_pair<T1, const T2> &b) {
	return a.first == b.first && a.second == b.second;
}

template <typename T1, typename T2>
bool operator!=(const judy_pair<T1, T2> &a, const ::std::pair<T1, T2> &b) {
	return a.first != b.first || a.second != b.second;
}

template <typename T1, typename T2>
bool operator!=(const judy_pair<T1, const T2> &a, const ::std::pair<T1, T2> &b) {
	return a.first != b.first || a.second != b.second;
}

template <typename T1, typename T2>
bool operator!=(const ::std::pair<T1, T2> &a, const judy_pair<T1, T2> &b) {
	return a.first != b.first || a.second != b.second;
}

template <typename T1, typename T2>
bool operator!=(const ::std::pair<T1, T2> &a, const judy_pair<T1, const T2> &b) {
	return a.first != b.first || a.second != b.second;
}

}  // detail

/*! \brief Implementation of map based on Judy array.
 *
 * This class gives std::map like interface to Judy array.
 * The limitation is that key_type and mapped_type must be smaller (or equal) than size of Word_t
 * (8 bytes on 64 bit architectures and 4 bytes on 32 bit).
 * The second difference to std::map is that removing element from judy_map
 * partially invalidates iterators. Each iterator keeps index (key) and pointer to mapped_value.
 * The pointer part may become invalid after erasing element from map. There is special function
 * reload that can be used to make pointer correct again.
 *
 * It is possible to implement iterator to behave fully like std::map counterpart but this would
 * require to sacrifice performance.
 */
template <typename T1, typename T2>
class judy_map {
	static_assert(sizeof(T1) <= sizeof(Word_t) && sizeof(T2) <= sizeof(Word_t),
	              "judy_map doesn't support types with size larger than size of Word_t");

public:
	typedef T1 key_type;
	typedef T2 mapped_type;
	typedef std::pair<key_type, mapped_type> value_type;

	typedef detail::judy_pair<T1, T2> reference;
	typedef detail::judy_pair<T1, const T2> const_reference;

	typedef Word_t index_type;
	typedef Word_t size_type;

	typedef detail::judy_iterator<T1, T2, false> iterator;
	typedef detail::judy_iterator<T1, T2, true> const_iterator;

public:
	judy_map() : data_() {
	}

	judy_map(const judy_map &other) : data_() {
		auto first = other.begin();
		auto last = other.end();
		for (; first != last; ++first) {
			insert_element(*first);
		}
	}

	judy_map(judy_map &&other) noexcept : data_() {
		std::swap(data_, other.data_);
	}

	template <class InputIterator>
	judy_map(InputIterator first, InputIterator last)
	    : data_() {
		insert(first, last);
	}

	judy_map(std::initializer_list<value_type> init) : data_() {
		insert(init.begin(), init.end());
	}

	~judy_map() {
		try {
			clear();
		} catch (...) {
		}
	}

	judy_map &operator=(const judy_map &other) {
		clear();
		insert(other.begin(), other.end());
		return *this;
	}

	judy_map &operator=(judy_map &&other) noexcept {
		std::swap(data_, other.data_);
		return *this;
	}

	/*! \brief Inserts element to judy_map.
	 *
	 * \param value Element to insert.
	 * \param no_check When true then check if element exists in map is not performed.
	 * \return A pair, with first element set to an iterator pointing to either
	 *         the newly inserted element or to the element with an equivalent key in the map.
	 *         The second element in the pair is set to true if a new element
	 *         was inserted or false if an equivalent key already existed.
	 */
	std::pair<iterator, bool> insert(const value_type &value, bool no_check = false) {
		if (!no_check) {
			auto it = find(value.first);
			if (it != end()) {
				return std::make_pair(it, false);
			}
		}

		return std::make_pair(insert_element(value), true);
	}

	/*! \brief Inserts element to judy_map.
	 *
	 * \param value Element to insert.
	 * \param no_check When true then check if element exists in map is not performed.
	 * \return A pair, with first element set to an iterator pointing to either
	 *         the newly inserted element or to the element with an equivalent key in the map.
	 *         The second element in the pair is set to true if a new element
	 *         was inserted or false if an equivalent key already existed.
	 */
	std::pair<iterator, bool> insert(value_type &&value, bool no_check = false) {
		if (!no_check) {
			auto it = find(value.first);
			if (it != end()) {
				return std::make_pair(it, false);
			}
		}

		JError_t error;
		std::array<uint8_t, sizeof(Word_t)> first_data{{}};

		new ((key_type *)first_data.data()) key_type(std::move(value.first));

		Word_t key = *(Word_t *)first_data.data();

		mapped_type *pvalue = (mapped_type *)JudyLIns(&data_, key, &error);
		if (!pvalue) {
			((key_type *)first_data.data())->~key_type();
			checkError(pvalue, error);
		}

		try {
			new (pvalue) mapped_type(std::move(value.second));
		} catch (...) {
			int rc = JudyLDel(&data_, key, &error);
			((key_type *)first_data.data())->~key_type();
			// It's better to throw std::bad_alloc from checkError than
			// exception created by mapped_type constructor.
			checkError(rc, error);
			throw;
		}

		return std::make_pair(iterator(&data_, key, pvalue), true);
	}

	template <class InputIterator>
	void insert(InputIterator first, InputIterator last) {
		for (; first != last; ++first) {
			insert(*first);
		}
	}

	iterator find(const key_type &key) {
		return find_index(convertKey(key));
	}

	const_iterator find(const key_type &key) const {
		return find_index(convertKey(key));
	}

	/*! \brief Find the Nth element that is present in judy_map
	 *
	 * \param nth position of element to find.
	 * \return An iterator to the element, if an element with specified position is found,
	 *         or judy_map::end otherwise.
	 */
	iterator find_nth(const Word_t &nth) {
		Word_t index;
		mapped_type *pvalue = (mapped_type *)JudyLByCount(data_, nth + 1, &index, nullptr);
		return pvalue ? iterator(&data_, index, pvalue) : iterator();
	}

	/*! \brief Find the Nth element that is present in judy_map
	 *
	 * \param nth position of element to find.
	 * \return An iterator to the element, if an element with specified position is found,
	 *         or judy_map::end otherwise.
	 */
	const_iterator find_nth(const Word_t &nth) const {
		Word_t index;
		mapped_type *pvalue = (mapped_type *)JudyLByCount(data_, nth + 1, &index, nullptr);
		return pvalue ? const_iterator(&data_, index, pvalue) : const_iterator();
	}

	/*! \brief Find element with specified Judy array index.
	 *
	 * \param index Index of element to find.
	 * \return An iterator to the element, if an element with specified index is found,
	 *         or judy_map::end otherwise.
	 */
	iterator find_index(const Word_t &index) {
		mapped_type *pvalue = (mapped_type *)JudyLGet(data_, index, nullptr);
		return pvalue ? iterator(&data_, index, pvalue) : iterator();
	}

	/*! \brief Find element with specified Judy array index.
	 *
	 * \param index Index of element to find.
	 * \return An iterator to the element, if an element with specified index is found,
	 *         or judy_map::end otherwise.
	 */
	const_iterator find_index(const Word_t &index) const {
		mapped_type *pvalue = (mapped_type *)JudyLGet(data_, index, nullptr);
		return pvalue ? const_iterator(&data_, index, pvalue) : const_iterator();
	}

	void clear() {
		if (!data_) {
			return;
		}

		Word_t index = 0;
		std::array<uint8_t, sizeof(Word_t)> key_data{{}};

		mapped_type *pvalue = (mapped_type *)JudyLFirst(data_, &index, nullptr);
		while (pvalue) {
			*(Word_t *)key_data.data() = index;
			((key_type *)key_data.data())->~key_type();
			pvalue->~mapped_type();

			pvalue = (mapped_type *)JudyLNext(data_, &index, nullptr);
		}

		JError_t error;
		Word_t rc = JudyLFreeArray(&data_, &error);
		checkError(rc, error);
	}

	size_type erase(const key_type &key) {
		auto it = find(key);

		if (it == end()) {
			return 0;
		}

		erase(it);

		return 1;
	}

	void erase(iterator it) {
		JError_t error;

		if (!it.data_ || !it.pvalue_) {
			return;
		}

		std::array<uint8_t, sizeof(Word_t)> key_data{{}};
		*(Word_t *)key_data.data() = it.index_;
		((key_type *)key_data.data())->~key_type();
		(it.pvalue_)->~mapped_type();

		int rc = JudyLDel(&data_, it.index_, &error);
		checkError(rc, error);
	}

	void erase(iterator first, iterator last) {
		while (first != last) {
			auto prev = first;
			++first;
			erase(prev);
			first.reload();
		}
	}

	iterator begin() {
		Word_t index = 0;

		mapped_type *pvalue = (mapped_type *)JudyLFirst(data_, &index, nullptr);

		return pvalue ? iterator(&data_, index, pvalue) : iterator();
	}

	iterator end() {
		return iterator();
	}

	const_iterator begin() const {
		Word_t index = 0;

		mapped_type *pvalue = (mapped_type *)JudyLFirst(data_, &index, nullptr);

		return pvalue ? const_iterator(&data_, index, pvalue) : const_iterator();
	}

	const_iterator end() const {
		return const_iterator();
	}

	const_iterator cbegin() const {
		Word_t index = 0;

		mapped_type *pvalue = (mapped_type *)JudyLFirst(data_, &index, nullptr);

		return pvalue ? const_iterator(&data_, index, pvalue) : const_iterator();
	}

	const_iterator cend() const {
		return const_iterator();
	}

	bool empty() const {
		return data_ == nullptr;
	}

	size_type size() const {
		JError_t error;

		size_type rc = JudyLCount(data_, 0, ~(size_type)0, &error);
		if (rc == 0 && JU_ERRNO(&error) > JU_ERRNO_NFMAX) {
			checkError((int)JERR, error);
		}

		return rc;
	}

	size_type max_size() const {
		return std::numeric_limits<size_type>::max();
	}

	mapped_type &operator[](const key_type &key) {
		auto it = find(key);
		if (it == end()) {
			it = insert({key, mapped_type()}, true).first;
		}
		return *(it.pvalue_);
	}

	mapped_type &operator[](key_type &&key) {
		auto it = find(key);
		if (it == end()) {
			it = insert({std::move(key), mapped_type()}, true).first;
		}
		return *it.pvalue_;
	}

	mapped_type &at(key_type key) {
		auto it = find(key);

		if (it == end()) {
			throw std::out_of_range("key not found");
		}

		return *it.pvalue_;
	}

	const mapped_type &at(const key_type &key) const {
		auto it = find(key);

		if (it == end()) {
			throw std::out_of_range("key not found");
		}

		return *it.pvalue_;
	}

	void swap(judy_map &other) {
		std::swap(data_, other.data_);
	}

	iterator lower_bound(const key_type &key) {
		return lower_bound_index(convertKey(key));
	}

	const_iterator lower_bound(const key_type &key) const {
		return lower_bound_index(convertKey(key));
	}

	iterator upper_bound(const key_type &key) {
		return upper_bound_index(convertKey(key));
	}

	const_iterator upper_bound(const key_type &key) const {
		return upper_bound_index(convertKey(key));
	}

	/*! \brief Find lower bound using Judy array index instead of key value. */
	iterator lower_bound_index(Word_t index) {
		mapped_type *pvalue = (mapped_type *)JudyLFirst(data_, &index, nullptr);
		return pvalue ? iterator(&data_, index, pvalue) : iterator();
	}

	/*! \brief Find lower bound using Judy array index instead of key value. */
	const_iterator lower_bound_index(Word_t index) const {
		mapped_type *pvalue = (mapped_type *)JudyLFirst(data_, &index, nullptr);
		return pvalue ? const_iterator(&data_, index, pvalue) : const_iterator();
	}

	/*! \brief Find upper bound using Judy array index instead of key value. */
	iterator upper_bound_index(Word_t index) {
		mapped_type *pvalue = (mapped_type *)JudyLNext(data_, &index, nullptr);
		return pvalue ? iterator(&data_, index, pvalue) : iterator();
	}

	/*! \brief Find upper bound using Judy array index instead of key value. */
	const_iterator upper_bound_index(Word_t index) const {
		mapped_type *pvalue = (mapped_type *)JudyLNext(data_, &index, nullptr);
		return pvalue ? const_iterator(&data_, index, pvalue) : const_iterator();
	}

private:
	template<class Value>
	iterator insert_element(const Value &value) {
		JError_t error;
		std::array<uint8_t, sizeof(Word_t)> first_data{{}};

		new ((key_type *)first_data.data()) key_type(value.first);

		Word_t key = *(Word_t *)first_data.data();

		mapped_type *pvalue = (mapped_type *)JudyLIns(&data_, key, &error);
		if (!pvalue) {
			((key_type *)first_data.data())->~key_type();
			checkError(pvalue, error);
		}

		try {
			new (pvalue) mapped_type(value.second);
		} catch (...) {
			int rc = JudyLDel(&data_, key, &error);
			((key_type *)first_data.data())->~key_type();
			// It's better to throw std::bad_alloc from checkError than
			// exception created by mapped_type constructor.
			checkError(rc, error);
			throw;
		}

		return iterator(&data_, key, pvalue);
	}

	void checkError(mapped_type *pvalue, const JError_t &error) const {
		if (pvalue == PJERR) {
			if (JU_ERRNO(&error) == JU_ERRNO_NOMEM) {
				throw std::bad_alloc();
			}
			throw std::runtime_error("Judy: internal error");
		}
	}

	void checkError(int value, const JError_t &error) const {
		if (value == JERR) {
			if (JU_ERRNO(&error) == JU_ERRNO_NOMEM) {
				throw std::bad_alloc();
			}
			throw std::runtime_error("Judy: internal error");
		}
	}

	void checkError(Word_t value, const JError_t &error) const {
		if (value == (Word_t)JERR) {
			if (JU_ERRNO(&error) == JU_ERRNO_NOMEM) {
				throw std::bad_alloc();
			}
			throw std::runtime_error("Judy: internal error");
		}
	}

	/*! \brief Converts key to binary representation.
	 * \param key Key to convert.
	 * \return Binary representation of \param key.
	 */
	Word_t convertKey(const key_type &key) const {
		std::array<uint8_t, sizeof(Word_t)> key_data{{}};
		const uint8_t *key_ptr = (const uint8_t *)&key;
		for (unsigned i = 0; i < sizeof(key_type); ++i) {
			key_data[i] = key_ptr[i];
		}
		return *(Word_t *)key_data.data();
	}

protected:
	Pvoid_t data_;
};

template <typename T1, typename T2>
bool operator==(const judy_map<T1, T2> &a, const judy_map<T1, T2> &b) {
	return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template <typename T1, typename T2>
bool operator!=(const judy_map<T1, T2> &a, const judy_map<T1, T2> &b) {
	return !(a == b);
}
