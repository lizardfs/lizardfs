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
#include <cassert>
#include <functional>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "flat_set.h"

/*!
 * Memory-efficient container - flat_map.
 * It has interface as close as possible to std::map.
 * It is possible to pass other container (with vector-like interface)
 * to this template as a parameter - they will be used for storage.
 * Time complexity for insert operation is O(n), for find operations it is O(log n).
 *
 */
template <typename Key, typename T, typename C = std::vector<std::pair<Key, T>>,
	class Compare = std::less<Key>>
class flat_map {
private:
	class internal_compare;

public:
	typedef Key key_type;
	typedef T mapped_type;
	typedef Compare key_compare;
	typedef internal_compare value_compare;
	typedef C container_type;
	typedef std::pair<key_type, mapped_type> value_type;

	typedef typename C::allocator_type allocator_type;
	typedef typename container_type::size_type size_type;
	typedef typename container_type::difference_type difference_type;
	typedef value_type& reference;
	typedef const value_type& const_reference;

	typedef flat_set<value_type, container_type, value_compare> set_type;

	typedef typename container_type::pointer pointer;
	typedef typename container_type::const_pointer const_pointer;

	typedef typename set_type::iterator iterator;
	typedef typename set_type::const_iterator const_iterator;
	typedef typename set_type::reverse_iterator reverse_iterator;
	typedef typename set_type::const_reverse_iterator const_reverse_iterator;

	flat_map() noexcept(std::is_nothrow_constructible<set_type, value_compare&&>::value
			&& std::is_nothrow_constructible<value_compare>::value)
		: container_(value_compare()) {
	}

	explicit flat_map(const key_compare &comp) noexcept(
			std::is_nothrow_constructible<set_type, value_compare &&>::value
			&& std::is_nothrow_constructible<value_compare, const key_compare &>::value)
		: container_(value_compare(comp)) {
	}

	explicit flat_map(container_type &&container) noexcept(
			std::is_nothrow_constructible<set_type, container_type &&>::value)
		: container_(std::move(container)) {
	}

	/*!
	 * Construct flat map with the contents of the range [first, last).
	 * \param sorted - Is the range sorted? In this case no duplicates are allowed.
	 *                 This makes construction O(n) instead of O(nlogn).
	 */
	template <class InputIt>
	flat_map(InputIt first, InputIt last, bool sorted = false, const Compare &comp = Compare())
		noexcept(std::is_nothrow_constructible<
				set_type, InputIt, InputIt, bool, value_compare &&>::value
			&& std::is_nothrow_constructible<value_compare, Compare>::value)
		: container_(first, last, sorted, value_compare(comp)) {
	}

	flat_map(const flat_map &other) noexcept(
			std::is_nothrow_constructible<set_type, const set_type &>::value)
		: container_(other.container_){};

	flat_map(flat_map &&other) noexcept(std::is_nothrow_constructible<set_type, set_type &&>::value)
		: container_(std::move(other.container_)) {
	}

	/*!
	 * Construct flat map based on initializer list.
	 * \param sorted - Is the initializer list sorted? In this case no duplicates are allowed.
	 *                 This makes construction O(n) instead of O(nlogn).
	 */
	flat_map(std::initializer_list<value_type> init, bool sorted = false,
			const Compare& comp = Compare())
		: container_(init, sorted, value_compare(comp)) {
	}

	flat_map &operator=(const flat_map &other) {
		container_ = other.container_;
		return *this;
	}

	flat_map &operator=(flat_map &&other) {
		container_ = std::move(other.container_);
		return *this;
	}

	flat_map& operator=(std::initializer_list<value_type> ilist) {
		container_ = ilist;
		return *this;
	}

	void swap(flat_map &other) {
		std::swap(*this, other);
	}

	allocator_type get_allocator() const noexcept {
		return container_.get_allocator();
	}

	mapped_type &at(const key_type &key) {
		auto it = lower_bound(key);
		if (it != container_.end() && !less_than(key, it->first)) {
			return it->second;
		}

		throw std::out_of_range("key not found");
	}

	const mapped_type &at(const key_type &key) const {
		auto it = lower_bound(key);
		if (it != container_.end() && !less_than(key, it->first)) {
			return it->second;
		}

		throw std::out_of_range("key not found");
	}

	mapped_type &operator[](const key_type &key) {
		auto it = lower_bound(key);
		if (it != container_.end() && !less_than(key, it->first)) {
			return it->second;
		}

		const auto &ret = container_.insert(it, {key, mapped_type()});
		return ret->second;
	}

	mapped_type &operator[](const key_type &&key) {
		auto it = lower_bound(key);
		if (it != container_.end() && !less_than(key, it->first)) {
			return it->second;
		}

		const auto &ret = container_.insert(it, {std::move(key), mapped_type()});
		return ret->second;
	}

	iterator erase(iterator pos) {
		return container_.erase(pos);
	}

	iterator erase(iterator first, iterator last) {
		return container_.erase(first, last);
	}

	size_type erase(const key_type& key) {
		iterator it = lower_bound(key);
		if (it != container_.end() && !less_than(key, it->first)) {
			container_.erase(it);
			return 1;
		} else  {
			return 0;
		}
	}

	std::pair<iterator, bool> insert(const value_type &value) {
		return container_.insert(value);
	}

	std::pair<iterator, bool> insert(value_type &&value) {
		return container_.insert(std::move(value));
	}

	iterator insert(iterator hint, const value_type &value) {
		return container_.insert(hint, value);
	}

	iterator insert(iterator hint, value_type &&value) {
		return container_.insert(hint, std::move(value));
	}

	iterator begin() noexcept {
		return container_.begin();
	}

	const_iterator begin() const noexcept {
		return container_.begin();
	}

	const_iterator cbegin() const noexcept {
		return container_.cbegin();
	}

	iterator end() noexcept {
		return container_.end();
	}

	const_iterator end() const noexcept {
		return container_.end();
	}

	const_iterator cend() const noexcept {
		return container_.cend();
	}

	reverse_iterator rbegin() noexcept {
		return container_.rbegin();
	}

	const_reverse_iterator rbegin() const noexcept {
		return container_.rbegin();
	}

	const_reverse_iterator crbegin() const noexcept {
		return container_.crbegin();
	}

	reverse_iterator rend() noexcept {
		return container_.rend();
	}

	const_reverse_iterator rend() const noexcept {
		return container_.rend();
	}

	const_reverse_iterator crend() const noexcept {
		return container_.crend();
	}

	size_type size() const noexcept {
		return container_.size();
	}

	container_type& data() noexcept {
		return container_.data();
	}

	const container_type& data() const noexcept {
		return container_.data();
	}

	size_type max_size() const noexcept {
		return container_.max_size();
	}

	bool full() const noexcept {
		return size() == max_size();
	}

	void clear() {
		container_.clear();
	}

	bool empty() const noexcept {
		return container_.empty();
	}

	// Lookup
	size_type count(const key_type &key) const {
		return find(key) != end();
	}

	/*!
	 * Returns an iterator pointing to the first element in the map
	 * that is equal to x.
	 * The map must be at least partially ordered, i.e. partitioned
	 * with respect to the expression custom_less(element, value).
	 * \param custom_less - should be equivalent to:
	 *                      bool custom_less(const K &a, const K &b);
	 */
	template<typename K, typename CustomLess>
	iterator find(const K &key, CustomLess custom_less) {
		auto it = container_.lower_bound(key,
			[custom_less](const value_type &a, const key_type &b) {
				return custom_less(a.first, b);
			});

		if (it != container_.end() && !custom_less(key, it->first)) {
			return it;
		}

		return container_.end();
	}

	/*!
	 * Returns an iterator pointing to the first element in the map
	 * that is equal to x.
	 * The map must be at least partially ordered, i.e. partitioned
	 * with respect to the expression custom_less(element, value).
	 * \param custom_less - should be equivalent to:
	 *                      bool custom_less(const K &a, const K &b);
	 */
	template<typename K, typename CustomLess>
	const_iterator find(const K &key, CustomLess custom_less) const {
		auto it = container_.lower_bound(key,
			[custom_less](const value_type &a, const key_type &b) {
				return custom_less(a.first, b);
			});

		if (it != container_.end() && !custom_less(key, it->first)) {
			return it;
		}

		return container_.end();
	}

	iterator find(const key_type &key) {
		return find(key, container_.value_comp());
	}

	const_iterator find(const key_type &key) const {
		return find(key, container_.value_comp());
	}

	iterator find_nth(size_type nth) {
		if (nth < container_.size()) {
			return container_.begin() + nth;
		}
		return container_.end();
	}

	const_iterator find_nth(size_type nth) const {
		if (nth < container_.size()) {
			return container_.begin() + nth;
		}
		return container_.end();
	}

	iterator lower_bound(const key_type &key) {
		return container_.lower_bound(key, container_.value_comp());
	}

	const_iterator lower_bound(const key_type &key) const {
		return container_.lower_bound(key, container_.value_comp());
	}

	iterator upper_bound(const key_type &key) {
		return container_.upper_bound(key, container_.value_comp());
	}

	const_iterator upper_bound(const key_type &key) const {
		return container_.upper_bound(key, container_.value_comp());
	}


	// Observers
	const key_compare &key_comp() const noexcept {
		return static_cast<Compare&>(container_.value_comp());
	}

	const value_compare &value_comp() const noexcept {
		return container_.value_comp();
	}

private:
	class internal_compare : public Compare {
	public:
		internal_compare() noexcept(std::is_nothrow_constructible<Compare>::value) : Compare() {
		}
		internal_compare(const internal_compare &other) noexcept(
				std::is_nothrow_constructible<Compare, const Compare &>::value)
			: Compare(other) {
		}
		internal_compare(internal_compare &&other) noexcept(
				std::is_nothrow_constructible<Compare, Compare &&>::value)
			: Compare(std::move(other)) {
		}
		explicit internal_compare(Compare comp) noexcept(
				std::is_nothrow_constructible<Compare, Compare &&>::value)
			: Compare(std::move(comp)) {
		}

		internal_compare &operator=(const internal_compare &other) {
			Compare::operator=(other);
			return *this;
		}
		internal_compare &operator=(internal_compare &&other) {
			Compare::operator=(std::move(other));
			return *this;
		}

		using Compare::operator();

		bool operator() (const key_type &a, const value_type &b) const {
			return Compare::operator()(a, b.first);
		}

		bool operator() (const value_type &a, const key_type &b) const {
			return Compare::operator()(a.first, b);
		}

		bool operator() (const value_type &a, const value_type &b) const {
			return Compare::operator()(a.first, b.first);
		}
	};

	bool less_than(const key_type &a, const key_type &b) const {
		return container_.value_comp()(a, b);
	}

	set_type container_;
};


template <typename Key, typename T, typename C, class Compare>
bool operator==(const flat_map<Key,T,C,Compare>& a, const flat_map<Key,T,C,Compare>& b) {
	return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template <typename Key, typename T, typename C, class Compare>
bool operator<(const flat_map<Key,T,C,Compare>& a, const flat_map<Key,T,C,Compare>& b) {
	return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

template <typename Key, typename T, typename C, class Compare>
bool operator!=(const flat_map<Key,T,C,Compare>& a, const flat_map<Key,T,C,Compare>& b) {
	return !(a == b);
}

template <typename Key, typename T, typename C, class Compare>
bool operator>(const flat_map<Key,T,C,Compare>& a, const flat_map<Key,T,C,Compare>& b) {
	return b < a;
}

template <typename Key, typename T, typename C, class Compare>
bool operator<=(const flat_map<Key,T,C,Compare>& a, const flat_map<Key,T,C,Compare>& b) {
	return !(b < a);
}

template <typename Key, typename T, typename C, class Compare>
bool operator>=(const flat_map<Key,T,C,Compare>& a, const flat_map<Key,T,C,Compare>& b) {
	return !(a < b);
}

template <typename Key, typename T, typename C, class Compare>
void swap(flat_map<Key,T,C,Compare>& a, flat_map<Key,T,C,Compare>& b) {
	a.swap(b);
}
