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
#include <type_traits>
#include <vector>

/*!
 * Memory-efficient container - flat_set.
 * It has interface as close as possible to std::set.
 * It is possible to pass other container (with vector-like interface)
 * to this template as a parameter - they will be used for storage.
 * Time complexity for insert operation is O(n), for find operations it is O(log n).
 *
 * NOTICE: emplace makes no sense so it's not implemented
 * NOTICE: flat_set derives from Compare in order to prevent additional memory allocation (EBO).
 */
template <typename T, typename C = std::vector<T>, class Compare = std::less<T>>
class flat_set : private Compare {
private:
	typedef Compare base;

public:
	typedef C container_type;

	typedef T key_type;
	typedef T value_type;
	typedef typename container_type::size_type size_type;
	typedef std::ptrdiff_t difference_type;
	typedef Compare key_compare;
	typedef Compare value_compare;
	typedef value_type& reference;
	typedef const value_type& const_reference;
	typedef T* pointer;
	typedef const T* const_pointer;
	typedef typename container_type::allocator_type allocator_type;
	typedef typename container_type::iterator iterator;
	typedef typename container_type::const_iterator const_iterator;
	typedef typename container_type::reverse_iterator reverse_iterator;
	typedef typename container_type::const_reverse_iterator const_reverse_iterator;

	// constructors
	flat_set() noexcept(std::is_nothrow_constructible<base>::value
	                    && std::is_nothrow_constructible<container_type>::value)
	    : base(),
	      container_() {
	}

	explicit flat_set(const Compare &comp) noexcept(
	    std::is_nothrow_constructible<base, const Compare &>::value
	    && std::is_nothrow_constructible<container_type>::value)
	    : base(comp),
	      container_() {
	}

	/*!
	 * Construct flat set with the contents of the range [first, last).
	 * \param sorted - Is the range sorted? In this case no duplicates are allowed.
	 *                 This makes construction O(n) instead of O(nlogn).
	 */
	template<class InputIt>
	flat_set(InputIt first, InputIt last, bool sorted = false, const Compare& comp = Compare())
		: base(comp),
		  container_() {
		if (sorted) {
			container_.insert(container_.end(), first, last);
		} else {
			insert(first, last);
		}
	}

	flat_set(const flat_set &other) noexcept(
	    std::is_nothrow_constructible<base, const base &>::value
	    && std::is_nothrow_constructible<container_type, const container_type &>::value)
	    : base(static_cast<const base &>(other)),
	      container_(other.container_) {
	}

	flat_set(flat_set &&other) noexcept(std::is_nothrow_constructible<base, base &&>::value
			&& std::is_nothrow_constructible<container_type, container_type &&>::value)
		: base(std::move(other)),
		  container_(std::move(other.container_)) {
	}

	/*!
	 * Construct flat set based on initializer list.
	 * \param sorted - Is the initializer list sorted? In this case no duplicates are allowed.
	 *                 This makes construction O(n) instead of O(nlogn).
	 */
	flat_set(std::initializer_list<value_type> init, bool sorted = false,
			const Compare& comp = Compare())
		: base(comp),
		  container_() {
		if (sorted) {
			container_.insert(container_.end(), init);
		} else {
			insert(init);
		}
	}

	explicit flat_set(container_type&& data, const Compare& comp = Compare()) noexcept(
			std::is_nothrow_constructible<base, const base&>::value
			&& std::is_nothrow_constructible<container_type, container_type &&>::value)
		: base(comp),
		  container_(data) {
	}

	// operator=
	flat_set &operator=(const flat_set &other) {
		base::operator=(other);
		container_ = other.container_;
		return *this;
	}

	flat_set &operator=(flat_set &&other) {
		base::operator=(std::move(other));
		container_ = std::move(other.container_);
		return *this;
	}

	flat_set& operator=(std::initializer_list<value_type> init) {
		container_.clear();
		insert(init);
		return *this;
	}


	// Allocator
	allocator_type get_allocator() const noexcept {
		return container_.get_allocator();
	}

	// Iterators
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

	// Data access
	container_type& data() noexcept {
		return container_;
	}

	const container_type& data() const noexcept {
		return container_;
	}

	// Capacity
	bool empty() const noexcept {
		return container_.empty();
	}

	size_type size() const noexcept {
		return container_.size();
	}

	size_type max_size() const noexcept {
		return container_.max_size();
	}

	bool full() const noexcept {
		return size() == max_size();
	}

	size_type capacity() const noexcept {
		return container_.capacity();
	}

	void reserve(size_type n) {
		container_.reserve(n);
	}


	// Modifiers
	void clear() noexcept {
		container_.clear();
	}

	std::pair<iterator, bool> insert(const value_type& value) {
		iterator it = lower_bound(value);
		if (it == container_.end() || less_than(value, *it)) {
			return  {container_.insert(it, value), true};
		}

		return {it, false};
	}

	std::pair<iterator, bool> insert(value_type&& value) {
		iterator it = lower_bound(value);
		if (it == container_.end() || less_than(value, *it)) {
			return  {container_.insert(it, std::move(value)), true};
		}

		return {it, false};
	}

	iterator insert(iterator hint, const value_type &value) {
		iterator first, last;

		// for more information about hints see:
		// https://gcc.gnu.org/onlinedocs/libstdc++/manual/associative.html#containers.associative.insert_hints

		if (hint == end()) {
			if (hint != begin() && less_than(*(hint - 1), value)) {
				return container_.insert(hint, value);
			}
			first = begin();
			last = end();
		} else if (less_than(value, *hint)) {
			if (hint == begin()) {
				return container_.insert(hint, value);
			}
			if (less_than(*(hint - 1), value)) {
				return container_.insert(hint, value);
			}
			first = begin();
			last = hint;
		} else if (less_than(*hint, value)) {
			if (hint == (end() - 1)) {
				return container_.insert(end(), value);
			}
			if (less_than(value, *(hint + 1))) {
				return container_.insert(hint + 1, value);
			}
			first = hint + 1;
			last = end();
		} else {
			// equivalent values;
			return hint;
		}

		iterator it = std::lower_bound(first, last, value, *static_cast<base *>(this));
		if (it == last || less_than(value, *it)) {
			return container_.insert(it, value);
		}

		return it;
	}

	iterator insert(iterator hint, value_type &&value) {
		iterator first, last;

		// for more information about hints see:
		// https://gcc.gnu.org/onlinedocs/libstdc++/manual/associative.html#containers.associative.insert_hints

		if (hint == end()) {
			if (hint != begin() && less_than(*(hint - 1), value)) {
				return container_.insert(hint, std::move(value));
			}
			first = begin();
			last = end();
		} else if (less_than(value, *hint)) {
			if (hint == begin()) {
				return container_.insert(hint, std::move(value));
			}
			if (less_than(*(hint - 1), value)) {
				return container_.insert(hint, std::move(value));
			}
			first = begin();
			last = hint;
		} else if (less_than(*hint, value)) {
			if (hint == (end() - 1)) {
				return container_.insert(end(), std::move(value));
			}
			if (less_than(value, *(hint + 1))) {
				return container_.insert(hint + 1, std::move(value));
			}
			first = hint + 1;
			last = end();
		} else {
			// equivalent values;
			return hint;
		}

		iterator it = std::lower_bound(first, last, value, *static_cast<base *>(this));
		if (it == last || less_than(value, *it)) {
			return container_.insert(it, std::move(value));
		}

		return it;
	}

	template<class InputIt>
	void insert(InputIt first, InputIt last) {
		assert(std::distance(first, last) >= 0);
		container_.reserve(container_.size() + std::distance(first, last));
		for (auto it = first; it != last; it++) {
			insert(*it);
		}
	}

	void insert(std::initializer_list<value_type> ilist) {
		container_.reserve(container_.size() + ilist.size());
		for (auto it = ilist.begin(); it != ilist.end(); it++) {
			insert(*it);
		}
	}

	iterator erase(iterator pos) {
		return container_.erase(pos);
	}

	iterator erase(iterator first, iterator last) {
		return container_.erase(first, last);
	}

	size_type erase(const key_type& key) {
		iterator it = lower_bound(key);
		if (it != container_.end() && !less_than(key, *it)) {
			container_.erase(it);
			return 1;
		} else  {
			return 0;
		}
	}

	void swap(flat_set& other) {
		std::swap(*this, other);
	}


	// Lookups
	size_type count(const key_type& key) const {
		const_iterator it = lower_bound(key);
		if (it == container_.end()) {
			return 0;
		}

		return !less_than(key, *it);
	}

	iterator find(const key_type& key) {
		return find(key, *static_cast<base*>(this));
	}

	const_iterator find(const key_type& key) const {
		return find(key, *static_cast<const base*>(this));
	}

	/*!
	 * Returns an iterator pointing to the first element in the set
	 * that is equal to x.
	 * The set must be at least partially ordered, i.e. partitioned
	 * with respect to the expression comp(element, value).
	 * \param comp - should be equivalent to:
	 *               bool cmp(const K &a, const K &b);
	 */
	template <class K, class CustomCompare>
	iterator find(const K &x, CustomCompare comp) {
		iterator it = lower_bound(x, comp);
		if (it != container_.end() && !comp(x, *it)) {
			return it;
		}

		return container_.end();
	}

	/*!
	 * Returns an iterator pointing to the first element in the set
	 * that is equal to x.
	 * The set must be at least partially ordered, i.e. partitioned
	 * with respect to the expression comp(element, value).
	 * \param comp - should be equivalent to:
	 *               bool cmp(const K &a, const K &b);
	 */
	template <class K, class CustomCompare>
	const_iterator find(const K &x, CustomCompare comp) const {
		const_iterator it = lower_bound(x, comp);
		if (it != container_.end() && !comp(x, *it)) {
			return it;
		}

		return container_.end();
	}

	iterator lower_bound(const T& key) {
		return std::lower_bound(container_.begin(), container_.end(), key,
			*static_cast<base*>(this));
	}

	const_iterator lower_bound(const T& key) const {
		return std::lower_bound(container_.begin(), container_.end(), key,
			*static_cast<const base*>(this));
	}

	/*!
	 * Returns an iterator pointing to the first element in the set
	 * that is not less than (i.e. greater or equal to) x.
	 * The set must be at least partially ordered, i.e. partitioned
	 * with respect to the expression comp(element, value).
	 * \param comp - should be equivalent to:
	 *               bool cmp(const K &a, const K &b);
	 */
	template <class K, class CustomCompare>
	iterator lower_bound(const K &x, CustomCompare comp) {
		return std::lower_bound(container_.begin(), container_.end(), x, comp);
	}

	/*!
	 * Returns an iterator pointing to the first element in the set
	 * that is not less than (i.e. greater or equal to) x.
	 * The set must be at least partially ordered, i.e. partitioned
	 * with respect to the expression comp(element, value).
	 * \param comp - should be equivalent to:
	 *               bool cmp(const K &a, const K &b);
	 */
	template <class K, class CustomCompare>
	const_iterator lower_bound(const K &x, CustomCompare comp) const {
		return std::lower_bound(container_.begin(), container_.end(), x, comp);
	}

	iterator upper_bound(const T& key) {
		return std::upper_bound(container_.begin(), container_.end(), key,
			*static_cast<base*>(this));
	}

	const_iterator upper_bound(const T& key) const {
		return std::upper_bound(container_.begin(), container_.end(), key,
			*static_cast<const base*>(this));
	}

	/*!
	 * Returns an iterator pointing to the first element in the set
	 * that is greater than x.
	 * The set must be at least partially ordered, i.e. partitioned
	 * with respect to the expression !comp(element, value).
	 * \param comp - should be equivalent to:
	 *               bool cmp(const K &a, const K &b);
	 */
	template <class K, class CustomCompare>
	iterator upper_bound(const K &x, CustomCompare comp) {
		return std::upper_bound(container_.begin(), container_.end(), x, comp);
	}

	/*!
	 * Returns an iterator pointing to the first element in the set
	 * that is greater than x.
	 * The set must be at least partially ordered, i.e. partitioned
	 * with respect to the expression !comp(element, value).
	 * \param comp - should be equivalent to:
	 *               bool cmp(const K &a, const K &b);
	 */
	template <class K, class CustomCompare>
	const_iterator upper_bound(const K &x, CustomCompare comp) const {
		return std::upper_bound(container_.begin(), container_.end(), x, comp);
	}

	std::pair<iterator, iterator> equal_range(const T& key) {
		return {lower_bound(key), upper_bound((key))};
	}

	std::pair<const_iterator, const_iterator> equal_range(const T& key) const {
		return {lower_bound(key), upper_bound((key))};
	}

	// Observers
	const key_compare& key_comp() const noexcept {
		return static_cast<base&>(*this);
	}

	const value_compare& value_comp() const noexcept {
		return static_cast<const base&>(*this);
	}

protected:

	inline bool less_than(const T& a, const T& b) const {
		return base::operator()(a, b);
	}

	container_type container_;
};

template <typename T, typename C, class Compare>
bool operator==(const flat_set<T,C,Compare>& a, const flat_set<T,C,Compare>& b) {
	return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template <typename T, typename C, class Compare>
bool operator!=(const flat_set<T,C,Compare>& a, const flat_set<T,C,Compare>& b) {
	return a.size() != b.size() || !std::equal(a.begin(), a.end(), b.begin());
}
