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
#include <stdexcept>

/*! \brief Class providing std::vector like interface to subrange of vector. */
template <class V, typename S = typename V::size_type, typename = void>
class vector_range {
public:
	typedef V vector_type;
	typedef typename V::value_type value_type;
	typedef typename V::pointer pointer;
	typedef typename V::const_pointer const_pointer;
	typedef typename V::reference reference;
	typedef typename V::const_reference const_reference;
	typedef typename V::iterator iterator;
	typedef typename V::const_iterator const_iterator;
	typedef typename V::const_reverse_iterator const_reverse_iterator;
	typedef typename V::reverse_iterator reverse_iterator;
	typedef typename V::size_type size_type;
	typedef typename V::difference_type difference_type;
	typedef typename V::allocator_type allocator_type;
	typedef S size_store_type;

	/*! \brief Constructor.
	 * \param data data vector
	 * \param offset start of subrange
	 * \param size reference to variable storing length of subrange
	 */
	vector_range(vector_type &data, size_type offset, size_store_type &size) noexcept
	  : data_(data),
	    offset_(offset),
	    size_(size) {
	}

	vector_range(const vector_range &other) noexcept
	  : data_(other.data_),
	    offset_(other.offset_),
	    size_(other.size_) {
	}

	vector_range(vector_range &&other) noexcept
	  : data_(other.data_),
	    offset_(other.offset_),
	    size_(other.size_) {
	}

	~vector_range() {
	}

	vector_range &operator=(const vector_range &x) {
		if (x.size_ > size_) {
			std::copy_n(x.begin(), size_, begin());
			data_.insert(end(), std::next(x.begin(), size_), x.end());
			size_ = x.size_;
		} else {
			resize(x.size_);
			std::copy(x.begin(), x.end(), begin());
		}
		return *this;
	}

	vector_range &operator=(const vector_type &x) {
		if (x.size_ > size_) {
			std::copy_n(x.begin(), size_, begin());
			data_.insert(end(), std::next(x.begin(), size_), x.end());
			size_ = x.size_;
		} else {
			resize(x.size_);
			std::copy(x.begin(), x.end(), begin());
		}
		return *this;
	}

	vector_range &operator=(std::initializer_list<value_type> list) {
		if (list.size() > size_) {
			std::copy_n(list.begin(), size_, begin());
			data_.insert(end(), std::next(list.begin(), size_), list.end());
			size_ = list.size();
		} else {
			resize(list.size());
			std::copy(list.begin(), list.end(), begin());
		}
		return *this;
	}

	void assign(size_type n, const value_type &val) {
		if (size_ < n) {
			std::fill(begin(), end(), val);
			insert(end(), n - size_, val);
		} else {
			resize(n);
			std::fill(begin(), end(), val);
		}
	}

	template <typename InputIterator,
	          typename = typename std::enable_if<std::is_convertible<
	              typename std::iterator_traits<InputIterator>::iterator_category,
	              std::input_iterator_tag>::value>::type>
	void assign(InputIterator first, InputIterator last) {
		size_type s = std::distance(first, last);
		if (s > size_) {
			std::copy_n(first, size_, begin());
			data_.insert(end(), std::next(first, size_), last);
			size_ = s;
		} else {
			resize(s);
			std::copy(first, last, begin());
		}
	}

	void assign(std::initializer_list<value_type> list) {
		assign(list.begin(), list.end());
	}

	iterator begin() noexcept {
		return data_.begin() + offset_;
	}

	const_iterator begin() const noexcept {
		return data_.begin() + offset_;
	}

	const_iterator cbegin() const noexcept {
		return data_.cbegin() + offset_;
	}

	iterator end() noexcept {
		return data_.begin() + (offset_ + size_);
	}

	const_iterator end() const noexcept {
		return data_.begin() + (offset_ + size_);
	}

	const_iterator cend() const noexcept {
		return data_.cbegin() + (offset_ + size_);
	}

	size_type size() const noexcept {
		return size_;
	}

	size_type max_size() const noexcept {
		return std::min(data_.max_size() - (data_.size() - size_),
		                (size_type)std::numeric_limits<size_store_type>::max());
	}

	void resize(size_type nsize) {
		if (nsize > size_) {
			data_.insert(end(), nsize - size_, value_type());
		} else if (nsize < size_) {
			data_.erase(begin() + nsize, end());
		}

		size_ = nsize;
	}

	void resize(size_type nsize, const value_type &x) {
		if (nsize > size_) {
			data_.insert(end(), nsize - size_, x);
		} else if (nsize < size_) {
			data_.erase(begin() + nsize, end());
		}

		size_ = nsize;
	}

	void shrink_to_fit() noexcept {
	}

	size_type capacity() const noexcept {
		return data_.capacity() - (data_.size() - size_);
	}

	bool empty() const noexcept {
		return size_ == 0;
	}

	bool full() const noexcept {
		return size_ == max_size();
	}

	void reserve(size_type n) noexcept {
	}

	reference operator[](size_type n) noexcept {
		assert(n < size_);
		return *(begin() + n);
	}

	const_reference operator[](size_type n) const noexcept {
		assert(n < size_);
		return *(begin() + n);
	}

	reference at(size_type n) {
		if (n >= size_) {
			throw std::out_of_range("vector_range: out of range");
		}
		return (*this)[n];
	}

	const_reference at(size_type n) const {
		if (n >= size_) {
			throw std::out_of_range("vector_range: out of range");
		}
		return (*this)[n];
	}

	reference front() noexcept {
		return *begin();
	}

	const_reference front() const noexcept {
		return *begin();
	}

	reference back() noexcept {
		return *(begin() + (size_ - 1));
	}

	const_reference back() const noexcept {
		return *(begin() + (size_ - 1));
	}

	vector_type &data() noexcept {
		return data_;
	}

	const vector_type &data() const noexcept {
		return data_;
	}

	void push_back(const value_type &x) {
		data_.insert(end(), x);
		size_++;
	}

	void push_back(value_type &&x) {
		data_.insert(end(), std::move(x));
		size_++;
	}

	void pop_back() noexcept {
		assert(size_ > 0);
		resize(size_ - 1);
	}

	iterator insert(iterator position, const value_type &x) {
		iterator it = data_.insert(position, x);
		size_++;
		return it;
	}

	iterator insert(iterator position, value_type &&x) {
		iterator it = data_.insert(position, std::move(x));
		size_++;
		return it;
	}

	void insert(iterator position, std::initializer_list<value_type> l) {
		data_.insert(position, l.begin(), l.end());
		size_ += std::distance(l.begin(), l.end());
	}

	void insert(iterator position, size_type n, const value_type &x) {
		data_.insert(position, n, x);
		size_ += n;
	}

	template <typename InputIterator,
	          typename = typename std::enable_if<std::is_convertible<
	              typename std::iterator_traits<InputIterator>::iterator_category,
	              std::input_iterator_tag>::value>::type>
	iterator insert(iterator position, InputIterator first, InputIterator last) {
		auto it = data_.insert(position, first, last);
		size_ += std::distance(first, last);
		return it;
	}

	iterator erase(iterator position) {
		auto it = data_.erase(position);
		size_ = size_ > 0 ? size_ - 1 : 0;
		return it;
	}

	iterator erase(iterator first, iterator last) {
		auto it = data_.erase(first, last);
		size_type length = std::distance(first, last);
		size_ = size_ > length ? size_ - length : 0;
		return it;
	}

	void clear() {
		resize(0);
	}

protected:
	vector_type &data_;
	size_type offset_;
	size_store_type &size_;
};

/*! \brief Class providing std::vector like interface to subrange of vector.
 *
 * This is specialization of vector range for const vector. The assumption is
 * that we can't change underlying vector. So the only function available
 * from std::vector are those that don't modify vector
 * and don't change subrange length.
 */
template <class V, typename S>
class vector_range<V, S, typename std::enable_if<std::is_const<V>::value>::type> {
public:
	typedef V vector_type;
	typedef typename V::value_type value_type;
	typedef typename V::const_pointer pointer;
	typedef typename V::const_pointer const_pointer;
	typedef typename V::const_reference reference;
	typedef typename V::const_reference const_reference;
	typedef typename V::const_iterator iterator;
	typedef typename V::const_iterator const_iterator;
	typedef typename V::const_reverse_iterator const_reverse_iterator;
	typedef typename V::const_reverse_iterator reverse_iterator;
	typedef typename V::size_type size_type;
	typedef typename V::difference_type difference_type;
	typedef typename V::allocator_type allocator_type;
	typedef S size_store_type;

	/*! \brief Constructor.
	 * \param data data vector
	 * \param offset start of subrange
	 * \param size subrange size
	 */
	vector_range(const vector_type &data, size_type offset, size_type size) noexcept
	  : data_(data),
	    offset_(offset),
	    size_(size) {
	}

	vector_range(const vector_range &other) noexcept
	  : data_(other.data_),
	    offset_(other.offset_),
	    size_(other.size_) {
	}

	vector_range(vector_range &&other) noexcept
	  : data_(other.data_),
	    offset_(other.offset_),
	    size_(other.size_) {
	}

	~vector_range() {
	}

	const_iterator begin() const noexcept {
		return data_.begin() + offset_;
	}

	const_iterator cbegin() const noexcept {
		return data_.cbegin() + offset_;
	}

	const_iterator end() const noexcept {
		return data_.begin() + (offset_ + size_);
	}

	const_iterator cend() const noexcept {
		return data_.cbegin() + (offset_ + size_);
	}

	size_type size() const noexcept {
		return size_;
	}

	size_type max_size() const noexcept {
		return data_.max_size() - (data_.size() - size_);
	}

	size_type capacity() const noexcept {
		return data_.capacity() - (data_.size() - size_);
	}

	bool empty() const noexcept {
		return size_ == 0;
	}

	bool full() const noexcept {
		return size_ == max_size();
	}

	const_reference operator[](size_type n) const noexcept {
		assert(n < size_);
		return *(begin() + n);
	}

	const_reference at(size_type n) const {
		if (n >= size_) {
			throw std::out_of_range("vector_range: out of range");
		}
		return (*this)[n];
	}

	const_reference front() const noexcept {
		return *begin();
	}

	const_reference back() const noexcept {
		return *(begin() + (size_ - 1));
	}

	const vector_type &data() const noexcept {
		return data_;
	}

protected:
	const vector_type &data_;
	size_type offset_;
	size_type size_;
};
