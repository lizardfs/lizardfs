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

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <vector>

namespace detail {

/*! \brief Allocator with local buffer for small requests.
 *
 * This allocator class has local buffer that is used
 * to fulfill small allocation requests (<= N).
 *
 * Local buffer can be used only for one request at a time.
 * It is up to the caller to make sure this condition is met.
 *
 * The class is designed to be used in small_vector.
 */
template <class T, std::size_t N>
class static_preallocator : public std::allocator<T> {
	typedef std::allocator<T> base;

public:
	typedef typename base::value_type value_type;
	typedef typename base::pointer pointer;
	typedef typename base::const_pointer const_pointer;
	typedef typename base::reference reference;
	typedef typename base::const_reference const_reference;
	typedef typename base::size_type size_type;
	typedef typename base::difference_type difference_type;

	template <typename U>
	struct rebind {
		typedef static_preallocator<U, N> other;
	};

public:
	static_preallocator() noexcept(std::is_nothrow_constructible<base>::value) : base() {
	}

	static_preallocator(const static_preallocator &other) noexcept(
	    std::is_nothrow_constructible<base, const base &>::value)
	    : base(other.as_base()) {
	}

	static_preallocator(static_preallocator &&other) noexcept(
	    std::is_nothrow_constructible<base, base &&>::value)
	    : base(std::move(other.as_base())) {
	}

	static_preallocator &operator=(const static_preallocator &other) {
		base::operator=(other.as_base());
		return *this;
	}
	static_preallocator &operator=(static_preallocator &&other) {
		base::operator=(std::move(other.as_base()));
		return *this;
	}

	template <class U>
	static_preallocator(const static_preallocator<U, N> &other)
	    : base(other.as_base()) {
	}

	~static_preallocator() {
	}

	pointer allocate(size_type n, const_pointer *hint = nullptr) {
		if (n == 0) {
			return nullptr;
		}

		if (n <= N) {
			return reinterpret_cast<pointer>(data_.data());
		}

		return base::allocate(n, hint);
	}

	void deallocate(pointer p, size_type n) {
		if (n > N) {
			base::deallocate(p, n);
		}
	}

	using base::address;
	using base::construct;
	using base::destroy;
	using base::max_size;

protected:
	const base &as_base() const {
		return static_cast<const base &>(*this);
	}

	base &as_base() {
		return static_cast<base &>(*this);
	}

	std::array<uint8_t, N * sizeof(T)> data_;
};

template <class T1, std::size_t N1, class T2, std::size_t N2>
bool operator==(const static_preallocator<T1, N1> &a, const static_preallocator<T2, N2> &b) {
	return static_cast<const std::allocator<T1>&>(a) == static_cast<const std::allocator<T2>&>(b);
}

template <class T1, std::size_t N1, class T2, std::size_t N2>
bool operator!=(const static_preallocator<T1, N1> &a, const static_preallocator<T2, N2> &b) {
	return static_cast<const std::allocator<T1>&>(a) != static_cast<const std::allocator<T2>&>(b);
}

}  // detail

/*! \brief Class providing std::vector interface for use with small number of elements.
 *
 * small_vector contains some preallocated elements in-place,
 * which can avoid the use of dynamic storage allocation when
 * the actual number of elements is below that preallocated threshold.
 */
template <class T, std::size_t N>
class small_vector : public std::vector<T, detail::static_preallocator<T, N>> {
	typedef std::vector<T, detail::static_preallocator<T, N>> base;

public:
	typedef typename base::value_type value_type;
	typedef typename base::pointer pointer;
	typedef typename base::const_pointer const_pointer;
	typedef typename base::reference reference;
	typedef typename base::const_reference const_reference;
	typedef typename base::iterator iterator;
	typedef typename base::const_iterator const_iterator;
	typedef typename base::const_reverse_iterator const_reverse_iterator;
	typedef typename base::reverse_iterator reverse_iterator;
	typedef typename base::size_type size_type;
	typedef typename base::difference_type difference_type;
	typedef typename base::allocator_type allocator_type;

public:
	small_vector() noexcept(std::is_nothrow_constructible<base>::value) : base() {
		reserve(N);
	}

	small_vector(const small_vector &other) : base() {
		reserve(N);
		operator=(other);
	}

	small_vector(small_vector &&other) noexcept(
			std::is_nothrow_constructible<base>::value &&
			std::is_nothrow_constructible<base, base &&>::value &&
			std::is_nothrow_constructible<T, T &&>::value)
	    : base() {
		reserve(N);
		operator=(std::move(other));
	}

	small_vector(std::initializer_list<T> il) {
		reserve(std::max(N, il.size()));
		insert(end(), il);
	}

	small_vector(size_type count, const value_type &value = value_type()) : base() {
		reserve(N);
		insert(end(), count, value);
	}

	template <typename InputIterator, typename =
	          typename std::enable_if<std::is_convertible<
	          typename std::iterator_traits<InputIterator>::iterator_category,
	          std::input_iterator_tag>::value>::type>
	small_vector(InputIterator first, InputIterator last) : base() {
		reserve(std::max<size_type>(N, std::distance(first, last)));
		insert(end(), first, last);
	}

	small_vector &operator=(const small_vector &other) {
		base::operator=(other);
		return *this;
	}

	small_vector &operator=(small_vector &&other) {
		clear();

		if (other.capacity() > N) {
			base::operator=(std::move(other));

			// With std c++ library implementation in gcc
			// there is no need for two next lines. Move assignment
			// makes 'other' equal to empty vector.
			other.clear();
			other.base::shrink_to_fit();
			assert(other.capacity() == 0);

			other.reserve(N);
			return *this;
		}

		insert(end(), std::make_move_iterator(other.begin()), std::make_move_iterator(other.end()));
		other.clear();
		return *this;
	}

	void shrink_to_fit() {
		// The assumption in small_vector class
		// is that if std::vector uses local storage
		// then we have reserved exactly N elements.
		// Calling shrink_to_fit if the size() < N
		// breaks this requirement.
		if (size() >= N) {
			base::shrink_to_fit();
		}
	}

	void swap(small_vector &x) {
		if (capacity() > N && x.capacity() > N) {
			return base::swap(x);
		}

		if (capacity() <= N && x.capacity() <= N) {
			size_type m = std::min(size(), x.size());

			std::swap_ranges(begin(), begin() + m, x.begin());
			if (size() < x.size()) {
				insert(end(), std::make_move_iterator(x.begin() + m),
				       std::make_move_iterator(x.end()));
				x.erase(x.begin() + m, x.end());
			} else if (x.size() < size()) {
				x.insert(x.end(), std::make_move_iterator(begin() + m),
				         std::make_move_iterator(end()));
				erase(begin() + m, end());
			}
			return;
		}

		if (capacity() > N) {
			small_vector<T, N> tmp(std::move(*this));

			*this = std::move(x);
			x = std::move(tmp);
		} else {
			small_vector<T, N> tmp(std::move(x));

			x = std::move(*this);
			*this = std::move(tmp);
		}
	}

	using base::at;
	using base::assign;
	using base::back;
	using base::begin;
	using base::capacity;
	using base::cbegin;
	using base::cend;
	using base::clear;
	using base::crbegin;
	using base::crend;
	using base::data;
	using base::emplace;
	using base::emplace_back;
	using base::empty;
	using base::end;
	using base::erase;
	using base::front;
	using base::get_allocator;
	using base::insert;
	using base::max_size;
	using base::operator[];
	using base::pop_back;
	using base::push_back;
	using base::rbegin;
	using base::reserve;
	using base::resize;
	using base::size;
};
