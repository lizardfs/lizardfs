/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace detail {

/*! \brief Generic compact vector storage class.
 *
 * This class keeps both pointer and size of data used by vector class.
 * This is generic version that doesn't allow keeping data in internal storage.
 */
template <typename Alloc, typename T, typename Size, typename Enable = void>
class compact_vector_storage : public Alloc {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
	typedef std::allocator_traits<Alloc> alloc_traits;
#else
	typedef Alloc alloc_traits;
#endif

public:
	typedef typename alloc_traits::pointer       pointer;
	typedef typename alloc_traits::const_pointer const_pointer;
	typedef typename std::conditional<std::is_void<Size>::value, unsigned int, Size>::type
	        size_type;

public:
	compact_vector_storage() : ptr_(nullptr), size_() {
	}

	pointer ptr() const {
		return ptr_;
	}

	pointer internal_ptr() {
		return pointer();
	}

	const_pointer internal_ptr() const {
		return const_pointer();
	}

	void set_ptr(pointer p) {
		ptr_ = p;
	}

	size_type size() const {
		return size_;
	}

	void set_size(size_type s) {
		size_ = s;
	}

	void swap(compact_vector_storage &v) {
		std::swap(ptr_, v.ptr_);
		std::swap(size_, v.size_);
	}

	constexpr size_type internal_size() const {
		return 0;
	}

	constexpr size_type max_size() const {
		return std::numeric_limits<size_type>::max();
	}

private:
	pointer   ptr_;
	size_type size_;
};

/*! \brief Compact vector storage class with internal store.
 *
 * This class keeps both pointer and size of data used by vector class.
 * Additionally it allows to store vector data in area occupied by pointer variable.
 */
template <typename Alloc, typename T, typename Size>
class compact_vector_storage<
	Alloc, T, Size,
	typename std::enable_if<
		std::is_trivial<T>::value && std::is_convertible<uint8_t *, T>::value &&
		!(std::is_pointer<T>::value && sizeof(T) == 8 &&
		sizeof(typename std::conditional<std::is_void<Size>::value, uint8_t,
		                                 Size>::type) <= 2)>::type> : public Alloc {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
	typedef std::allocator_traits<Alloc> alloc_traits;
#else
	typedef Alloc alloc_traits;
#endif

public:
	typedef typename alloc_traits::pointer       pointer;
	typedef typename alloc_traits::const_pointer const_pointer;
	typedef typename std::conditional<std::is_void<Size>::value, unsigned int, Size>::type
	        size_type;

public:
	compact_vector_storage() : ptr_(nullptr), size_() {
	}

	pointer ptr() const {
		return ptr_;
	}

	pointer internal_ptr() {
		return static_cast<pointer>(static_cast<uint8_t*>(data_));
	}

	const_pointer internal_ptr() const {
		return static_cast<const_pointer>(static_cast<const uint8_t*>(data_));
	}

	void set_ptr(pointer p) {
		ptr_ = p;
	}

	size_type size() const {
		return size_;
	}

	void set_size(size_type s) {
		size_ = s;
	}

	void swap(compact_vector_storage &v) {
		std::swap(ptr_, v.ptr_);
		std::swap(size_, v.size_);
	}

	constexpr size_type internal_size() const {
		return sizeof(data_);
	}

	constexpr size_type max_size() const {
		return std::numeric_limits<size_type>::max();
	}

private:
	union {
		volatile pointer ptr_;
		uint8_t data_[sizeof(pointer)];
	};
	size_type size_;
};

/*! \brief Compact vector storage class for 64 bit system
 *
 * On 64 bit system there is a limit on where memory can be allocated in user space.
 * Also current processors have build-in hardware limit on how much memory can be addressed.
 * User space limit by OS:
 *   - Linux   128 TB
 *   - Windows 8 TB
 *
 * This implies that top bits of any address returned by malloc must be equal to 0.
 * So we can use this wasted space to store vector size.
 * Also each address returned by malloc is aligned to multiply of >=8 (tcmalloc can return pointer
 * aligned to 8, linux and windows malloc on x86_64 returns pointer with 16 bytes alignment).
 * This gives us extra 3 low bits that we can use.
 *
 * The only problem with this approach is memory leak check in valgrind.
 * If the program exit without calling compact_vector destructor (i.e. exit())
 * then valgrind reports that the memory allocated by compact_vector is unreachable.
 * Leak check in valgrind is done by testing if there is memory value pointing
 * at the allocated block. Because our pointer is obfuscated valgrind can't find
 * the correct pointer and reports leak.
 * To somehow  elevate the problem in debug mode there is second debug_ptr_ variable
 * which just stores pointer (and isn't used for anything else) so valgrind can find
 * correct pointer.
 */
template <typename Alloc, typename T, typename Size>
class compact_vector_storage<
	Alloc, T *, Size,
	typename std::enable_if<sizeof(T *) == 8 &&
	                        sizeof(typename std::conditional<std::is_void<Size>::value, uint8_t,
	                                                         Size>::type) <= 2>::type>
	: public Alloc {
public:
	typedef T       *pointer;
	typedef const T *const_pointer;
	typedef typename std::conditional<std::is_void<Size>::value, uint32_t, Size>::type size_type;

private:
	static const uint64_t size_shift =
	        64 - (sizeof(size_type) <= 2 ? 8 * sizeof(size_type) : 20);
	static const uint64_t ptr_mask = (static_cast<uint64_t>(1) << size_shift) - 1;
	static const uint64_t ptr_shift = 3;
	static const uint64_t size_mask = ~ptr_mask;

public:
	compact_vector_storage() : ptr_(0) {
#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
		debug_ptr_ = 0;
#endif
	}

	pointer ptr() const {
		assert(debug_ptr_ == reinterpret_cast<pointer>((ptr_ & ptr_mask) << ptr_shift));
		return reinterpret_cast<pointer>((ptr_ & ptr_mask) << ptr_shift);
	}

	pointer internal_ptr() {
#ifdef WORDS_BIGENDIAN
		return reinterpret_cast<pointer>(&data_[(64 - size_shift + 7) / 8]);
#else
		return reinterpret_cast<pointer>(static_cast<uint8_t *>(data_));
#endif
	}

	const_pointer internal_ptr() const {
#ifdef WORDS_BIGENDIAN
		return reinterpret_cast<const_pointer>(&data_[(64 - size_shift + 7) / 8]);
#else
		return reinterpret_cast<const_pointer>(static_cast<const uint8_t *>(data_));
#endif
	}

	void set_ptr(pointer p) {
		assert((reinterpret_cast<uint64_t>(p) & ((static_cast<uint64_t>(1) << ptr_shift) - 1)) == 0);
		assert(((reinterpret_cast<uint64_t>(p) >> ptr_shift) & size_mask) == 0);
		assert((reinterpret_cast<uint64_t>(p) &
		        ((static_cast<uint64_t>(1) << ptr_shift) - 1)) == 0);
		ptr_ = ((reinterpret_cast<uint64_t>(p) >> ptr_shift) & ptr_mask) |
		       (ptr_ & size_mask);
#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
		debug_ptr_ = p;
#endif
		assert(debug_ptr_ == ptr());
	}

	size_type size() const {
		return ptr_ >> size_shift;
	}

	void set_size(size_type s) {
		assert(s <= max_size());
		ptr_ = (ptr_ & ptr_mask) | (static_cast<uint64_t>(s) << size_shift);
	}

	void swap(compact_vector_storage &v) noexcept {
		std::swap(ptr_, v.ptr_);
#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
		std::swap(debug_ptr_, v.debug_ptr_);
#endif
	}

	constexpr size_type internal_size() const {
		return size_shift / 8;
	}

	constexpr size_type max_size() const {
		return (static_cast<uint64_t>(1) << (64 - size_shift)) - 1;
	}

private:
	union {
		volatile uint64_t ptr_;
		uint8_t data_[8];
	};
#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
	pointer debug_ptr_;
#endif
};

/*! \brief Base class for compact_vector
 *
 * The idea behind creating base class for compact_vector is to simplify
 * handling of exception generated in constructors.
 */
template <typename T, typename Size, typename Alloc>
struct compact_vector_base {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
	typedef typename std::allocator_traits<Alloc>::template rebind_alloc<T> allocator_type;
#else
	typedef typename Alloc::template rebind<T>::other allocator_type;
#endif

	static_assert(std::is_empty<Alloc>::value,"compact_vector requires empty Allocator type");

	typedef compact_vector_storage<allocator_type, typename allocator_type::pointer, Size> storage_type;
	typedef typename storage_type::pointer                  pointer;
	typedef typename storage_type::const_pointer            const_pointer;
	typedef typename storage_type::size_type                size_type;

	compact_vector_base() : storage_() {
	}

	explicit compact_vector_base(compact_vector_base &&x) noexcept : storage_() {
		storage_.swap(x.storage_);
	}

	explicit compact_vector_base(size_type n) : storage_() {
		set_storage(allocate(n), n);
	}

	~compact_vector_base() {
		deallocate(get_ptr(), get_size());
	}

	constexpr std::size_t internal_size() const {
		return storage_.internal_size() / sizeof(T);
	}

	pointer get_ptr() {
		if (storage_.size() > 0 && storage_.size() <= internal_size()) {
			return storage_.internal_ptr();
		}
		return storage_.ptr();
	}

	const_pointer get_ptr() const {
		if (storage_.size() > 0 && storage_.size() <= internal_size()) {
			return storage_.internal_ptr();
		}
		return storage_.ptr();
	}

	std::size_t get_size() const {
		return storage_.size();
	}

	void set_ptr(pointer p) {
		if (storage_.size() == 0 || storage_.size() > internal_size()) {
			storage_.set_ptr(p);
		}
	}

	void set_size(std::size_t s) {
		storage_.set_size(s);
	}

	void set_storage(pointer p, std::size_t s) {
		if (s == 0 || s > internal_size()) {
			storage_.set_ptr(p);
		}
		storage_.set_size(s);
	}

	pointer allocate(size_type n) {
		if (n == 0) {
			return pointer();
		}

		if (n > 0 && n <= internal_size()) {
			return storage_.internal_ptr();
		}

#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
		typedef std::allocator_traits<allocator_type> Tr;
		return Tr::allocate(get_allocator(), n);
#else
		return get_allocator().allocate(n);
#endif
	}

	void deallocate(pointer p, size_type n) {
		if (p && n <= internal_size()) {
			assert(p == storage_.internal_ptr());
			return;
		}

#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
		typedef std::allocator_traits<allocator_type> Tr;
		if (p) {
			Tr::deallocate(get_allocator(), p, n);
		}
#else
		if (p) {
			get_allocator().deallocate(p, n);
		}
#endif
	}

	allocator_type &get_allocator() {
		// conversion required so we don't call allocator_type constructor.
		return *static_cast<allocator_type *>(&storage_);
	}

	const allocator_type &get_allocator() const {
		// conversion required so we don't call allocator_type constructor.
		return *static_cast<const allocator_type *>(&storage_);
	}

	storage_type storage_;
};

/*! \brief Generic simple iterator class.
 */
template <typename Iterator, typename Container>
class normal_iterator {
protected:
	Iterator                               current_;
	typedef std::iterator_traits<Iterator> traits_type;

public:
	typedef Iterator                                iterator_type;
	typedef typename traits_type::iterator_category iterator_category;
	typedef typename traits_type::value_type        value_type;
	typedef typename traits_type::difference_type   difference_type;
	typedef typename traits_type::reference         reference;
	typedef typename traits_type::pointer           pointer;

	constexpr normal_iterator() noexcept : current_(Iterator()) {
	}

	explicit normal_iterator(const Iterator &i) noexcept : current_(i) {
	}

	// Conversion from iterator to const_iterator
	template <typename Iter>
	normal_iterator(const normal_iterator<
	        Iter,
	        typename std::enable_if<std::is_same<Iter, typename Container::pointer>::value,
	                                Container>::type> &i) noexcept : current_(i.current()) {
	}

	reference operator*() const noexcept {
		return *current_;
	}

	pointer operator->() const noexcept {
		return current_;
	}

	normal_iterator &operator++() noexcept {
		++current_;
		return *this;
	}

	normal_iterator operator++(int) noexcept {
		return normal_iterator(current_++);
	}

	normal_iterator &operator--() noexcept {
		--current_;
		return *this;
	}

	normal_iterator operator--(int) noexcept {
		return normal_iterator(current_--);
	}

	reference operator[](difference_type n) const noexcept {
		return current_[n];
	}

	normal_iterator &operator+=(difference_type n) noexcept {
		current_ += n;
		return *this;
	}

	normal_iterator operator+(difference_type n) const noexcept {
		return normal_iterator(current_ + n);
	}

	normal_iterator &operator-=(difference_type n) noexcept {
		current_ -= n;
		return *this;
	}

	normal_iterator operator-(difference_type n) const noexcept {
		return normal_iterator(current_ - n);
	}

	const Iterator &current() const noexcept {
		return current_;
	}
};

template <typename IteratorL, typename IteratorR, typename Container>
inline bool operator==(const normal_iterator<IteratorL, Container> &lhs,
	const normal_iterator<IteratorR, Container> &rhs) noexcept {
	return lhs.current() == rhs.current();
}

template <typename Iterator, typename Container>
inline bool operator==(const normal_iterator<Iterator, Container> &lhs,
	const normal_iterator<Iterator, Container> &rhs) noexcept {
	return lhs.current() == rhs.current();
}

template <typename IteratorL, typename IteratorR, typename Container>
inline bool operator!=(const normal_iterator<IteratorL, Container> &lhs,
	const normal_iterator<IteratorR, Container> &rhs) noexcept {
	return lhs.current() != rhs.current();
}

template <typename Iterator, typename Container>
inline bool operator!=(const normal_iterator<Iterator, Container> &lhs,
	const normal_iterator<Iterator, Container> &rhs) noexcept {
	return lhs.current() != rhs.current();
}

template <typename IteratorL, typename IteratorR, typename Container>
inline bool operator<(const normal_iterator<IteratorL, Container> &lhs,
	const normal_iterator<IteratorR, Container> &rhs) noexcept {
	return lhs.current() < rhs.current();
}

template <typename Iterator, typename Container>
inline bool operator<(const normal_iterator<Iterator, Container> &lhs,
	const normal_iterator<Iterator, Container> &rhs) noexcept {
	return lhs.current() < rhs.current();
}

template <typename IteratorL, typename IteratorR, typename Container>
inline bool operator>(const normal_iterator<IteratorL, Container> &lhs,
	const normal_iterator<IteratorR, Container> &rhs) noexcept {
	return lhs.current() > rhs.current();
}

template <typename Iterator, typename Container>
inline bool operator>(const normal_iterator<Iterator, Container> &lhs,
	const normal_iterator<Iterator, Container> &rhs) noexcept {
	return lhs.current() > rhs.current();
}

template <typename IteratorL, typename IteratorR, typename Container>
inline bool operator<=(const normal_iterator<IteratorL, Container> &lhs,
	const normal_iterator<IteratorR, Container> &rhs) noexcept {
	return lhs.current() <= rhs.current();
}

template <typename Iterator, typename Container>
inline bool operator<=(const normal_iterator<Iterator, Container> &lhs,
	const normal_iterator<Iterator, Container> &rhs) noexcept {
	return lhs.current() <= rhs.current();
}

template <typename IteratorL, typename IteratorR, typename Container>
inline bool operator>=(const normal_iterator<IteratorL, Container> &lhs,
	const normal_iterator<IteratorR, Container> &rhs) noexcept {
	return lhs.current() >= rhs.current();
}

template <typename Iterator, typename Container>
inline bool operator>=(const normal_iterator<Iterator, Container> &lhs,
	const normal_iterator<Iterator, Container> &rhs) noexcept {
	return lhs.current() >= rhs.current();
}

template <typename IteratorL, typename IteratorR, typename Container>
inline auto operator-(const normal_iterator<IteratorL, Container> &lhs,
	const normal_iterator<IteratorR, Container> &rhs) noexcept
	-> decltype(lhs.current() - rhs.current()) {
	return lhs.current() - rhs.current();
}

template <typename Iterator, typename Container>
inline typename normal_iterator<Iterator, Container>::difference_type operator-(
	const normal_iterator<Iterator, Container> &lhs,
	const normal_iterator<Iterator, Container> &rhs) noexcept {
	return lhs.current() - rhs.current();
}

template <typename Iterator, typename Container>
inline normal_iterator<Iterator, Container> operator+(
	typename normal_iterator<Iterator, Container>::difference_type n,
	const normal_iterator<Iterator, Container> &i) noexcept {
	return normal_iterator<Iterator, Container>(i.current() + n);
}

}  // detail

/*! \brief Compact vector
 *
 * This is drop in replacement for std::vector class. This class should be used when
 * we just need to store data. Any operation resulting in change of vector size will be
 * much slower than in std::vector. This is because compact vector never reserves extra memory.
 * Always allocated space is equal to vector size.
 *
 * If the Size type is void then pointer to data and vector size are kept in 8 bytes
 * (on 64 bit system).
 */
template <typename T, typename Size = void, typename Alloc = std::allocator<T>>
class compact_vector : protected detail::compact_vector_base<T, Size, Alloc> {
	typedef detail::compact_vector_base<T, Size, Alloc> base;
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
	typedef std::allocator_traits<typename base::allocator_type> alloc_traits;
#else
	typedef typename base::allocator_type alloc_traits;
#endif

public:
	typedef T                                                      value_type;
	typedef typename base::pointer                                 pointer;
	typedef typename alloc_traits::const_pointer                   const_pointer;
	typedef T&                                                     reference;
	typedef const T&                                               const_reference;
	typedef detail::normal_iterator<pointer, compact_vector>       iterator;
	typedef detail::normal_iterator<const_pointer, compact_vector> const_iterator;
	typedef std::reverse_iterator<const_iterator>                  const_reverse_iterator;
	typedef std::reverse_iterator<iterator>                        reverse_iterator;
	typedef typename base::size_type                               size_type;
	typedef std::ptrdiff_t                                         difference_type;
	typedef Alloc                                                  allocator_type;

public:
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
	compact_vector() noexcept(std::is_nothrow_default_constructible<allocator_type>::value)
	    : base() {
	}
#else
	compact_vector() noexcept(std::is_nothrow_constructible<allocator_type>::value)
	    : base() {
	}
#endif

	~compact_vector() {
		destroy(base::get_ptr(), base::get_ptr() + base::get_size());
	}

	explicit compact_vector(size_type n) : base(n) {
		uninitialized_init(begin(), end());
	}

	compact_vector(size_type n, const value_type &value) : base(n) {
		std::uninitialized_fill(begin(), end(), value);
	}

	compact_vector(const compact_vector &x) : base(x.size()) {
		std::uninitialized_copy(x.begin(), x.end(), begin());
	}

	compact_vector(compact_vector &&x) noexcept : base(std::move(x)) {
	}

	compact_vector(std::initializer_list<value_type> list)
	    : compact_vector(list.begin(), list.end()) {
	}

	template <typename InputIterator, typename =
	          typename std::enable_if< std::is_convertible<
	          typename std::iterator_traits<InputIterator>::iterator_category,
	          std::input_iterator_tag>::value>::type>
	compact_vector(InputIterator first, InputIterator last)
	    : base(std::distance(first, last)) {
		std::uninitialized_copy(first, last, begin());
	}

	compact_vector &operator=(const compact_vector &x) {
		resize(x.size());
		std::copy(x.begin(), x.end(), begin());
		return *this;
	}

	compact_vector &operator=(compact_vector &&x) {
		resize(0);
		base::storage_.swap(x.storage_);
		return *this;
	}

	compact_vector &operator=(std::initializer_list<value_type> list) {
		resize(list.size());
		std::copy(list.begin(), list.end(), begin());
		return *this;
	}

	void assign(size_type n, const value_type &val) {
		resize(n);
		std::fill(begin(), end(), val);
	}

	template <typename InputIterator,
	          typename = typename std::enable_if<std::is_convertible<
	                  typename std::iterator_traits<InputIterator>::iterator_category,
	                  std::input_iterator_tag>::value>::type>
	void assign(InputIterator first, InputIterator last) {
		resize(std::distance(first, last));
		std::copy(first, last, begin());
	}

	void assign(std::initializer_list<value_type> list) {
		assign(list.begin(), list.end());
	}

	iterator begin() noexcept {
		return iterator(base::get_ptr());
	}

	const_iterator begin() const noexcept {
		return const_iterator(base::get_ptr());
	}

	const_iterator cbegin() const noexcept {
		return const_iterator(base::get_ptr());
	}

	iterator end() noexcept {
		return iterator(base::get_ptr() + size());
	}

	const_iterator end() const noexcept {
		return const_iterator(base::get_ptr() + size());
	}

	const_iterator cend() const noexcept {
		return const_iterator(base::get_ptr() + size());
	}

	reverse_iterator rbegin() noexcept {
		return reverse_iterator(end());
	}

	const_reverse_iterator rbegin() const noexcept {
		return const_reverse_iterator(end());
	}

	reverse_iterator rend() noexcept {
		return reverse_iterator(begin());
	}

	const_reverse_iterator rend() const noexcept {
		return const_reverse_iterator(begin());
	}

	const_reverse_iterator crbegin() const noexcept {
		return const_reverse_iterator(end());
	}

	const_reverse_iterator crend() const noexcept {
		return const_reverse_iterator(begin());
	}

	size_type size() const noexcept {
		return base::get_size();
	}

	size_type max_size() const noexcept {
		return base::storage_.max_size();
	}

	void resize(size_type nsize) {
		if (nsize == size()) {
			return;
		}

		if (nsize == 0) {
			set_new_ptr(nullptr, 0);
			return;
		}

		pointer old_ptr = base::get_ptr();
		pointer ptr     = base::allocate(nsize);

		auto dr = std::make_pair(ptr, ptr);

		try {
			if (nsize > size()) {
				uninitialized_init(ptr + size(), ptr + nsize);
			}
			if (ptr != old_ptr) {
				dr = std::make_pair(ptr + size(), ptr + nsize);
				uninitialized_move_if_no_except(
				        old_ptr, old_ptr + std::min(size(), nsize), ptr);
			}
		} catch (...) {
			destroy(dr.first, dr.second);
			base::deallocate(ptr, nsize);
			base::set_ptr(old_ptr);
			throw;
		}

		set_new_ptr(old_ptr, ptr, nsize);
	}

	void resize(size_type nsize, const value_type &x) {
		if (nsize == size()) {
			return;
		}

		if (nsize == 0) {
			set_new_ptr(nullptr, 0);
			return;
		}

		pointer old_ptr = base::get_ptr();
		pointer ptr     = base::allocate(nsize);

		if (nsize > 0) {
			auto dr = std::make_pair(ptr, ptr);

			try {
				if (nsize > size()) {
					std::uninitialized_fill(ptr + size(), ptr + nsize, x);
				}
				if (ptr != old_ptr) {
					dr = std::make_pair(ptr + size(), ptr + nsize);
					uninitialized_move_if_no_except(
					        old_ptr, old_ptr + std::min(size(), nsize), ptr);
				}
			} catch (...) {
				destroy(dr.first, dr.second);
				base::deallocate(ptr, nsize);
				base::set_ptr(old_ptr);
				throw;
			}
		}

		set_new_ptr(old_ptr, ptr, nsize);
	}

	void shrink_to_fit() {
	}

	size_type capacity() const noexcept {
		return size();
	}

	bool empty() const noexcept {
		return base::get_size() == 0;
	}

	bool full() const noexcept {
		return size() == base::storage_.max_size();
	}

	void reserve(size_type /*n*/) {
	}

	reference operator[](size_type n) noexcept {
		assert(n < size());
		return *(begin() + n);
	}

	const_reference operator[](size_type n) const noexcept {
		assert(n < size());
		return *(begin() + n);
	}

	reference at(size_type n) {
		if (n >= size()) {
			throw std::out_of_range("compact_vector: out of range");
		}
		return (*this)[n];
	}

	const_reference at(size_type n) const {
		if (n >= size()) {
			throw std::out_of_range("compact_vector: out of range");
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
		return *(begin() + size() - 1);
	}

	const_reference back() const noexcept {
		return *(begin() + size() - 1);
	}

	pointer data() noexcept {
		return base::get_ptr();
	}

	const_pointer data() const noexcept {
		return base::get_ptr();
	}

	void push_back(const value_type &x) {
		resize(size() + 1, x);
	}

	void push_back(value_type &&x) {
		emplace_back(std::move(x));
	}

	template <typename... Args>
	void emplace_back(Args &&...args) {
		size_type nsize = size() + 1;
		pointer old_ptr = base::get_ptr();
		pointer ptr     = base::allocate(nsize);
		auto dr = std::make_pair(ptr, ptr);

		try {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
			alloc_traits::construct(base::get_allocator(), ptr + size(), std::forward<Args>(args)...);
#else
			base::get_allocator().construct(ptr + size(), std::forward<Args>(args)...);
#endif
			if (ptr != old_ptr) {
				dr = std::make_pair(ptr + size(), ptr + size() + 1);
				uninitialized_move_if_no_except(old_ptr, old_ptr + size(), ptr);
			}
		} catch (...) {
			destroy(dr.first, dr.second);
			base::deallocate(ptr, nsize);
			base::set_ptr(old_ptr);
			throw;
		}

		set_new_ptr(old_ptr, ptr, nsize);
	}

	void pop_back() noexcept {
		assert(size() > 0);
		resize(size() - 1);
	}

	template <typename... Args>
	iterator emplace(const_iterator position, Args &&... args) {
		size_type nsize = size() + 1;
		pointer old_ptr = base::get_ptr();
		pointer ptr = base::allocate(nsize);
		auto dr = std::make_pair(ptr, ptr);
		auto pos = position - cbegin();

		if (ptr == old_ptr) {
			return inplace_emplace(drop_const(position), std::forward<Args>(args)...);
		}

		try {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
			alloc_traits::construct(base::get_allocator(), ptr + pos,
			                        std::forward<Args>(args)...);
#else
			base::get_allocator().construct(ptr + pos, std::forward<Args>(args)...);
#endif
			dr = std::make_pair(ptr + pos, ptr + pos + 1);
			uninitialized_move_if_no_except(iterator(old_ptr), drop_const(position), ptr);
			dr = std::make_pair(ptr, ptr + pos + 1);
			uninitialized_move_if_no_except(drop_const(position),
			                                iterator(old_ptr) + size(), ptr + pos + 1);
		} catch (...) {
			destroy(dr.first, dr.second);
			base::deallocate(ptr, nsize);
			base::set_ptr(old_ptr);
			throw;
		}

		pointer new_iter = ptr + pos;

		set_new_ptr(old_ptr, ptr, nsize);

		return iterator(new_iter);
	}

	iterator insert(const_iterator position, const value_type &x) {
		return insert(position, 1, x);
	}

	iterator insert(const_iterator position, value_type &&x) {
		return emplace(position, std::move(x));
	}

	iterator insert(const_iterator position, std::initializer_list<value_type> l) {
		return insert(position, l.begin(), l.end());
	}

	iterator insert(const_iterator position, size_type n, const value_type &x) {
		if (n <= 0) {
			return drop_const(position);
		}

		size_type nsize = size() + n;
		pointer old_ptr = base::get_ptr();
		pointer ptr = base::allocate(nsize);
		auto dr = std::make_pair(ptr, ptr);
		auto pos = position - cbegin();

		if (ptr == old_ptr) {
			return inplace_insert(drop_const(position), n, x);
		}

		try {
			std::uninitialized_fill(ptr + pos, ptr + pos + n, x);
			dr = std::make_pair(ptr + pos, ptr + pos + n);
			uninitialized_move_if_no_except(iterator(old_ptr), drop_const(position),
			                                ptr);
			dr = std::make_pair(ptr, ptr + pos + n);
			uninitialized_move_if_no_except(drop_const(position),
			                                iterator(old_ptr) + size(), ptr + pos + n);
		} catch (...) {
			destroy(dr.first, dr.second);
			base::deallocate(ptr, nsize);
			base::set_ptr(old_ptr);
			throw;
		}

		pointer new_iter = ptr + pos;

		set_new_ptr(old_ptr, ptr, nsize);

		return iterator(new_iter);
	}

	template <typename InputIterator,
	          typename = typename std::enable_if<std::is_convertible<
	                  typename std::iterator_traits<InputIterator>::iterator_category,
	                  std::input_iterator_tag>::value>::type>
	iterator insert(const_iterator position, InputIterator first, InputIterator last) {
		size_type gap_size = std::distance(first, last);

		if (gap_size == 0) {
			return drop_const(position);
		}

		size_type nsize = size() + gap_size;
		pointer old_ptr = base::get_ptr();
		pointer ptr = base::allocate(nsize);
		auto dr = std::make_pair(ptr, ptr);
		auto pos = position - cbegin();

		if (ptr == old_ptr) {
			return inplace_insert(gap_size, drop_const(position), first, last);
		}

		try {
			std::uninitialized_copy(first, last, ptr + pos);
			dr = std::make_pair(ptr + pos, ptr + pos + gap_size);
			uninitialized_move_if_no_except(iterator(old_ptr), drop_const(position),
			                                ptr);
			dr = std::make_pair(ptr, ptr + pos + gap_size);
			uninitialized_move_if_no_except(drop_const(position),
			                                iterator(old_ptr) + size(),
			                                ptr + pos + gap_size);
		} catch (...) {
			destroy(dr.first, dr.second);
			base::deallocate(ptr, nsize);
			base::set_ptr(old_ptr);
			throw;
		}

		pointer new_iter = ptr + pos;

		set_new_ptr(old_ptr, ptr, nsize);

		return iterator(new_iter);
	}

	iterator erase(const_iterator position) {
		return erase(position, position + 1);
	}

	iterator erase(const_iterator first, const_iterator last) {
		assert(first >= begin() && last <= end() && last >= first);

		size_type nsize = size() - std::distance(first, last);
		if (nsize == size()) {
			return drop_const(first);
		}

		if (nsize == 0) {
			resize(0);
			return end();
		}

		pointer  old_ptr = base::get_ptr();
		pointer  ptr = base::allocate(nsize);
		pointer  ptr_last = ptr;
		iterator new_iter;

		if (ptr != old_ptr) {
			try {
				ptr_last = uninitialized_move_if_no_except(iterator(old_ptr),
				                                           drop_const(first), ptr);
				new_iter = iterator(ptr_last);
				ptr_last = uninitialized_move_if_no_except(
				        drop_const(last), iterator(old_ptr) + size(),
				        ptr_last);
			} catch (...) {
				destroy(ptr, ptr_last);
				base::deallocate(ptr, nsize);
				base::set_ptr(old_ptr);
				throw;
			}
		} else {
			new_iter = drop_const(first);
			std::move(drop_const(last), end(), new_iter);
		}

		set_new_ptr(old_ptr, ptr, nsize);

		return new_iter;
	}

	void swap(compact_vector &x) noexcept {
		base::storage_.swap(x.storage_);
	}

	void clear() noexcept {
		resize(0);
	}

private:
	void destroy(pointer s, pointer e) {
		for (; s != e; ++s) {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
			alloc_traits::destroy(base::get_allocator(), s);
#else
			base::get_allocator().destroy(s);
#endif
		}
	}

	void set_new_ptr(pointer old_ptr, pointer ptr, size_type nsize) {
		if (ptr != old_ptr) {
			destroy(old_ptr, old_ptr + base::get_size());
			base::deallocate(old_ptr, base::get_size());
		} else {
			if (nsize < base::get_size()) {
				destroy(old_ptr + nsize, old_ptr + base::get_size());
			}
		}

		base::set_storage(ptr, nsize);
	}

	void set_new_ptr(pointer ptr, size_type nsize) {
		set_new_ptr(base::get_ptr(), ptr, nsize);
	}

	template <typename InputIterator, typename ForwardIterator>
	ForwardIterator uninitialized_move_if_no_except(InputIterator first, InputIterator last,
	                                                ForwardIterator result) {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
		typedef typename std::conditional<std::is_nothrow_move_constructible<T>::value,
		                                  std::move_iterator<InputIterator>,
		                                  InputIterator>::type iterator_type;
#else
		typedef InputIterator iterator_type;
#endif

		return std::uninitialized_copy(iterator_type(first), iterator_type(last), result);
	}

	template <typename ForwardIterator>
	void uninitialized_init(ForwardIterator first, ForwardIterator last) {
		typedef typename std::iterator_traits<ForwardIterator>::value_type Value;

		ForwardIterator current = first;
		try {
			for (; current != last; ++current) {
				::new (static_cast<void *>(std::addressof(*current))) Value();
			}
		} catch (...) {
			for (; first != current; ++first) {
				first->~Value();
			}
			throw;
		}
	}

	iterator drop_const(const const_iterator &i) const {
		return iterator(const_cast<pointer>(i.current()));
	}

	template <typename InputIterator>
	iterator inplace_insert(size_type gap_size, iterator position, InputIterator first,
	                        InputIterator last) {
		auto dr = std::make_pair(base::get_ptr(), base::get_ptr());

		try {
			// size of new data to be copied into vector
			difference_type nd_n = gap_size;

			// size of nd that goes to uninitialized memory region
			difference_type nd_n2 =
			        std::min(position + gap_size - end(), (difference_type)0);

			// size of nd that goes to initialized memory
			difference_type nd_n1 = nd_n - nd_n2;

			// size of old data that needs to be moved
			difference_type md_n = end() - position;

			// size of md part that needs to be moved to uninitialized memory region
			difference_type md_n2 = std::min(md_n, nd_n);

			// size of md part that goes to initialized memory
			difference_type md_n1 = md_n - md_n2;

			// copy of new data to uninitialized memory
			std::uninitialized_copy(first + nd_n1, last, end());
			dr = std::make_pair(end().current(), end().current() + nd_n2);

			// move old data to uninitialized region
			uninitialized_move_if_no_except(position + md_n1, end(),
			                                position + (nd_n + md_n1));
			dr = std::make_pair(end().current(), end().current() + nd_n);

			// move of old data to initialized memory region
			std::move_backward(position, position + md_n1, position + nd_n);

			// copy of new data to initialized memory range
			std::copy(first, first + nd_n1, position);
		} catch (...) {
			destroy(dr.first, dr.second);
			throw;
		}

		set_new_ptr(base::get_ptr(), base::get_size() + gap_size);

		return position;
	}

	iterator inplace_insert(iterator position, size_type n, const value_type &x) {
		auto dr = std::make_pair(base::get_ptr(), base::get_ptr());

		try {
			difference_type nd_n = n;
			difference_type nd_n2 = std::min(position + n - end(), (difference_type)0);
			difference_type nd_n1 = nd_n - nd_n2;
			difference_type md_n = end() - position;
			difference_type md_n2 = std::min(md_n, nd_n);
			difference_type md_n1 = md_n - md_n2;

			// fill part of the uninitialized memory
			std::uninitialized_fill(end(), end() + nd_n2, x);
			dr = std::make_pair(end().current(), end().current() + nd_n2);

			// move old data to uninitialized region
			uninitialized_move_if_no_except(position + md_n1, end(),
			                                position + (nd_n + md_n1));
			dr = std::make_pair(end().current(), end().current() + nd_n);

			// move old data to initialized memory region
			std::move_backward(position, position + md_n1, position + nd_n);

			// fill initialized memory range
			std::fill(position, position + nd_n1, x);
		} catch (...) {
			destroy(dr.first, dr.second);
			throw;
		}

		set_new_ptr(base::get_ptr(), base::get_size() + n);

		return position;
	}

	template <typename... Args>
	iterator inplace_emplace(iterator position, Args &&... args) {
		auto dr = std::make_pair(base::get_ptr(), base::get_ptr());

		try {
			difference_type nd_n = 1;
			difference_type nd_n2 = std::min(position + 1 - end(), (difference_type)0);
			difference_type nd_n1 = nd_n - nd_n2;
			difference_type md_n = end() - position;
			difference_type md_n2 = std::min(md_n, nd_n);
			difference_type md_n1 = md_n - md_n2;

			//  the uninitialized memory
			if (nd_n2 > 0) {
#ifdef LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS
				alloc_traits::construct(base::get_allocator(), end().current(),
				                        std::forward<Args>(args)...);
#else
				base::get_allocator().construct(end().current(),
				                                std::forward<Args>(args)...);
#endif
				dr = std::make_pair(end().current(), end().current() + nd_n2);
			}

			// move old data to uninitialized region
			uninitialized_move_if_no_except(position + md_n1, end(),
			                                position + (nd_n + md_n1));
			dr = std::make_pair(end().current(), end().current() + nd_n);

			// move old data to initialized memory region
			std::move_backward(position, position + md_n1, position + nd_n);

			// fill initialized memory range
			if (nd_n1 > 0) {
				*position = value_type(std::forward<Args>(args)...);
			}
		} catch (...) {
			destroy(dr.first, dr.second);
			throw;
		}

		set_new_ptr(base::get_ptr(), base::get_size() + 1);

		return position;
	}
};

template <typename Tp, typename Alloc>
inline bool operator==(const compact_vector<Tp, Alloc> &x, const compact_vector<Tp, Alloc> &y) {
	return x.size() == y.size() && std::equal(x.begin(), x.end(), y.begin());
}

template <typename Tp, typename Alloc>
inline bool operator<(const compact_vector<Tp, Alloc> &x, const compact_vector<Tp, Alloc> &y) {
	return std::lexicographical_compare(x.begin(), x.end(), y.begin(), y.end());
}

template <typename Tp, typename Alloc>
inline bool operator!=(const compact_vector<Tp, Alloc> &x, const compact_vector<Tp, Alloc> &y) {
	return !(x == y);
}

template <typename Tp, typename Alloc>
inline bool operator>(const compact_vector<Tp, Alloc> &x, const compact_vector<Tp, Alloc> &y) {
	return y < x;
}

template <typename Tp, typename Alloc>
inline bool operator<=(const compact_vector<Tp, Alloc> &x, const compact_vector<Tp, Alloc> &y) {
	return !(y < x);
}

template <typename Tp, typename Alloc>
inline bool operator>=(const compact_vector<Tp, Alloc> &x, const compact_vector<Tp, Alloc> &y) {
	return !(x < y);
}

template <typename Tp, typename Alloc>
inline void swap(compact_vector<Tp, Alloc> &x, compact_vector<Tp, Alloc> &y) {
	x.swap(y);
}
