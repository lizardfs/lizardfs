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

#include <array>
#include <cassert>

template<typename T, int Capacity>
class RingBuffer {
static const int N = Capacity + 1;
static_assert(Capacity >= 1, "RingBuffer capacity must be >= 1");
public:
	RingBuffer() : data_(), start_(), end_() {}

	int empty() const {
		return start_ == end_;
	}

	int full() const {
		return next(end_) == start_;
	}

	void push_back(const T &obj) {
		assert(!full());
		data_[end_] = obj;
		end_ = next(end_);
	}

	void push_back(T &&obj) {
		assert(!full());
		data_[end_] = std::move(obj);
		end_ = next(end_);
	}

	void pop_front() {
		assert(!empty());
		start_ = next(start_);
	}

	T &operator[](long pos) {
		assert(pos >= 0 && pos < size());
		return data_[advance(start_, pos)];
	}

	const T &operator[](long pos) const {
		assert(pos >= 0 && pos < size());
		return data_[advance(start_, pos)];
	}

	size_t size() const {
		return end_ >= start_ ? end_ - start_ : end_ + (N - start_);
	}

	size_t capacity() const {
		return N - 1;
	}

	T &front() {
		assert(!empty());
		return data_[start_];
	}

	T &back() {
		assert(!empty());
		return data_[prev(end_)];
	}

	const T &front() const {
		assert(!empty());
		return data_[start_];
	}

	const T &back() const {
		assert(!empty());
		return data_[prev(end_)];
	}

private:
	static inline int next(int i) {
		return advance(i, 1);
	}

	static inline int prev(int i) {
		return advance(i, N - 1);
	}

	static inline int advance(int i, int distance) {
		assert(distance >= 0);
		return i + distance >= N ? i + distance - N : i + distance;
	}

	std::array<T, N> data_;
	int start_;
	int end_;
};
