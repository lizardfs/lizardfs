/*
   Copyright 2016 Skytechnology sp. z o.o..

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "chunkserver/chunk.h"
#include "common/platform.h"

/*! Class for keeping resources that can be easily indexed with integers.
 *  Use case: Keeping open descriptors of chunks in order to close them
 *  not immediately, but when they are unused for a long period of time
 *  or desperately needed.
 *
 * Resources can be accessed either by their id or in LRU manner.
 * Thanks to that, it is possible to free least recently used resources
 * on demand while keeping the hot ones available.
 *
 */
template <typename Resource,
	int DefaultCapacity = 65536,
	int ReleaseThreshold_s = 4,
	int PopUnusedCount = 4>
class IndexedResourcePool {
public:
	static const int kNullId = 0;

	/*! Helper structure for keeping resources in double-linked list implemented
	 *  on top of a contiguous container.
	 */
	struct Entry {
		uint32_t timestamp;
		int next;
		int prev;
		Resource resource;

		Entry() : timestamp(), next(), prev(), resource() {}
	};

	IndexedResourcePool(int capacity = DefaultCapacity)
		: data_(capacity), mutex_(), garbage_collector_head_() {
	}

	/*!
	 * \brief Acquire existing resource.
	 *
	 * \param id Resource's index.
	 * \param data Resource passed as rvalue reference.
	 */
	void acquire(int id) {
		if (id < kNullId) {
			return;
		}
		std::lock_guard<std::mutex> guard(mutex_);
		assert((size_t)id < data_.size());
		erase(id);
	}

	/*!
	 * \brief Acquire created resource.
	 *
	 * \param id Resource's index.
	 * \param data Resource passed as rvalue reference.
	 */
	void acquire(int id, Resource &&data) {
		assert(id > kNullId);
		std::lock_guard<std::mutex> guard(mutex_);
		if ((size_t)id >= data_.size()) {
			data_.resize(id + 1);
		}
		erase(id);
		data_[id].resource = std::move(data);
	}

	/*!
	 * \brief Release resource and put it in the free list.
	 *
	 * \param id Resource's index.
	 * \param timestamp Timestamp of the moment of release.
	 */
	void release(int id, uint32_t timestamp) {
		if (id <= kNullId) {
			return;
		}
		std::lock_guard<std::mutex> guard(mutex_);
		assert((size_t)id < data_.size());
		erase(id);
		push_back(id);
		data_[id].timestamp = timestamp;
	}

	/*!
	 * \brief Immediately free resource.
	 *
	 * \param id Resource's index.
	 * \return True if id was correct.
	 */
	int purge(int id) {
		if (id <= kNullId) {
			return false;
		}
		Resource tmp;
		std::lock_guard<std::mutex> guard(mutex_);
		erase(id);
		tmp = std::move(data_[id].resource);
		tmp.purge();
		return true;
	}

	/*!
	 * \brief Free up to 'count' resources unused since 'now'.
	 * Resources which can be freed should return true from their implementation
	 * of test method. Freeing is done in resource's destructor.
	 *
	 * \param now Current timestamp.
	 * \param count Maximum number of resources to be freed.
	 * \return Number of elements freed.
	 */
	int freeUnused(uint32_t now,  int count = PopUnusedCount) {
		int freed = 0;
		small_vector<Resource, PopUnusedCount> candidates;
		candidates.reserve(count);
		mutex_.lock();
		garbage_collector_head_ = front();
		mutex_.unlock();
		while (true) {
			std::lock_guard<std::mutex> guard(mutex_);
			if (freed >= count || garbage_collector_head_ == kNullId) {
				break;
			}

			Entry &node = data_[garbage_collector_head_];
			if (node.timestamp + ReleaseThreshold_s > now) {
				break;
			}

			if (node.resource.canRemove()) {
				candidates.emplace_back(std::move(node.resource));
				erase(garbage_collector_head_);
				freed++;
			} else {
				garbage_collector_head_ = node.next;
			}
		}
		return freed;
	}

	/*!
	 * \brief Get resource indexed by given id by reference.
	 *
	 * \param id Resource's index.
	 * \return Reference to resource with given id. Undefined if id is not acquired.
	 */
	Resource &getResource(int id) {
		assert(id > kNullId);
		return data_[id].resource;
	};

protected:
	void erase(int id) {
		if (!contains(id)) {
			return;
		}
		int next_id = data_[id].next;
		data_[data_[id].next].prev = data_[id].prev;
		data_[data_[id].prev].next = data_[id].next;
		data_[id].prev = data_[id].next = kNullId;
		if (id == garbage_collector_head_) {
			garbage_collector_head_ = next_id;
		}
	}

	void push_back(int id) {
		data_[id].prev = back();
		data_[id].next = kNullId;
		data_[back()].next = id;
		data_[kNullId].prev = id;
	}

	inline bool contains(int id) {
		assert(id > kNullId);
		return data_[id].prev != kNullId || id == front();
	}

	inline int front() {
		return data_[kNullId].next;
	}

	inline int back() {
		return data_[kNullId].prev;
	}

	std::vector<Entry> data_;
	std::mutex mutex_;
	int garbage_collector_head_;
};
