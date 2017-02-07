/*
   Copyright 2015 Skytechnology sp. z o.o..

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

#include "common/platform.h"

#include <algorithm>
#include <functional>
#include <list>
#include <unordered_map>

/*! \brief Cache container designed to keep large key structures only once
 *
 * Design:
 *  - data is kept in an LRU-style list of <key, value>. Each time an element
 *    is accessed, it is moved to the end of the queue
 *  - data is indexed by unordered map, which keeps a reference to key
 *    and an iterator to list entry
 */
template <typename Key, typename Value, typename Hasher = std::hash<Key>,
		typename Comparator = std::equal_to<Key>, std::size_t DefaultCapacity = 0x10000>
class GenericLruCache {
public:
	typedef std::list<std::pair<Key, Value>> Queue;
	typedef std::unordered_map<std::reference_wrapper<const Key>, typename Queue::iterator, Hasher,
	                           Comparator> CacheMap;
	typedef typename Queue::iterator iterator;
	typedef typename Queue::const_iterator const_iterator;

	GenericLruCache() : capacity_(DefaultCapacity) {
	}

	explicit GenericLruCache(size_t capacity) : capacity_(capacity) {
	}

	void put(const Key &key, const Value &value) {
		if (cache_.size() >= capacity_) {
			cache_.erase(queue_.back().first);
			queue_.pop_back();
		}
		queue_.emplace_front(key, value);
		try {
			cache_[queue_.front().first] = queue_.begin();
		} catch (...) {
			queue_.pop_front();
			throw;
		}
	}

	void put(Key &&key, Value &&value) {
		if (cache_.size() >= capacity_) {
			cache_.erase(queue_.back().first);
			queue_.pop_back();
		}
		queue_.emplace_front(std::move(key), std::move(value));
		try {
			cache_[queue_.front().first] = queue_.begin();
		} catch (...) {
			queue_.pop_front();
			throw;
		}
	}

	void invalidate() {
		cache_.clear();
		queue_.clear();
	}

	size_t size() const {
		return cache_.size();
	}

	iterator find(const Key &key) {
		auto entry = cache_.find(key);
		if (entry != cache_.end()) {
			auto ret = entry->second;
			// Move the found element to the front of lru queue
			if (ret != queue_.begin()) {
				queue_.splice(queue_.begin(), queue_, ret);
			}
			return ret;
		}
		return queue_.end();
	}

	iterator findByValue(const Value &value) {
		return std::find_if(queue_.begin(), queue_.end(),
			[&value](const std::pair<Key, Value> &e){return e.second == value;});
	}

	const_iterator end() const {
		return queue_.end();
	}

private:
	Queue queue_;
	CacheMap cache_;
	size_t capacity_;
};
