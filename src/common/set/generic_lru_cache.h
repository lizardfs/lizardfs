/*
   Copyright 2017 Skytechnology sp. z o.o..

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
#include <utility>

/*! \brief Cache container designed to keep large key structures only once
 *
 * Design:
 *  - data is kept in an LRU-style list of <key, value>. Each time an element
 *    is accessed, it is moved to the end of the queue
 *  - data is indexed by unordered map, which keeps a reference to key
 *    and an iterator to list entry
 */
template <typename Key, typename Value, std::size_t DefaultCapacity = 0x10000,
		typename Hasher = std::hash<Key>, typename Comparator = std::equal_to<Key>>
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

	std::pair<iterator, bool> insert(const Key &key, const Value &value) {
		if (cache_.size() >= capacity_) {
			cache_.erase(queue_.back().first);
			queue_.pop_back();
		}
		auto it = cache_.find(key);
		if (it != cache_.end()) {
			return std::make_pair(it->second, false);
		}

		queue_.emplace_front(key, value);
		try {
			cache_.insert(typename CacheMap::value_type(queue_.front().first, queue_.begin()));
		} catch (...) {
			queue_.pop_front();
			throw;
		}

		return std::make_pair(queue_.begin(), true);
	}

	std::pair<iterator, bool> insert(Key &&key, Value &&value) {
		if (cache_.size() >= capacity_) {
			cache_.erase(queue_.back().first);
			queue_.pop_back();
		}
		auto it = cache_.find(key);
		if (it != cache_.end()) {
			return std::make_pair(it->second, false);
		}

		queue_.emplace_front(std::move(key), std::move(value));
		try {
			cache_.insert(typename CacheMap::value_type(queue_.front().first, queue_.begin()));
		} catch (...) {
			queue_.pop_front();
			throw;
		}

		return std::make_pair(queue_.begin(), true);
	}

	template<class... Args>
	std::pair<iterator, bool> emplace(Args&&... args) {
		if (cache_.size() >= capacity_) {
			cache_.erase(queue_.back().first);
			queue_.pop_back();
		}

		queue_.emplace_front(std::forward<Args>(args)...);

		auto it = cache_.find(queue_.front().first);
		if (it != cache_.end()) {
			queue_.pop_front();
			return std::make_pair(it->second, false);
		}

		try {
			cache_.insert(typename CacheMap::value_type(queue_.front().first, queue_.begin()));
		} catch (...) {
			queue_.pop_front();
			throw;
		}

		return std::make_pair(queue_.begin(), true);
	}

	void clear() {
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
		auto ret = std::find_if(queue_.begin(), queue_.end(),
			[&value](const std::pair<Key, Value> &e){return e.second == value;});

		if (ret != queue_.end() && ret != queue_.begin()) {
			queue_.splice(queue_.begin(), queue_, ret);
		}
		return ret;
	}

	iterator begin() {
		return queue_.begin();
	}

	iterator end() {
		return queue_.end();
	}

	const_iterator begin() const {
		return queue_.begin();
	}

	const_iterator end() const {
		return queue_.end();
	}

	const_iterator cbegin() const noexcept {
		return queue_.cbegin();
	}

	const_iterator cend() const noexcept {
		return queue_.cend();
	}

private:
	Queue queue_;
	CacheMap cache_;
	size_t capacity_;
};
