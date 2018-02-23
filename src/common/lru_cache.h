/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <tuple>
#include <type_traits>
#include <unordered_map>

#include "common/hashfn.h"
#include "common/massert.h"
#include "common/time_utils.h"

/**
 * Options for LruCache.
 */
struct LruCacheOption {
	// Map type used by the cache
	typedef std::true_type  UseHashMap;
	typedef std::false_type UseTreeMap;

	// Reentrant map will use a mutex internally to work with multiple threads
	struct Reentrant {
		typedef std::mutex Mutex;
	};

	// Not-reentrant map doesn't use any locking
	struct NotReentrant {
		struct Mutex {
			void lock() {}
			void unlock() {}
		};
	};
};

/**
 * A map caching 'Values' for 'Keys' in LRU manner.
 *
 * 'KeysTupleToTimeAndValue' is an internal map type used by the cache. For details have a look
 * at example usages. If possible use 'LruCache' typedef instead.
 */
template <class HashMapType, class ReentrancyType, class Value, class... Keys>
class LruCache {
public:
	typedef std::function<Value(Keys...)> ValueObtainer;

	/**
	 * \param maxTime period after which every cache entry is discarded
	 * \param maxElements capacity of the cache, after exceeding which values added least recently
	 *                are removed
	 * \param valueObtainer a function that is used for obtaining a value for a given key, if
	 *                   cached value was not available.
	 */
	LruCache(SteadyDuration maxTime, uint64_t maxElements, ValueObtainer valueObtainer = ValueObtainer())
		: cacheHit(0),
		  cacheExpired(0),
		  cacheMiss(0),
		  maxTime_ms(std::chrono::duration_cast<std::chrono::milliseconds>(maxTime).count()),
		  maxTime_(maxTime),
		  maxElements_(maxElements),
		  valueObtainer_(valueObtainer) {
	}

	/**
	 * If available return value from cache. Otherwise obtain it with use of 'valueObtainer',
	 * fill the cache and return the value. If new cache entry was added, try to cleanup the whole
	 * cache a bit by removing few outdated entries if there were any, or remove the oldest entry
	 * if the capacity was exceeded.
	 */
	template<typename CustomValueObtainer>
	Value get(SteadyTimePoint currentTs, Keys... keys, CustomValueObtainer value_obtainer) {
		std::unique_lock<Mutex> lock(mutex_);
		uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(maxTime_).count();
		if (maxTime_ms != ms) {
			maxTime_ = std::chrono::milliseconds(maxTime_ms);
		}
		auto keyTuple = std::make_tuple(keys...);
		auto iterator = keysToTimeAndValue_.find(keyTuple);
		if (iterator != keysToTimeAndValue_.end()) {
			auto keyTuplePointer = &iterator->first;
			auto& ts = std::get<0>(iterator->second);
			if (ts + maxTime_ >= currentTs) {
				++cacheHit;
				auto& value = std::get<1>(iterator->second);
				return value;
			} else {
				++cacheExpired;
				++cacheMiss;
				auto tsAndKeys = std::make_pair(ts, keyTuplePointer);
				sassert(timeToKeys_.erase(tsAndKeys) == 1);
				keysToTimeAndValue_.erase(keyTuple);
			}
		} else {
			++cacheMiss;
		}

		// Don't call valueObtainer under a lock
		lock.unlock();
		auto value = value_obtainer(keys...);
		lock.lock();
		// If there was a race and the cache was filled after the lock was released, return the
		// value that was just obtained, don't update the cache itself.
		iterator = keysToTimeAndValue_.find(keyTuple);
		if (iterator != keysToTimeAndValue_.end()) {
			return value;
		}

		try {
			auto tsAndValue = std::make_pair(currentTs, value);
			keysToTimeAndValue_.insert(std::make_pair(keyTuple, std::move(tsAndValue)));
			auto keyToTimeAndValue = keysToTimeAndValue_.find(keyTuple);
			auto keysTuplePointer = &keyToTimeAndValue->first;
			auto tsAndKeys = std::make_pair(currentTs, keysTuplePointer);
			timeToKeys_.insert(tsAndKeys);
		} catch (...) {
			keysToTimeAndValue_.erase(keyTuple);
			throw;
		}

		// If one value was (possibly) added, remove a few values, to keep
		// number of elements in the cache limited
		uint64_t few = 3;
		this->cleanupWithoutLocking(currentTs, few);
		return value;
	}

	Value get(SteadyTimePoint currentTs, Keys... keys) {
		assert(valueObtainer_);
		return get(currentTs, keys..., valueObtainer_);
	}


	/**
	 * Remove 'maxOperations' or less entries that are either outdated or make the cache exceed
	 * its capacity.
	 *
	 * Due to the way 'get' method was implemented the cache should never exceed its limit by more
	 * then 1, so this function does not have to be called by hand.
	 */
	void cleanup(SteadyTimePoint currentTs, uint32_t maxOperations) {
		std::unique_lock<Mutex> lock(mutex_);
		cleanupWithoutLocking(currentTs, maxOperations);
	}

	/**
	 * Erase a cache entry, if there was one matching the key. Returns the number of erased
	 * entries;
	 */
	uint32_t erase(Keys... keys) {
		std::unique_lock<Mutex> lock(mutex_);
		auto keyTuple = std::make_tuple(keys...);
		auto iterator = keysToTimeAndValue_.find(keyTuple);
		return eraseWithoutLocking(iterator);
	}

	/**
	 * Erases cache entries from [lowerBound, upperBound) range. Returns the number of erased
	 * entries;
	 */
	uint32_t erase(Keys... lowerBound, Keys... upperBound) {
		std::unique_lock<Mutex> lock(mutex_);

		auto lowerKeys = std::make_tuple(lowerBound...);
		auto lowerIt = keysToTimeAndValue_.lower_bound(lowerKeys);

		auto upperKeys = std::make_tuple(upperBound...);
		auto upperIt = keysToTimeAndValue_.lower_bound(upperKeys);

		auto removed = 0;
		for (auto it = lowerIt; it != upperIt;) {
			removed += eraseWithoutLocking(it++);
		}
		return removed;
	}

	/**
	 * Removes all elements from cache.
	 */
	void clear() {
		std::unique_lock<Mutex> lock(mutex_);
		timeToKeys_.clear();
		keysToTimeAndValue_.clear();
	}

	std::atomic<uint64_t> cacheHit;
	std::atomic<uint64_t> cacheExpired;
	std::atomic<uint64_t> cacheMiss;
	std::atomic<uint64_t> maxTime_ms;

private:
	typedef std::tuple<Keys...> KeysTuple;
	typedef std::pair<SteadyTimePoint, Value> TimeAndValue;
	typedef AlmostGenericTupleHash<Keys...> Hasher;
	typedef std::pair<SteadyTimePoint, const KeysTuple*> TimeAndKeysPtr;

	typedef typename std::conditional<
				HashMapType::value,
				std::unordered_map<KeysTuple, TimeAndValue, Hasher>,
				std::map<KeysTuple, TimeAndValue>
			>::type KeysTupleToTimeAndValue;

	typedef typename ReentrancyType::Mutex Mutex;

	/**
	 * Given an iterator erases a cache entry without acquiring a lock. Return 1 if some element
	 * was removed and 0 otherwise.
	 */
	uint32_t eraseWithoutLocking(typename KeysTupleToTimeAndValue::iterator iterator) {
		if (iterator != keysToTimeAndValue_.end()) {
			auto keyTuplePointer = &iterator->first;
			TimeAndValue& timeAndValue = iterator->second;
			auto ts = timeAndValue.first;
			auto tsAndKeys = std::make_pair(ts, keyTuplePointer);
			sassert(timeToKeys_.erase(tsAndKeys) == 1);
			keysToTimeAndValue_.erase(iterator);
			return 1;
		} else {
			return 0;
		}
	}

	/**
	 * Does the same as 'cleanup', but without acquiring a lock
	 */
	void cleanupWithoutLocking(SteadyTimePoint currentTs, uint64_t maxOperations) {
		for (uint64_t i = 0; i < maxOperations; ++i) {
			auto oldestEntry = timeToKeys_.begin();
			if (oldestEntry == timeToKeys_.end()) {
				return;
			}
			auto& ts = std::get<0>(*oldestEntry);
			if ((ts + maxTime_ < currentTs) || (timeToKeys_.size() > maxElements_)) {
				++cacheExpired;
				auto keyTuplePtr = std::get<1>(*oldestEntry);
				timeToKeys_.erase(oldestEntry);
				sassert(keysToTimeAndValue_.erase(*keyTuplePtr) == 1);
			} else {
				return;
			}
		}
	}

	SteadyDuration maxTime_;
	uint64_t maxElements_;
	ValueObtainer valueObtainer_;
	Mutex mutex_;
	KeysTupleToTimeAndValue keysToTimeAndValue_;
	std::set<TimeAndKeysPtr> timeToKeys_;
};
