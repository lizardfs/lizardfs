#pragma once

#include <cassert>
#include <mutex>
#include <unordered_map>

#include "common/platform.h"


template<typename K, typename D>
class ThreadSafeMap {
public:
	ThreadSafeMap() : next_key_(0) {}

	/*! \brief Inserts element */
	void put(K key, D data) {
		std::lock_guard<std::mutex> lock(map_mutex_);
		map_[key] = data;
	}

	/*!
	* \brief Removes and returns element
	* \return if element of given key was present in the map,
	*         returns (true, element), otherwise (false, ??).
	*/
	std::pair<bool, D> take(K key) {
		std::lock_guard<std::mutex> lock(map_mutex_);
		std::pair<bool, D> ret;
		ret.first = false;
		if (map_.count(key)) {
			ret.first = true;
			ret.second = map_[key];
			map_.erase(key);
		}

		return ret;
	}

	K generateKey() {
		std::lock_guard<std::mutex> lock(map_mutex_);
		return ++next_key_;
	}

private:
	std::mutex map_mutex_;
	std::unordered_map<K, D> map_;
	K next_key_;

};
