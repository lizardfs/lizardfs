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

#include "common/generic_lru_cache.h"
#include "common/small_vector.h"
#include "protocol/cltoma.h"

#include <atomic>
#include <mutex>

/*!
 * Class responsible for cache'ing primary/secondary groups information.
 * Added group sets are indexed by consecutive integers.
 */
class GroupCache {
public:
	static constexpr uint32_t kMaxGroupId = ((uint32_t)1 << 31) - 1;
	static constexpr int kDefaultGroupsSize = cltoma::updateCredentials::kDefaultGroupsSize;
	typedef cltoma::updateCredentials::GroupsContainer Groups;

	struct GroupHash {
		size_t operator()(const Groups &groups) const {
			size_t seed = 0;
			for (uint32_t i : groups) {
				hash_combine(seed, i);
			}
			return seed;
		}

		inline void hash_combine(size_t &seed, uint32_t v) const {
			seed ^= std::hash<uint32_t>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
		}
	};

	/*! \brief Result of a find operation. If 'found' is true, index is a valid group set index */
	struct Result {
		uint32_t index;
		bool found;
	};

	typedef GenericLruCache<Groups, uint32_t, 1024, GroupHash> Cache;
	typedef typename Cache::iterator iterator;
	typedef typename Cache::const_iterator const_iterator;

	GroupCache() : index_(0), mutex_(), cache_() {}

	/*! \brief Find group set in cache */
	Result find(const Groups &groups) {
		std::lock_guard<std::mutex> guard(mutex_);
		auto it = cache_.find(groups);
		return (it == cache_.end()) ? Result{0, false} : Result{it->second, true};
	}

	/*! \brief Put group set in cache */
	uint32_t put(const Groups &groups) {
		std::lock_guard<std::mutex> guard(mutex_);
		index_++;
		index_ %= kMaxGroupId;
		cache_.insert(groups, index_);
		return index_;
	}

	/*! \brief Find group set in cache by its index */
	Groups findByIndex(uint32_t index) {
		std::lock_guard<std::mutex> guard(mutex_);
		auto it = cache_.findByValue(index);
		return (it == cache_.end()) ? Groups() : it->first;
	}

	void reset() {
		std::lock_guard<std::mutex> guard(mutex_);
		cache_.clear();
		index_ = 0;
	}

protected:
	uint32_t index_;
	std::mutex mutex_;
	Cache cache_;
};
