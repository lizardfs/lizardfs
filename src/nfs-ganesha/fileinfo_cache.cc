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

#include <atomic>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <cassert>
#include <mutex>

#include "common/time_utils.h"
#include "fileinfo_cache.h"
#include "mount/client/lizardfs_c_api.h"

class FileInfoCache {
public:
	struct Entry : public boost::intrusive::list_base_hook<>,
	               public boost::intrusive::set_base_hook<> {
		liz_inode_t inode;
		liz_fileinfo_t *fileinfo;
		bool is_used;
		Timer timer;

		Entry() : inode(), fileinfo(), is_used(false), timer() {
		}
		Entry(liz_inode_t inode, liz_fileinfo_t *fileinfo)
		    : inode(inode), fileinfo(fileinfo), is_used(false), timer() {
		}

		bool operator==(const Entry &other) const {
			return inode == other.inode;
		}

		bool operator!=(const Entry &other) const {
			return inode != other.inode;
		}

		bool operator<(const Entry &other) const {
			return inode < other.inode;
		}

		bool expired(int timeout_ms) {
			return !is_used && timer.elapsed_ms() >= timeout_ms;
		}
	};
	struct EntryCompare {
		bool operator()(const liz_inode_t &inode, const Entry &entry) const {
			return inode < entry.inode;
		}
		bool operator()(const Entry &entry, const liz_inode_t &inode) const {
			return entry.inode < inode;
		}
	};

	typedef boost::intrusive::list<Entry, boost::intrusive::constant_time_size<true>,
	                               boost::intrusive::cache_last<true>>
	    EntryList;
	typedef boost::intrusive::multiset<Entry, boost::intrusive::constant_time_size<true>>
	    EntryMultiSet;

	static constexpr unsigned kDefaultMaxEntries = 16384;
	static constexpr int kDefaultMinTimeout_ms = 30000;

	FileInfoCache() : max_entries_(kDefaultMaxEntries), min_timeout_ms_(kDefaultMinTimeout_ms) {
	}
	FileInfoCache(unsigned max_entries, int min_timeout_ms)
	    : max_entries_(max_entries), min_timeout_ms_(min_timeout_ms) {
	}

	~FileInfoCache() {
		assert(lru_.empty());
		assert(entry_lookup_.empty());
		// If anything is actually here it will leak internal descriptors,
		// but we cannot 'assert' anything here in case this cache is used
		// in some external code.
		while (!used_.empty()) {
			Entry *entry = std::addressof(used_.front());
			used_.pop_front();
			delete entry;
		}
	}

	/*!
	 * \brief Acquire fileinfo from cache
	 * \param inode Inode of a file
	 * \return Cache entry. entry->fileinfo will be null if file still needs to be open first
	 * \post Set entry->fileinfo to a valid fileinfo pointer after opening a file
	 */
	Entry *acquire(liz_inode_t inode) {
		std::lock_guard<std::mutex> guard(mutex_);
		auto it = entry_lookup_.find(inode, EntryCompare());
		if (it != entry_lookup_.end()) {
			assert(!it->is_used);
			it->is_used = true;
			it->timer.reset();
			used_.splice(used_.begin(), lru_, lru_.iterator_to(*it));
			entry_lookup_.erase(entry_lookup_.iterator_to(*it));
			return std::addressof(*it);
		}
		Entry *entry = new Entry(inode, nullptr);
		entry->is_used = true;
		used_.push_front(*entry);
		return entry;
	}

	/*!
	 * \brief Release fileinfo from cache
	 * \param entry pointer returned from previous acquire() call
	 */
	void release(Entry *entry) {
		std::lock_guard<std::mutex> guard(mutex_);
		assert(entry->is_used);
		entry->is_used = false;
		entry->timer.reset();
		lru_.splice(lru_.end(), used_, used_.iterator_to(*entry));
		entry_lookup_.insert(*entry);
	}

	/*!
	 * \brief Immediately erase acquired entry
	 * \param entry pointer returned from previous acquire() call
	 */
	void erase(Entry *entry) {
		std::lock_guard<std::mutex> guard(mutex_);
		assert(entry->is_used);
		used_.erase(used_.iterator_to(*entry));
		delete entry;
	}

	/*!
	 * \brief Get expired fileinfo from cache
	 * \return entry removed from cache
	 * \post use this entry to call release() on entry->fileinfo
	 */
	Entry *popExpired() {
		std::lock_guard<std::mutex> guard(mutex_);
		if (lru_.empty()) {
			return nullptr;
		}
		Entry *entry = std::addressof(lru_.front());
		bool is_full = lru_.size() + used_.size() >= max_entries_;
		int timeout = is_full ? 0 : min_timeout_ms_.load();
		if (entry->expired(timeout)) {
			lru_.erase(lru_.iterator_to(*entry));
			entry_lookup_.erase(entry_lookup_.iterator_to(*entry));
			return entry;
		}
		return nullptr;
	}

	size_t size() {
		std::lock_guard<std::mutex> guard(mutex_);
		return lru_.size() + used_.size();
	}

	bool empty() {
		return size() == 0;
	}

	void setMaxEntries(unsigned max_entries) {
		max_entries_ = max_entries;
	}

	void setMinTimeout_ms(int min_timeout_ms) {
		min_timeout_ms_ = min_timeout_ms;
	}

protected:
	EntryList lru_;
	EntryList used_;
	EntryMultiSet entry_lookup_;

	std::atomic<unsigned> max_entries_;
	std::atomic<int> min_timeout_ms_;
	std::mutex mutex_;
};

liz_fileinfo_cache_t *liz_create_fileinfo_cache(unsigned max_entries, int min_timeout_ms) {
	try {
		FileInfoCache *cache = new FileInfoCache(max_entries, min_timeout_ms);
		return (liz_fileinfo_cache_t *)cache;
	} catch (...) {
		return nullptr;
	}
}

void liz_reset_fileinfo_cache_params(liz_fileinfo_cache_t *cache, unsigned max_entries,
                                     int min_timeout_ms) {
	FileInfoCache &fileinfo_cache = *(FileInfoCache *)cache;
	fileinfo_cache.setMaxEntries(max_entries);
	fileinfo_cache.setMinTimeout_ms(min_timeout_ms);
}

void liz_destroy_fileinfo_cache(liz_fileinfo_cache_t *cache) {
	delete (FileInfoCache *)cache;
}

liz_fileinfo_entry_t *liz_fileinfo_cache_acquire(liz_fileinfo_cache_t *cache, liz_inode_t inode) {
	FileInfoCache &fileinfo_cache = *(FileInfoCache *)cache;
	return (liz_fileinfo_entry_t *)fileinfo_cache.acquire(inode);
}

void liz_fileinfo_cache_release(liz_fileinfo_cache_t *cache, liz_fileinfo_entry_t *entry) {
	FileInfoCache &fileinfo_cache = *(FileInfoCache *)cache;
	FileInfoCache::Entry *fileinfo_entry = (FileInfoCache::Entry *)entry;
	fileinfo_cache.release(fileinfo_entry);
}

void liz_fileinfo_cache_erase(liz_fileinfo_cache_t *cache, liz_fileinfo_entry_t *entry) {
	FileInfoCache &fileinfo_cache = *(FileInfoCache *)cache;
	FileInfoCache::Entry *fileinfo_entry = (FileInfoCache::Entry *)entry;
	fileinfo_cache.erase(fileinfo_entry);
}

liz_fileinfo_entry_t *liz_fileinfo_cache_pop_expired(liz_fileinfo_cache_t *cache) {
	FileInfoCache &fileinfo_cache = *(FileInfoCache *)cache;
	return (liz_fileinfo_entry_t *)fileinfo_cache.popExpired();
}

void liz_fileinfo_entry_free(liz_fileinfo_entry_t *entry) {
	FileInfoCache::Entry *entry_ptr = (FileInfoCache::Entry *)entry;
	assert(!entry_ptr->is_used);
	delete entry_ptr;
}

liz_fileinfo_t *liz_extract_fileinfo(liz_fileinfo_entry_t *entry) {
	FileInfoCache::Entry &fileinfo_entry = *(FileInfoCache::Entry *)entry;
	return fileinfo_entry.fileinfo;
}

void liz_attach_fileinfo(liz_fileinfo_entry_t *entry, liz_fileinfo_t *fileinfo) {
	FileInfoCache::Entry &fileinfo_entry = *(FileInfoCache::Entry *)entry;
	fileinfo_entry.fileinfo = fileinfo;
}
