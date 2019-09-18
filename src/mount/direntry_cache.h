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

#include "common/platform.h"

#include <atomic>

#include "common/attributes.h"
#include "common/shared_mutex.h"
#include "common/time_utils.h"
#include "mount/lizard_client_context.h"
#include "protocol/directory_entry.h"

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

/*! \brief Cache for directory entries
 *
 * Implementation of directory cache with following properties
 *   - fast lookup by parent inode + entry name
 *   - fast lookup by parent inode + entry index
 *   - fast lookup by inode
 *   - fast removal of oldest entries
 *
 * \warning Only explicitly specified methods are thread safe.
 */
class DirEntryCache {
public:
	struct DirEntry {
		DirEntry(const LizardClient::Context &ctx, uint32_t parent_inode, uint32_t inode,
		         uint64_t index, uint64_t next_index, std::string name,
				 Attributes attr, uint64_t ts)
		    : uid(ctx.uid),
		      gid(ctx.gid),
		      parent_inode(parent_inode),
		      inode(inode),
		      index(index),
			  next_index(next_index),
		      timestamp(ts),
		      name(name),
		      attr(attr) {
		}

		std::string toString() const {
			return "Entry " + std::to_string(inode) + ": ctx=(" + std::to_string(uid) +
			       "," + std::to_string(gid) + ") parent_inode=" +
			       std::to_string(parent_inode) + ", index=" + std::to_string(index) +
				   ", next_index=" + std::to_string(next_index) +
			       ", timestamp=" + std::to_string(timestamp) + ", name=" + name;
		}

		uint32_t uid;
		uint32_t gid;
		uint32_t parent_inode;
		uint32_t inode;
		uint64_t index;
		uint64_t next_index;
		uint64_t timestamp;
		std::string name;
		Attributes attr;

		boost::intrusive::set_member_hook<>
		        lookup_hook; /*!< For lookups (parent inode, name) */
		boost::intrusive::set_member_hook<>
		        index_hook; /*!< For getdir (parent inode, index) */
		boost::intrusive::set_member_hook<>
		        inode_hook; /*!< For extracting inode's attributes */
		boost::intrusive::list_member_hook<> fifo_hook; /*!< For removing oldest entries */
	};

protected:
	struct LookupCompare {
		bool operator()(const DirEntry &e1, const DirEntry &e2) const {
			return std::make_tuple(e1.parent_inode, e1.uid, e1.gid, e1.name) <
			       std::make_tuple(e2.parent_inode, e2.uid, e2.gid, e2.name);
		}

		bool operator()(const DirEntry &e,
		                const std::tuple<uint32_t, uint32_t, uint32_t, std::string>
		                        &lookup_info) const {
			return std::make_tuple(e.parent_inode, e.uid, e.gid, e.name) < lookup_info;
		}

		bool operator()(
		        const std::tuple<uint32_t, uint32_t, uint32_t, std::string> &lookup_info,
		        const DirEntry &e) const {
			return lookup_info < std::make_tuple(e.parent_inode, e.uid, e.gid, e.name);
		}
	};

	struct IndexCompare {
		bool operator()(const DirEntry &e1, const DirEntry &e2) const {
			return std::make_tuple(e1.parent_inode, e1.uid, e1.gid, e1.index) <
			       std::make_tuple(e2.parent_inode, e2.uid, e2.gid, e2.index);
		}

		bool operator()(const DirEntry &e,
		                const std::tuple<uint32_t, uint32_t, uint32_t, uint64_t>
		                        &index_info) const {
			return std::make_tuple(e.parent_inode, e.uid, e.gid, e.index) < index_info;
		}

		bool operator()(
		        const std::tuple<uint32_t, uint32_t, uint32_t, uint64_t> &index_info,
		        const DirEntry &e) const {
			return index_info < std::make_tuple(e.parent_inode, e.uid, e.gid, e.index);
		}
	};

	struct InodeCompare {
		bool operator()(const DirEntry &e1, const DirEntry &e2) const {
			return e1.inode < e2.inode;
		}

		bool operator()(const DirEntry &e, const uint32_t &inode) const {
			return e.inode < inode;
		}

		bool operator()(const uint32_t &inode, const DirEntry &e) const {
			return inode < e.inode;
		}
	};

public:
	typedef boost::intrusive::set<
	        DirEntry,
	        boost::intrusive::member_hook<DirEntry, boost::intrusive::set_member_hook<>,
	                                      &DirEntry::lookup_hook>,
	        boost::intrusive::compare<LookupCompare>,
	        boost::intrusive::constant_time_size<true>>
	        LookupSet;

	typedef boost::intrusive::set<
	        DirEntry,
	        boost::intrusive::member_hook<DirEntry, boost::intrusive::set_member_hook<>,
	                                      &DirEntry::index_hook>,
	        boost::intrusive::compare<IndexCompare>, boost::intrusive::constant_time_size<true>>
	        IndexSet;

	typedef boost::intrusive::multiset<
	        DirEntry,
	        boost::intrusive::member_hook<DirEntry, boost::intrusive::set_member_hook<>,
	                                      &DirEntry::inode_hook>,
	        boost::intrusive::compare<InodeCompare>, boost::intrusive::constant_time_size<true>>
	        InodeMultiset;

	typedef boost::intrusive::list<
	        DirEntry,
	        boost::intrusive::member_hook<DirEntry, boost::intrusive::list_member_hook<>,
	                                      &DirEntry::fifo_hook>,
	        boost::intrusive::constant_time_size<true>, boost::intrusive::cache_last<true>>
	        FifoList;

	typedef shared_mutex SharedMutex;

	/*! \brief Constructor.
	 *
	 * \param timeout    cache entry expiration timeout (us).
	 */
	DirEntryCache(uint64_t timeout = kDefaultTimeout_us)
	    : timer_(), current_time_(0), timeout_(timeout) {
	}

	~DirEntryCache() {
		auto it = fifo_list_.begin();
		while (it != fifo_list_.end()) {
			auto next_it = std::next(it);
			erase(std::addressof(*it));
			it = next_it;
		}
	}

	/*! \brief Set cache entry expiration timeout (us).
	 *
	 * \param timeout    entry expiration timeout (us).
	 */
	void setTimeout(uint64_t timeout) {
		timeout_ = timeout;
	}

	/*! \brief Check if entry is valid (not expired). */
	bool isValid(const IndexSet::iterator &index_it) const {
		return index_it != index_set_.end() && !expired(*index_it, current_time_);
	}

	/*! \brief Check if entry is valid (not expired). */
	bool isValid(const IndexSet::const_iterator &index_it) const {
		return index_it != index_set_.end() && !expired(*index_it, current_time_);
	}

	/*! \brief Find directory entry in cache.
	 *
	 * \param ctx Process credentials.
	 * \param parent_inode Parent inode.
	 * \param name Directory entry name.
	 *
	 * \return Iterator to found entry.
	 */
	LookupSet::iterator find(const LizardClient::Context &ctx, uint32_t parent_inode,
	                         const std::string &name) {
		return lookup_set_.find(std::make_tuple(parent_inode, ctx.uid, ctx.gid, name),
		                        LookupCompare());
	}

	/*! \brief Find directory entry in cache.
	 *
	 * \param ctx Process credentials.
	 * \param parent_inode Parent inode.
	 * \param index Entry index in directory.
	 *
	 * \return Iterator to found entry.
	 */
	IndexSet::iterator find(const LizardClient::Context &ctx, uint32_t parent_inode,
	                        uint64_t index) {
		return index_set_.find(std::make_tuple(parent_inode, ctx.uid, ctx.gid, index),
		                       IndexCompare());
	}

	/*! \brief Find directory entry in cache.
	 *
	 * \param ctx Process credentials.
	 * \param inode Node inode.
	 *
	 * \return Iterator to found entry.
	 */
	InodeMultiset::iterator find(const LizardClient::Context &ctx, uint32_t inode) {
		auto it = inode_multiset_.lower_bound(inode, InodeCompare());

		while (it != inode_multiset_.end() && it->inode == inode) {
			if (it->uid == ctx.uid && it->gid == ctx.gid) {
				return it;
			}
			it++;
		}
		return inode_multiset_.end();
	}

	/*! \brief Get attributes of an inode.
	 *
	 * \warning This function takes read (shared) lock.
	 *
	 * \param ctx Process credentials.
	 * \param inode Node index (inode).
	 * \param attr Output: attributes of found inode.
	 *
	 * \return True if inode has been found in cache, false otherwise.
	 */
	bool lookup(const LizardClient::Context &ctx, uint32_t inode, Attributes &attr) {
		shared_lock<SharedMutex> guard(rwlock_);
		updateTime();
		auto it = find(ctx, inode);
		if (it == inode_multiset_.end() || expired(*it, current_time_) || it->inode == 0) {
			return false;
		}
		attr = it->attr;
		return true;
	}

	/*! \brief Get attributes of directory entry.
	 *
	 * \warning This function takes read (shared) lock.
	 *
	 * \param ctx Process credentials.
	 * \param parent_inode Parent node index (inode).
	 * \param name Name of directory entry to find.
	 * \param inode Output: inode of directory entry.
	 * \param attr Output: attributes of found directory entry.
	 *
	 * \return True if inode has been found in cache, false otherwise.
	 */
	bool lookup(const LizardClient::Context &ctx, uint32_t parent_inode,
	            const std::string &name, uint32_t &inode, Attributes &attr) {
		shared_lock<SharedMutex> guard(rwlock_);
		updateTime();
		auto it = find(ctx, parent_inode, name);
		if (it == lookup_set_.end() || expired(*it, current_time_) || it->inode == 0) {
			return false;
		}
		inode = it->inode;
		attr = it->attr;
		return true;
	}

	/*! \brief Add directory entry information to cache.
	 *
	 * \param ctx Process credentials.
	 * \param parent_inode Parent node index (inode).
	 * \param inode Inode of directory entry.
	 * \param index Position of entry in directory listing.
	 * \param next_index Position of the next entry.
	 * \param name Name of directory entry.
	 * \param attr attributes of found directory entry.
	 * \param timestamp Time when data has been obtained (used for entry timeout).
	 */
	void insert(const LizardClient::Context &ctx, uint32_t parent_inode, uint32_t inode,
	            uint64_t index, uint64_t next_index, const std::string name,
				const Attributes &attr, uint64_t timestamp) {
		// Avoid inserting stale data
		if (timestamp + timeout_ <= current_time_) {
			return;
		}
		removeExpired(1, timestamp);
		auto lookup_it = find(ctx, parent_inode, name);
		if (lookup_it != lookup_set_.end()) {
			erase(std::addressof(*lookup_it));
		}
		auto index_it = find(ctx, parent_inode, index);
		if (index_it != index_set_.end()) {
			erase(std::addressof(*index_it));
		}
		addEntry(ctx, parent_inode, inode, index, next_index, name, attr, timestamp);
	}

	/*! \brief Add data to cache from container.
	 *
	 * \param ctx Process credentials.
	 * \param parent_inode Parent node index (inode).
	 * \param first_index Position of entry in directory listing.
	 * \param container Container with data to add to cache.
	 * \param timestamp Time when data has been obtained (used for entry timeout).
	 */
	template <typename Container>
	void insertSubsequent(const LizardClient::Context &ctx, uint32_t parent_inode,
	                      uint64_t first_index, const Container &container,
	                      uint64_t timestamp) {
		// Avoid inserting stale data
		if (timestamp + timeout_ <= current_time_) {
			return;
		}
		removeExpired(container.size(), timestamp);
		auto it = index_set_.lower_bound(
		        std::make_tuple(parent_inode, ctx.uid, ctx.gid, first_index),
		        IndexCompare());

		for (const DirectoryEntry &de : container) {
			auto lookup_it = find(ctx, parent_inode, de.name);
			if (it == index_set_.end() ||
			    std::make_tuple(parent_inode, ctx.uid, ctx.gid) !=
			            std::make_tuple(it->parent_inode, it->uid, it->gid) ||
			    it->index != de.index) {
				if (lookup_it != lookup_end()) {
					erase(std::addressof(*lookup_it));
				}
				it = addEntry(ctx, parent_inode, de.inode, de.index, de.next_index, de.name,
				              de.attributes, timestamp);
			} else {
				if (lookup_it != lookup_end() && it != index_set_.iterator_to(*lookup_it)) {
					erase(std::addressof(*lookup_it));
				}
				overwriteEntry(*it, de, timestamp);
			}
			++it;
		}
	}

	/*! \brief Remove data from cache matching specified criteria.
	 *
	 * \param ctx Process credentials.
	 * \param parent_inode Parent node inode.
	 * \param first_index Directory index of first entry to remove.
	 */
	void invalidate(const LizardClient::Context &ctx, uint32_t parent_inode,
	                uint64_t first_index = 0) {
		auto it = index_set_.lower_bound(
		        std::make_tuple(parent_inode, ctx.uid, ctx.gid, first_index),
		        IndexCompare());
		while (it != index_set_.end() &&
		       std::make_tuple(parent_inode, ctx.uid, ctx.gid) ==
		               std::make_tuple(it->parent_inode, it->uid, it->gid)) {
			assert(it->index >= first_index);

			DirEntry *entry = std::addressof(*it);
			++it;
			erase(entry);
		}
	}

	/*! \brief Remove data from cache matching specified criteria.
	 *
	 * \warning This function takes write (unique) lock.
	 *
	 * \param inode Inode.
	 */
	void lockAndInvalidateInode(uint32_t inode) {
		std::unique_lock<SharedMutex> guard(rwlock_);
		auto it = inode_multiset_.find(inode, InodeCompare());
		while (it != inode_multiset_.end() && it->inode == inode) {
			DirEntry *entry = std::addressof(*it);
			++it;
			erase(entry);
		}
	}

	/*! \brief Remove data from cache matching specified criteria.
	 *
	 * \warning This function takes write (unique) lock.
	 *
	 * \param parent_inode Parent inode.
	 */
	void lockAndInvalidateParent(uint32_t parent_inode) {
		std::unique_lock<SharedMutex> guard(rwlock_);
		auto it = index_set_.lower_bound(std::make_tuple(parent_inode, 0, 0, 0),
		                                 IndexCompare());
		while (it != index_set_.end() && it->parent_inode == parent_inode) {
			DirEntry *entry = std::addressof(*it);
			++it;
			erase(entry);
		}
	}

	/*! \brief Remove data from cache matching specified criteria.
	 *
	 * \warning This function takes write (unique) lock.
	 *
	 * \param ctx Process credentials.
	 * \param parent_inode Parent inode.
	 */
	void lockAndInvalidateParent(const LizardClient::Context &ctx, uint32_t parent_inode) {
		std::unique_lock<SharedMutex> guard(rwlock_);

		auto it = index_set_.lower_bound(std::make_tuple(parent_inode, ctx.uid, ctx.gid, 0),
		                                 IndexCompare());
		while (it != index_set_.end() &&
		       std::make_tuple(parent_inode, ctx.uid, ctx.gid) ==
		               std::make_tuple(it->parent_inode, it->uid, it->gid)) {
			DirEntry *entry = std::addressof(*it);
			++it;
			erase(entry);
		}
	}

	LookupSet::const_iterator lookup_begin() const {
		return lookup_set_.begin();
	}

	IndexSet::const_iterator index_begin() const {
		return index_set_.begin();
	}

	InodeMultiset::const_iterator inode_begin() const {
		return inode_multiset_.begin();
	}

	LookupSet::const_iterator lookup_end() const {
		return lookup_set_.end();
	}

	IndexSet::const_iterator index_end() const {
		return index_set_.end();
	}

	InodeMultiset::const_iterator inode_end() const {
		return inode_multiset_.end();
	}

	/*! \brief Get number of elements in cache. */
	size_t size() const {
		return lookup_set_.size();
	}

	/*! \brief Get reference to reader-writer (shared) mutex. */
	SharedMutex &rwlock() {
		return rwlock_;
	}

	/*! \brief Remove expired elements from cache.
	 *
	 * \param max_to_remove Limit on number of removed entries.
	 * \param timestamp Current time.
	 */
	void removeExpired(int max_to_remove, uint64_t timestamp) {
		int i = 0;
		while (!fifo_list_.empty()) {
			if (!expired(fifo_list_.front(), timestamp)) {
				break;
			}
			if (i >= max_to_remove) {
				break;
			}
			DirEntry *entry = std::addressof(fifo_list_.front());
			erase(entry);
			++i;
		}
	}

	/*! \brief Remove expired elements from cache.
	 *
	 * \param max_to_remove Limit on number of removed entries.
	 */
	void removeExpired(int max_to_remove) {
		removeExpired(max_to_remove, current_time_);
	}

	/*! \brief Remove oldest elements from cache.
	 *
	 * \param count Number of entries to remove.
	 */
	void removeOldest(std::size_t count) {
		for(std::size_t i = 0; i < count && !fifo_list_.empty(); ++i) {
			DirEntry *entry = std::addressof(fifo_list_.front());
			erase(entry);
		}
	}

	/*! \brief Update internal time to wall time.
	 *
	 * \return Current internal time.
	 */
	uint64_t updateTime() {
		current_time_ = timer_.elapsed_us();
		return current_time_;
	}

protected:
	void erase(DirEntry *entry) {
		lookup_set_.erase(lookup_set_.iterator_to(*entry));
		index_set_.erase(index_set_.iterator_to(*entry));
		inode_multiset_.erase(inode_multiset_.iterator_to(*entry));
		fifo_list_.erase(fifo_list_.iterator_to(*entry));
		delete entry;
	}

	bool expired(const DirEntry &entry, uint64_t timestamp) const {
		return entry.timestamp + timeout_ <= timestamp;
	}

	void overwriteEntry(DirEntry &entry, DirectoryEntry de, uint64_t timestamp) {
		if (entry.inode != de.inode) {
			inode_multiset_.erase(inode_multiset_.iterator_to(entry));
			entry.inode = de.inode;
			inode_multiset_.insert(entry);
		}

		if (entry.name != de.name) {
			lookup_set_.erase(lookup_set_.iterator_to(entry));
			entry.name = de.name;
			lookup_set_.insert(entry);
		}

		fifo_list_.erase(fifo_list_.iterator_to(entry));
		fifo_list_.push_back(entry);
		entry.timestamp = timestamp;
		entry.attr = de.attributes;
	}

	IndexSet::iterator addEntry(const LizardClient::Context &ctx, uint32_t parent_inode,
	                            uint32_t inode, uint64_t index, uint64_t next_index,
								std::string name, Attributes attr, uint64_t timestamp) {
		DirEntry *entry =
		        new DirEntry(ctx, parent_inode, inode, index, next_index, name, attr, timestamp);
		lookup_set_.insert(*entry);
		auto result = index_set_.insert(*entry);
		inode_multiset_.insert(*entry);
		fifo_list_.push_back(*entry);
		return result.first;
	}

	Timer timer_;
	std::atomic<uint64_t> current_time_;
	uint64_t timeout_;
	LookupSet lookup_set_;
	IndexSet index_set_;
	InodeMultiset inode_multiset_;
	FifoList fifo_list_;
	SharedMutex rwlock_;

	static const int kDefaultTimeout_us = 500000;
};
