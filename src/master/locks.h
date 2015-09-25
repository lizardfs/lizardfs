/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include "common/compact_vector.h"
#include "protocol/lock_info.h"

#include <unordered_map>

/*! \brief Representation of half-open interval [a, b) with it's type and owner */
struct LockRange {
	/*! \brief LockRange owner identified by session id and FUSE's owner */
	struct Owner {
		/*! \brief Compares owner and sessionid fields, ignoring reqid and msgid */
		inline bool operator==(const Owner &other) const {
			return owner == other.owner && sessionid == other.sessionid;
		}

		/*! \brief Compares owner and sessionid fields, ignoring reqid and msgid */
		inline bool operator!=(const Owner &other) const {
			return !this->operator==(other);
		}

		/*! \brief Compares owner and sessionid fields, ignoring reqid and msgid */
		inline bool operator<(const Owner &other) const {
			return (owner < other.owner)
				|| (owner == other.owner && sessionid < other.sessionid);
		}

		uint64_t owner;     /*!< owner field provided by FUSE */
		uint32_t sessionid; /*!< client's session id */
		uint32_t reqid;     /*!< request id provided from FUSE (interrupt handling only) */
		uint32_t msgid;     /*!< message id provided from mount (interrupt handling only) */
	};

	/*!
	 * \brief Possible types of LockRange
	 *
	 *   kExclusive - equivalent to Linux's F_WRLCK
	 *   kShared    - equivalent to Linux's F_RDLCK
	 *   kUnlock    - equivalent to Linux's F_UNLCK
	 */
	enum class Type : uint8_t {
		kInvalid   = lzfs_locks::kInvalid,
		kUnlock    = lzfs_locks::kUnlock,
		kShared    = lzfs_locks::kShared,
		kExclusive = lzfs_locks::kExclusive
	};

	typedef compact_vector<Owner> Owners;

	LockRange() : type(Type::kInvalid), start(), end(), owners() {}

	LockRange(Type type, uint64_t start, uint64_t end, Owner first)
		: type(type), start(start), end(end), owners() {
		owners.push_back(first);
	}

	bool valid() const {
		return type != Type::kInvalid;
	}

	/*! \brief Checks if range overlaps with the other one */
	bool overlaps(const LockRange &other) const {
		return start < other.end && other.start < end;
	}

	/*! \brief Checks if one range's end is equal to the other one's start */
	bool sticks(const LockRange &other) const {
		return start == other.end || other.start == end;
	}

	/*! \brief Checks if range lock is shared */
	bool shared() const {
		return type == Type::kShared;
	}

	/*! \brief Checks if range lock is exclusive */
	bool exclusive() const {
		return type == Type::kExclusive;
	}

	/*! \brief Checks if range lock is destined to be removed (unlocked) */
	bool unlocking() const {
		return type == Type::kUnlock || owners.size() == 0;
	}

	/*! \brief Marks range to be removed in the future */
	void markUnlocking() {
		type = Type::kUnlock;
	}

	/*! \brief Returns the only owner of range. This function can be called only
	 *  on ranges that have exactly one unique owner
	 */
	const Owner &owner() const {
		assert(owners.size() == 1);
		return *owners.begin();
	}

	/*! \brief Checks if candidate is one of the owners of range */
	bool hasOwner(const Owner &candidate) const {
		return std::binary_search(owners.begin(), owners.end(), candidate);
	}

	/*! \brief Removes candidate from range's owners. If candidate is not in owners, does nothing */
	void eraseOwner(const Owner &candidate) {
		auto it = std::lower_bound(owners.begin(), owners.end(), candidate);
		if (it != owners.end() && *it == candidate) {
			owners.erase(it);
		}
	}

	/*! \brief Adds a set of owners to range */
	void addOwners(const Owners &other) {
		Owners result;

		std::merge(owners.begin(), owners.end(), other.begin(), other.end(),
				std::back_inserter(result));
		result.erase(unique(result.begin(), result.end()), result.end());

		std::swap(owners, result);
	}

	Type type;       /*!< lock type (shared, exclusive, unlock) */
	uint64_t start;  /*!< beginning of locked range */
	uint64_t end;    /*!< end of locked range */
	Owners owners;   /*!< set of lock owners */
};


/*! \brief Set of ranges, allows to insert, delete and overwrite existing ranges */
class LockRanges {
public:
	typedef compact_vector<LockRange> Container;
	typedef Container::iterator iterator;
	typedef Container::const_iterator const_iterator;

	/*! \brief Checks if range is suitable for insertion */
	bool fits(const LockRange &range) const;

	/*! Tries to find first lock colliding with the given one
	 * \return address of the first colliding lock, nullptr otherwise */
	const LockRange *findCollision(const LockRange &range) const;

	/*! \brief Inserts range into the structure
	 *  Assumes that range is suitable for insertion (fits() function returned true)
	 */
	void insert(LockRange &range);

	iterator erase(const_iterator start, const_iterator end) {
		return data_.erase(start, end);
	}

	size_t size() const {
		return data_.size();
	}

	iterator begin() {
		return data_.begin();
	}

	iterator end() {
		return data_.end();
	}

	const_iterator begin() const {
		return data_.begin();
	}

	const_iterator end() const {
		return data_.end();
	}

	void clear();

private:

	/*! \brief Inserts range into data structure, preserving begin/end iterators */
	Container::iterator insert(const Container::iterator &it, const LockRange &range,
			Container::iterator &begin, Container::iterator &end);

	Container::iterator insert(const Container::iterator &it, LockRange &&range,
			Container::iterator &begin, Container::iterator &end);

	Container data_;
};

inline bool operator<(const LockRange &range, const LockRange &other) {
	return range.start < other.start || (range.start == other.start && range.end < other.end);
}

inline bool operator==(const LockRange &range, const LockRange &other) {
	return range.start == other.start && range.end == other.end && range.owners == other.owners;
}

class FileLocks {
public:
	typedef LockRange::Owner Owner;
	typedef LockRange Lock;
	/*! \brief Set of all applied locks */
	typedef LockRanges Locks;
	/*! \brief Queue of all pending locks */
	typedef compact_vector<Lock> LockQueue;

	FileLocks() : active_locks_(), pending_locks_() {
	}

	/*! \brief Tries to place a lock on inode */
	bool apply(uint32_t inode, Lock lock, bool nonblocking = false);

	/*!
	 * \brief Tries to place a read (shared) lock on inode
	 * \param start - beginning of locked range
	 * \param end - end of locked range
	 * \return true if operation succeeded
	 */
	bool sharedLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
			bool nonblocking = false);

	/*!
	 * \brief Tries to place a write (exclusive) lock on inode
	 * \param start - beginning of locked range
	 * \param end - end of locked range
	 * \return true if operation succeeded
	 */
	bool exclusiveLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
			bool nonblocking = false);

	/*!
	 * \brief Checks if any offending locks are active.
	 * \return lock's address if it exists, nullptr otherwise.
	 */
	const Lock *findCollision(uint32_t inode, Lock::Type type, uint64_t start, uint64_t end,
			Owner owner);

	/*!
	 * \brief Tries to unlock an inode.
	 * Call to this function should likely be followed by gatherCandidates(),
	 * because there might be locks in the queue available for insertion
	 * after this unlock.
	 * \param start - beginning of unlocked range
	 * \param end - end of unlocked range
	 * \return true if operation succeeded
	 */
	bool unlock(uint32_t inode, uint64_t start, uint64_t end, Owner owner);

	/*!
	 * \brief Removes all locks from specified inode
	 */
	void unlock(uint32_t inode);

	/*! \brief Removes locks from specified inode.
	 * \param pred unary predicate applied to its owner.
	 * \return Range of a file affected by this unlock
	 */
	template<typename UnaryPredicate>
	std::pair<uint64_t, uint64_t> unlock(uint32_t inode, UnaryPredicate pred);

	/*! \brief Gather candidates from pending locks.
	 * \param inode inode number
	 * \param start beginning of regarded range
	 * \param end end of regarded range
	 * \return a list of locks from pending queue that might be available
	 * after removing a lock from range [start, end).
	 * Candidates are not guaranteed to be suitable for insertion,
	 * it still needs to be checked with a call to fits() function.
	 * This function effectively removes candidates from queue,
	 * so they need to be reinserted after checking if they can be applied
	 */
	void gatherCandidates(uint32_t inode, uint64_t start, uint64_t end, LockQueue &result);

	/*! \brief Removes pending lock from queue using unary predicate applied to lock */
	template<typename UnaryPredicate>
	void removePending(uint32_t inode, UnaryPredicate pred);

	/*! \brief Copy active locks to vector storage.
	 * \param index index of first lock to copy
	 * \param count number of locks to copy
	 * \param data output vector with copied locks
	 */
	void copyActiveToVector(int64_t index, int64_t count, std::vector<lzfs_locks::Info> &data);

	/*! \brief Copy pending locks to vector storage.
	 * \param index index of first lock to copy
	 * \param count number of locks to copy
	 * \param data output vector with copied locks
	 */
	void copyPendingToVector(int64_t index, int64_t count, std::vector<lzfs_locks::Info> &data);

	/*! \brief Copy active locks for specific inode to vector storage.
	 * \param inode inode number
	 * \param index index of first lock to copy
	 * \param count number of locks to copy
	 * \param data output vector with copied locks
	 */
	void copyActiveToVector(uint32_t inode, int64_t index, int64_t count,
	                        std::vector<lzfs_locks::Info> &data);

	/*! \brief Copy pending locks for specific inode to vector storage.
	 * \param inode inode number
	 * \param index index of first lock to copy
	 * \param count number of locks to copy
	 * \param data output vector with copied locks
	 */
	void copyPendingToVector(uint32_t inode, int64_t index, int64_t count,
	                        std::vector<lzfs_locks::Info> &data);

	/*! \brief Load class state from stream.
	 * \param file pointer to FILE structure that specifies input stream
	 */
	void load(FILE *file);

	/*! \brief Save class state to stream.
	 * \param file pointer to FILE structure that specifies output stream
	 */
	void store(FILE *file);

	/*! \brief Removes all locks from the class. */
	void clear();

private:
	FileLocks(const FileLocks &other) = delete;
	FileLocks(FileLocks &&other) = delete;
	FileLocks &operator=(const FileLocks &) = delete;
	FileLocks &operator=(FileLocks &&) = delete;

	/*! \brief Enqueues a lock */
	void enqueue(uint32_t inode, Lock lock);

	std::unordered_map<uint32_t, Locks> active_locks_;
	std::unordered_map<uint32_t, LockQueue> pending_locks_;
};

template<typename UnaryPredicate>
void FileLocks::removePending(uint32_t inode, UnaryPredicate pred) {
	auto it = pending_locks_.find(inode);
	if (it == pending_locks_.end()) {
		return;
	}
	LockQueue &queue = it->second;

	queue.erase(
		std::remove_if(queue.begin(), queue.end(), pred),
		queue.end()
	);

	// If last lock was unqueued, inode info can be removed from structure
	if (queue.empty()) {
		pending_locks_.erase(it);
	}
}

template<typename UnaryPredicate>
std::pair<uint64_t, uint64_t> FileLocks::unlock(uint32_t inode, UnaryPredicate pred) {
	uint64_t start = std::numeric_limits<uint64_t>::max();
	uint64_t end = 0;
	auto it = active_locks_.find(inode);
	if (it == active_locks_.end()) {
		return {start, end};
	}
	auto &locks = it->second;

	auto erase_it = std::remove_if(locks.begin(), locks.end(),
			[pred, &start, &end] (LockRange &lock) {
				auto erased_owners_it = std::remove_if(lock.owners.begin(),
						lock.owners.end(), pred);
				// If owner was erased, lock's range might become a candidate for overwrite
				if (erased_owners_it != lock.owners.end()) {
					start = std::min(start, lock.start);
					end = std::max(end, lock.end);
				}
				if (erased_owners_it != lock.owners.begin()) {
					lock.owners.erase(erased_owners_it, lock.owners.end());
					return false;
				}

				return true;
			});
	locks.erase(erase_it, locks.end());
	return {start, end};
}
