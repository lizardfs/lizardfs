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
		kInvalid, kExclusive, kShared, kUnlock
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
	typedef compact_vector<LockRange> Data;

	/*! \brief Checks if range is suitable for insertion */
	bool fits(const LockRange &range) const;

	/*!
	 *  \brief Inserts range into the structure
	 *  Assumes that range is suitable for insertion (fits() function returned true)
	 */
	void insert(LockRange &range);

	size_t size() const {
		return data_.size();
	}

private:

	/*! \brief Inserts range into data structure, preserving begin/end iterators */
	Data::iterator insert(const Data::iterator &it, const LockRange &range,
			Data::iterator &begin, Data::iterator &end);

	Data::iterator insert(const Data::iterator &it, LockRange &&range,
			Data::iterator &begin, Data::iterator &end);

	Data data_;
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

	static FileLocks &instance() {
		static FileLocks instance_;

		return instance_;
	}

	/*!
	 * \brief Tries to place a lock on inode
	 */
	bool apply(uint32_t inode, Lock lock, bool nonblocking = false);

	/*!
	 * \brief Tries to place a read (shared) lock on inode
	 * \param start - beginning of locked range
	 * \param end - end of locked range
	 */
	bool readLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
			bool nonblocking = false);

	/*!
	 * \brief Tries to place a write (exclusive) lock on inode
	 * \param start - beginning of locked range
	 * \param end - end of locked range
	 */
	bool writeLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
			bool nonblocking = false);

	/*!
	 * \brief Tries to unlock an inode.
	 * Call to this function should likely be followed by gatherCandidates(),
	 * because there might be locks in the queue available for insertion
	 * after this unlock.
	 * \param start - beginning of unlocked range
	 * \param end - end of unlocked range
	 */
	bool unlock(uint32_t inode, uint64_t start, uint64_t end, Owner owner);


	/*!
	 * Returns a list of locks from pending queue that might be available
	 * after removing a lock from range [start, end).
	 * Candidates are not guaranteed to be suitable for insertion,
	 * it still needs to be checked with a call to fits() function.
	 * This function effectively removes candidates from queue,
	 * so they need to be reinserted after checking if they can be applied.
	 * \param start - beginning of regarded range
	 * \param end - end of regarded range
	 */
	void gatherCandidates(uint32_t inode, uint64_t start, uint64_t end, LockQueue &result);

	void clear();


private:
	FileLocks() : active_locks_(), pending_locks_() {}
	FileLocks(const FileLocks &other) = delete;
	FileLocks(FileLocks &&other) = delete;
	FileLocks &operator=(const FileLocks &) = delete;
	FileLocks &operator=(FileLocks &&) = delete;

	/*!
	 * \brief Enqueues a lock
	 */
	void enqueue(uint32_t inode, Lock lock);

	std::unordered_map<uint32_t, Locks> active_locks_;
	std::unordered_map<uint32_t, LockQueue> pending_locks_;
};
