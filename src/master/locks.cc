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

#include "common/platform.h"
#include "master/locks.h"
#include <algorithm>
#include <functional>

bool LockRanges::fits(const LockRange &range) const {
	auto start = std::lower_bound(data_.begin(), data_.end(), range.start,
			[](const LockRange &other, uint64_t offset) {return other.start < offset;});
	auto end = std::upper_bound(data_.begin(), data_.end(), range.end,
			[](uint64_t offset, const LockRange &other) {return offset < other.start;});

	if (start != data_.begin()) {
		start--;
	}

	for (auto it = start; it != end; ++it) {
		const LockRange &other = *it;
		if (range.overlaps(other)) {
			// Range is to be removed
			if (range.unlocking() && other.hasOwner(range.owner())) {
				continue;
			}
			// Range overlaps, is not shared and owner is not the same
			if ((!range.shared() || !other.shared()) && other.owners != range.owners) {
				return false;
			}
		}
	}
	return true;
}

/*!
 *  \brief Inserts range into the structure
 *  Assumes that range is suitable for insertion (fits() function returned true)
 */
void LockRanges::insert(LockRange &range) {
	auto start = ::std::lower_bound(data_.begin(), data_.end(), range.start,
			[](const LockRange &other, uint64_t offset) {return other.start < offset;});
	auto end = ::std::upper_bound(data_.begin(), data_.end(), range.end,
			[](uint64_t offset, const LockRange &other) {return offset < other.start;});

	if (start != data_.begin()) {
		start--;
	}

	auto it = start;
	for (; it != end && range.end > range.start; ++it) {
		LockRange &other = *it;

		bool same_owners = range.owners == other.owners;

		// Ranges are adjacent, so they can be merged
		if (range.sticks(other) && same_owners && range.type == other.type) {
			range.start = std::min(range.start, other.start);
			range.end = std::max(range.end, other.end);
			other.markUnlocking();
			continue;
		}

		// Ranges do not overlap, no need for collision check
		if (!range.overlaps(other)) {
			continue;
		}

		// If owners are different, the only valid situation is that
		// both ranges are shared or one of them is removing
		assert(same_owners ||
		 (range.shared() && other.shared()) || range.unlocking());

		if (range.start < other.start) {
			LockRange tmp = range;
			tmp.end = other.start;
			range.start = other.end;
			it = insert(it, std::move(tmp), start, end);
		} else if (range.start == other.start) {
			if (range.end < other.end) {
				LockRange tmp = other;
				tmp.end = range.end;
				other.start = range.end;
				range.start = other.end;
				if (same_owners) {
					tmp.type = range.type;
				} else {
					if (range.unlocking()) {
						tmp.eraseOwner(range.owner());
					} else {
						tmp.addOwners(range.owners);
					}
				}
				it = insert(it, std::move(tmp), start, end);
			} else { // range.end >= other.end
				if (same_owners) {
					other.type = range.type;
				} else {
					if (range.unlocking()) {
						other.eraseOwner(range.owner());
					} else {
						other.addOwners(range.owners);
					}
				}
				range.start = other.end;
			}
		} else { // range.start > other.start
			LockRange tmp = other;
			tmp.end = range.start;
			other.start = range.start;
			it = insert(it, std::move(tmp), start, end);
		}
	}

	// If anything from the range is left, insert it
	if (range.end > range.start) {
		it = insert(it, range, start, end);
	}

	// Remove all leftover ranges
	data_.erase(std::remove_if(
			start,
			end,
			std::bind(&LockRange::unlocking, std::placeholders::_1)),
			end
		);
}

LockRanges::Data::iterator LockRanges::insert(const Data::iterator &it, const LockRange &range,
		Data::iterator &begin, Data::iterator &end) {
	int dist_begin = std::distance(begin, it);
	int dist_end = std::distance(it, end);
	auto ret = data_.insert(it, range);
	begin = ret - dist_begin;
	end = ret + dist_end + 1;
	return ret;
}

LockRanges::Data::iterator LockRanges::insert(const Data::iterator &it, LockRange &&range,
		Data::iterator &begin, Data::iterator &end) {
	int dist_begin = std::distance(begin, it);
	int dist_end = std::distance(it, end);
	auto ret = data_.insert(it, std::move(range));
	begin = ret - dist_begin;
	end = ret + dist_end + 1;
	return ret;
}

bool FileLocks::readLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
		bool nonblocking) {
	return apply(inode, Lock{Lock::Type::kShared, start, end, owner}, nonblocking);
}

bool FileLocks::writeLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
		bool nonblocking) {
	return apply(inode, Lock{Lock::Type::kExclusive, start, end, owner}, nonblocking);
}

bool FileLocks::unlock(uint32_t inode, uint64_t start, uint64_t end, Owner owner) {
	return apply(inode, Lock{Lock::Type::kUnlock, start, end, owner});
}

bool FileLocks::apply(uint32_t inode, Lock lock, bool nonblocking) {
	Locks &locks = active_locks_[inode];

	if (locks.fits(lock)) {
		locks.insert(lock);
		return true;
	}

	if (!nonblocking) {
		enqueue(inode, lock);
	}
	return false;
}

void FileLocks::enqueue(uint32_t inode, Lock lock) {
	LockQueue &queue = pending_locks_[inode];

	queue.insert(std::lower_bound(queue.begin(), queue.end(), lock), lock);
}

void FileLocks::gatherCandidates(uint32_t inode, uint64_t start, uint64_t end, LockQueue &result) {
	LockQueue &queue = pending_locks_[inode];

	auto first = ::std::lower_bound(queue.begin(), queue.end(), start,
			[](const Lock &lock, uint64_t offset) {return lock.start < offset;});
	auto last = ::std::upper_bound(queue.begin(), queue.end(), end,
			[](uint64_t offset, const Lock &lock) {return offset < lock.start;});

	if (first != queue.begin()) {
		first--;
	}

	std::move(first, last, std::back_inserter(result));
	queue.erase(first, last);
}

void FileLocks::clear() {
	return active_locks_.clear();
}
