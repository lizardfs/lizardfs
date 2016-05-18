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

#include "common/slogger.h"

bool LockRanges::fits(const LockRange &range) const {
	return findCollision(range) == nullptr;
}

const LockRange *LockRanges::findCollision(const LockRange &range) const {
	if (range.unlocking()) {
		return nullptr;
	}

	auto start = ::std::lower_bound(data_.begin(), data_.end(), range.start,
			[](const LockRange &other, uint64_t offset) {return other.start < offset;});
	auto end = ::std::upper_bound(data_.begin(), data_.end(), range.end,
			[](uint64_t offset, const LockRange &other) {return offset < other.start;});

	if (start != data_.begin()) {
		start--;
	}

	for (auto it = start; it != end; ++it) {
		const LockRange &other = *it;
		if (range.overlaps(other)) {
			// Range overlaps, is not shared and owner is not the same
			if ((!range.shared() || !other.shared()) && other.owners != range.owners) {
				return std::addressof(other);
			}
		}
	}

	return nullptr;
}

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

		// There are no more ranges that can overlap - break
		if (range.end <= other.start) {
			break;
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
			range.start = other.start;
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

LockRanges::Container::iterator LockRanges::insert(const Container::iterator &it, const LockRange &range,
		Container::iterator &begin, Container::iterator &end) {
	int dist_begin = std::distance(begin, it);
	int dist_end = std::distance(it, end);
	auto ret = data_.insert(it, range);
	begin = ret - dist_begin;
	end = ret + dist_end + 1;
	return ret;
}

LockRanges::Container::iterator LockRanges::insert(const Container::iterator &it, LockRange &&range,
		Container::iterator &begin, Container::iterator &end) {
	int dist_begin = std::distance(begin, it);
	int dist_end = std::distance(it, end);
	auto ret = data_.insert(it, std::move(range));
	begin = ret - dist_begin;
	end = ret + dist_end + 1;
	return ret;
}

void LockRanges::clear() {
	data_.clear();
}

bool FileLocks::sharedLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
		bool nonblocking) {
	return apply(inode, Lock{Lock::Type::kShared, start, end, owner}, nonblocking);
}

bool FileLocks::exclusiveLock(uint32_t inode, uint64_t start, uint64_t end, Owner owner,
		bool nonblocking) {
	return apply(inode, Lock{Lock::Type::kExclusive, start, end, owner}, nonblocking);
}

const FileLocks::Lock *FileLocks::findCollision(uint32_t inode, Lock::Type type, uint64_t start,
		uint64_t end, Owner owner) {
	auto it = active_locks_.find(inode);
	if (it == active_locks_.end()) {
		return nullptr;
	}

	return it->second.findCollision(Lock{type, start, end, owner});
}

bool FileLocks::unlock(uint32_t inode, uint64_t start, uint64_t end, Owner owner) {
	return apply(inode, Lock{Lock::Type::kUnlock, start, end, owner});
}

void FileLocks::unlock(uint32_t inode) {
	active_locks_.erase(inode);
}

bool FileLocks::apply(uint32_t inode, Lock lock, bool nonblocking) {
	auto it = active_locks_.find(inode);
	if (it == active_locks_.end()) {
		it = active_locks_.insert(std::make_pair(inode, Locks())).first;
	}
	Locks &locks = it->second;

	if (locks.fits(lock)) {
		locks.insert(lock);
		// If last lock was unlocked, inode info can be removed from structure
		if (locks.size() == 0) {
			active_locks_.erase(it);
		}
		return true;
	}

	if (!nonblocking && !lock.unlocking()) {
		enqueue(inode, lock);
	}
	return false;
}

void FileLocks::enqueue(uint32_t inode, Lock lock) {
	LockQueue &queue = pending_locks_[inode];

	queue.insert(std::lower_bound(queue.begin(), queue.end(), lock), lock);
}

void FileLocks::gatherCandidates(uint32_t inode, uint64_t start, uint64_t end, LockQueue &result) {
	auto it = pending_locks_.find(inode);
	if (it == pending_locks_.end()) {
		return;
	}
	LockQueue &queue = it->second;

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

template <typename Container>
void copyToVector(const Container &full_data, int64_t index, int64_t count,
		std::vector<lzfs_locks::Info> &data) {
	int64_t pos = 0; // current index (increased only until pos < index)

	data.clear();

	if (count <= 0) {
		return;
	}

	for (const auto &entry : full_data) {
		for (const auto &lock : entry.second) {
			// each owner is treated as distinct entry in output
			if ((pos + lock.owners.size()) <= index) {
				pos += lock.owners.size();
				continue;
			}

			assert(lock.owners.size() > 0);

			// first we skip to proper owner
			auto iowner = std::next(lock.owners.begin(), std::max(index - pos, (int64_t)0));
			pos += std::max(index - pos, (int64_t)0);
			for (;iowner != lock.owners.end(); ++iowner) {
				data.push_back(
					{0, entry.first, iowner->owner, iowner->sessionid,
					static_cast<uint16_t>(lock.type), lock.start, lock.end
					}
				);
				if (--count <= 0) {
					return;
				}
			};
		}
	}
}

template <typename Container>
void copyInodeToVector(const Container &lock_data, uint32_t inode, int64_t index, int64_t count,
		std::vector<lzfs_locks::Info> &data) {
	int64_t pos = 0; // current index (increased only until pos < index)

	data.clear();

	if (count <= 0) {
		return;
	}

	for (const auto &lock : lock_data) {
		// each owner is treated as distinct entry in output
		if ((pos + lock.owners.size()) <= index) {
			pos += lock.owners.size();
			continue;
		}

		assert(lock.owners.size() > 0);

		// first we skip to proper owner
		auto iowner = std::next(lock.owners.begin(), std::max(index - pos, (int64_t)0));
		pos += std::max(index - pos, (int64_t)0);
		for (;iowner != lock.owners.end(); ++iowner) {
			data.push_back(
				{0, inode, iowner->owner, iowner->sessionid,
				static_cast<uint16_t>(lock.type), lock.start, lock.end
				}
			);
			if (--count <= 0) {
				return;
			}
		};
	}
}

void FileLocks::copyActiveToVector(int64_t index, int64_t count,
		std::vector<lzfs_locks::Info> &data) {
	::copyToVector(active_locks_, index, count, data);
}

void FileLocks::copyPendingToVector(int64_t index, int64_t count,
		std::vector<lzfs_locks::Info> &data) {
	::copyToVector(pending_locks_, index, count, data);
}

void FileLocks::copyActiveToVector(uint32_t inode, int64_t index, int64_t count,
		std::vector<lzfs_locks::Info> &data) {
	auto active_it = active_locks_.find(inode);

	if (active_it == active_locks_.end()) {
		data.clear();
		return;
	}

	::copyInodeToVector(active_it->second, inode, index, count, data);
}

void FileLocks::copyPendingToVector(uint32_t inode, int64_t index, int64_t count,
		std::vector<lzfs_locks::Info> &data) {
	auto pending_it = pending_locks_.find(inode);

	if (pending_it == pending_locks_.end()) {
		data.clear();
		return;
	}

	::copyInodeToVector(pending_it->second, inode, index, count, data);
}

template <typename Inserter>
void load(FILE *file, Inserter insert) {
	static std::vector<uint8_t> buffer;
	uint64_t count;
	uint32_t size;

	size = sizeof(count);
	buffer.resize(size);
	if (fread(buffer.data(), 1, size, file) != size) {
		throw Exception("fread error (size)");
	}
	deserialize(buffer, count);

	size = serializedSize(lzfs_locks::Info());
	for (uint64_t i = 0; i < count; ++i) {
		lzfs_locks::Info info;

		buffer.resize(size);
		if (fread(buffer.data(), 1, size, file) != size) {
			throw Exception("fread error (size)");
		}

		deserialize(buffer, info);

		FileLocks::Lock lock{static_cast<FileLocks::Lock::Type>(info.type), info.start, info.end,
		                     FileLocks::Owner{info.owner, info.sessionid, 0, 0}};

		insert(info.inode, lock);
	}
}

template <typename Container>
void store(FILE *file, const Container &data) {
	std::vector<uint8_t> buffer;

	uint64_t count = 0;
	for (const auto &entry : data) {
		for (const auto &lock : entry.second) {
			count += lock.owners.size();
		}
	}

	buffer.clear();
	serialize(buffer, count);
	if (fwrite(buffer.data(), 1, buffer.size(), file) != buffer.size()) {
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
	}

	for (const auto &entry : data) {
		for (const auto &lock : entry.second) {
			for (const auto &owner : lock.owners) {
				lzfs_locks::Info info = {0, entry.first, owner.owner, owner.sessionid,
				                         static_cast<uint16_t>(lock.type), lock.start, lock.end};

				buffer.clear();
				serialize(buffer, info);
				if (fwrite(buffer.data(), 1, buffer.size(), file) != buffer.size()) {
					lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
				}
			}
		}
	}
}

void FileLocks::load(FILE *file) {
	::load(file, [this](uint32_t inode, Lock &lock) { active_locks_[inode].insert(lock); });
	::load(file, [this](uint32_t inode, Lock &lock) { pending_locks_[inode].push_back(lock); });
}

void FileLocks::store(FILE *file) {
	::store(file, active_locks_);
	::store(file, pending_locks_);
}
