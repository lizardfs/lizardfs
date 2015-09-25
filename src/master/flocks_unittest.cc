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

#include <gtest/gtest.h>

#include "master/locks.h"

namespace {
	// Users who will try to apply locks to files
	enum {
		USER0,
		USER1,
		USER2,
		USER3
	};
}

bool wlock(FileLocks &locks, uint32_t inode, uint64_t owner, bool nonblocking = false) {
	return locks.exclusiveLock(inode, 0, 1, {owner, 0, 0, 0}, nonblocking);
}

bool rlock(FileLocks &locks, uint32_t inode, uint64_t owner, bool nonblocking = false) {
	return locks.sharedLock(inode, 0, 1, {owner, 0, 0, 0}, nonblocking);
}

bool ulock(FileLocks &locks, uint32_t inode, uint64_t owner) {
	return locks.unlock(inode, 0, 1, {owner, 0, 0, 0});
}

void gather(FileLocks &locks, uint32_t inode, FileLocks::LockQueue &queue) {
	locks.gatherCandidates(inode, 0, 1, queue);
}

void flush(FileLocks &locks, uint32_t inode, FileLocks::LockQueue &queue) {
	for (auto &candidate : queue) {
		locks.apply(inode, candidate);
	}

	queue.clear();
}

TEST(FlocksTest, SharedAndExclusive) {
	FileLocks locks;
	FileLocks::LockQueue queue;

	// Three read locks are held
	EXPECT_TRUE(rlock(locks, 0, USER0));
	gather(locks, 0, queue);
	flush(locks, 0, queue);
	EXPECT_TRUE(rlock(locks, 0, USER1));
	gather(locks, 0, queue);
	flush(locks, 0, queue);
	EXPECT_TRUE(rlock(locks, 0, USER2));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// Overwriting read lock with read lock is allowed
	EXPECT_TRUE(rlock(locks, 0, USER2));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// Two locks are released
	EXPECT_TRUE(ulock(locks, 0, USER0));
	gather(locks, 0, queue);
	flush(locks, 0, queue);
	EXPECT_TRUE(ulock(locks, 0, USER1));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// Write lock is still unavailable - it is put into the queue
	// Queue status: [W3]
	EXPECT_FALSE(wlock(locks, 0, USER3));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// This lock will result in overwriting readlock of user 2
	EXPECT_TRUE(wlock(locks, 0, USER2));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// This overwrite will fail, because write lock of user 2 exists
	EXPECT_FALSE(wlock(locks, 0, USER3));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// User 3 can release a lock, thus applying pending write lock for user 3
	// Queue status: []
	EXPECT_TRUE(ulock(locks, 0, USER2));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// Read locks will be queued - a write lock is held by user 3
	// Queue status: [R0, R1]
	EXPECT_FALSE(rlock(locks, 0, USER0));
	gather(locks, 0, queue);
	flush(locks, 0, queue);
	EXPECT_FALSE(rlock(locks, 0, USER1));
	gather(locks, 0, queue);
	flush(locks, 0, queue);

	// User 3 holds an only write lock now, so he can overwrite it to read lock
	// Queue status: [R0, R1]
	EXPECT_TRUE(rlock(locks, 0, USER3));

	// Read locks belonging to user 1 and user 2 should be ready for insertion by now
	gather(locks, 0, queue);
	EXPECT_EQ(queue.size(), static_cast<size_t>(2));
	flush(locks, 0, queue);

}

TEST(FlocksTest, Nonblocking) {
	FileLocks locks;
	FileLocks::LockQueue queue;
	const bool nonblocking = true;

	// Ensure that no locks are enqueued in case of nonblocking flocks
	EXPECT_TRUE(rlock(locks, 0, USER0));

	EXPECT_FALSE(wlock(locks, 0, USER1, nonblocking));

	EXPECT_FALSE(wlock(locks, 0, USER2));

	EXPECT_FALSE(wlock(locks, 0, USER3, nonblocking));

	// Only 1 lock should be ready for insertion - the blocking one from user 2
	EXPECT_TRUE(ulock(locks, 0, USER0));
	gather(locks, 0, queue);

	EXPECT_EQ(queue.size(), static_cast<FileLocks::LockQueue::size_type>(1));
}
