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
#include "common/shared_mutex.h"

#include <unistd.h>
#include <atomic>
#include <list>
#include <thread>

#include <gtest/gtest.h>

TEST(SharedMutex, LockTest) {
	shared_mutex m;
	std::atomic_int shared_count{0};
	int exclusive_count = 0;
	std::list<std::thread> threads;

	for(int i = 0; i < 100; ++i) {
		threads.emplace_back([&m, &shared_count, &exclusive_count]() {
			std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100000));
			shared_lock<shared_mutex> guard(m);
			shared_count++;
			EXPECT_TRUE(shared_count > 0);
			EXPECT_EQ(exclusive_count, 0);
			std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100000));
			shared_count--;
		});
	}
	for(int i = 0; i < 10; ++i) {
		threads.emplace_back([&m, &shared_count, &exclusive_count]() {
			std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100000));
			std::unique_lock<shared_mutex> guard(m);
			exclusive_count++;
			EXPECT_EQ(shared_count, 0);
			EXPECT_EQ(exclusive_count, 1);
			std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100000));
			exclusive_count--;
		});
	}

	for(auto &th : threads) {
		th.join();
	}
}
