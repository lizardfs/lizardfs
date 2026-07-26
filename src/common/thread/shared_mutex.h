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

#include <condition_variable>
#include <mutex>

class shared_mutex {
public:
	shared_mutex() : shared_count_(0), exclusive_waiting_(0), exclusive_(false) {
	}
	~shared_mutex() {
	}

	void lock() {
		std::unique_lock<std::mutex> lock(state_change_);

		if (shared_count_ == 0 && !exclusive_) {
			exclusive_ = true;
			return;
		}

		++exclusive_waiting_;
		while (shared_count_ > 0 || exclusive_) {
			exclusive_cond_.wait(lock);
		}
		--exclusive_waiting_;
		exclusive_ = true;
	}

	void unlock() {
		std::unique_lock<std::mutex> lock(state_change_);

		exclusive_ = false;
		if (exclusive_waiting_ > 0) {
			exclusive_cond_.notify_one();
			return;
		}

		shared_cond_.notify_all();
	}

	void lock_shared() {
		std::unique_lock<std::mutex> lock(state_change_);

		while (exclusive_ || exclusive_waiting_ > 0) {
			shared_cond_.wait(lock);
		}
		shared_count_++;
	}

	void unlock_shared() {
		std::unique_lock<std::mutex> lock(state_change_);
		shared_count_--;

		if (shared_count_ == 0 && exclusive_waiting_ > 0) {
			exclusive_cond_.notify_one();
		}
	}

protected:
	std::mutex state_change_;
	std::condition_variable shared_cond_;
	std::condition_variable exclusive_cond_;
	int shared_count_;
	int exclusive_waiting_;
	bool exclusive_;
};

template <typename SharedMutex>
class shared_lock {
public:
	shared_lock(SharedMutex &m) : mutex_(m), locked_(false) {
		lock();
	}

	~shared_lock() {
		unlock();
	}

	void unlock() {
		if (!locked_) {
			return;
		}
		mutex_.unlock_shared();
		locked_ = false;
	}

	void lock() {
		if (locked_) {
			return;
		}

		mutex_.lock_shared();
		locked_ = true;
	}

protected:
	SharedMutex &mutex_;
	bool locked_;
};
