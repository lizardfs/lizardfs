/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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

// A queue which transparently deduplicates inserted elements

#include "common/platform.h"

#include <mutex>
#include <queue>
#include <set>

#include "common/exception.h"

class UniqueQueueEmptyException : public Exception {
public:
	UniqueQueueEmptyException() : Exception("UniqueQueue::get(): empty queue") {};
};

template <class T>
class UniqueQueue {
public:
	void put(T element) {
		std::unique_lock<std::mutex> lock(mutex_);
		std::pair<typename Set::iterator, bool> inserted = set_.insert(element);
		if (inserted.second) {         // new, unique element
			queue_.push(inserted.first);
		}
	}
	T get() {
		std::unique_lock<std::mutex> lock(mutex_);
		if (queue_.empty()) {
			throw UniqueQueueEmptyException();
		} else {
			typename Set::iterator iterator = queue_.front();
			T element = std::move(*iterator);
			queue_.pop();
			set_.erase(iterator);
			return element;
		}
	}
private:
	typedef std::set<T> Set;
	typedef std::queue<typename Set::iterator> Queue;
	Set set_;
	Queue queue_;
	std::mutex mutex_;
};
