#pragma once

// A queue which transparently deduplicates inserted elements

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
