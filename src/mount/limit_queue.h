#pragma once

#include <vector>
#include <queue>
#include <chrono>

class LimitQueue {
public:
	typedef uint64_t Id;

	LimitQueue(uint64_t beginReserve, uint64_t maxReserve);
	void push(Id id, uint64_t requestSize);
	std::vector<Id> pop(uint64_t capacity);
	void setMaxReserve(uint64_t maxReserve) { maxReserve_ = maxReserve; }

private:
	struct QueueEntry {
		QueueEntry(Id id, uint64_t requestSize)
				: id(id),
				  requestSize(requestSize) {
		}
		Id id;
		uint64_t requestSize;
	};

	uint64_t maxReserve_;
	uint64_t reserve_;
	std::queue<QueueEntry> queue_;
};
