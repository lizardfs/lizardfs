#include "limit_queue.h"

#include "common/massert.h"

LimitQueue::LimitQueue(uint64_t beginReserve, uint64_t maxReserve)
		: maxReserve_(maxReserve),
		  reserve_(beginReserve) {
	sassert(reserve_ <= maxReserve_);
}

void LimitQueue::push(Id id, uint64_t requestSize) {
	queue_.push(QueueEntry(id, requestSize));
}

std::vector<LimitQueue::Id> LimitQueue::pop(uint64_t capacity) {
	capacity += reserve_;
	std::vector<Id> result;
	while (!queue_.empty()) {
		QueueEntry& request = queue_.front();
		if (capacity >= request.requestSize) {
			capacity -= request.requestSize;
			result.push_back(request.id);
			queue_.pop();
		} else {
			request.requestSize -= capacity;
			capacity = 0;
			break;
		}
	}
	reserve_ = std::min(maxReserve_, capacity);
	return result;
}
