#include "speed_limit_queue.h"

#include "common/massert.h"

const uint64_t SpeedLimitQueue::kMaxAvaliableCapacity = 1 << 30;
const double SpeedLimitQueue::kWriteMultiplier = 1.5;
const double SpeedLimitQueue::kReadMultiplier = 1;

SpeedLimitQueue::SpeedLimitQueue(uint32_t speedLimit)
		: speedLimit_(speedLimit),
		  avaliableCapacity_(speedLimit) {
}

void SpeedLimitQueue::push(pid_t pid, OperationType operation, uint32_t size) {
	switch (operation) {
	case OperationType::kRead:
		size = static_cast<double>(size) * kReadMultiplier;
		break;
	case OperationType::kWrite:
		size = static_cast<double>(size) * kWriteMultiplier;
		break;
	default:
		sassert(false);
		break;
	}
	queue_.push(QueueEntry(pid, size));
}

std::vector<pid_t> SpeedLimitQueue::pop(SteadyDuration duration) {
	if (avaliableCapacity_ < kMaxAvaliableCapacity) {
		uint64_t seconds = duration_int64_cast<std::ratio<1>>(duration);
		if (seconds >= kMaxAvaliableCapacity / static_cast<uint64_t>(speedLimit_)) {
			avaliableCapacity_ = kMaxAvaliableCapacity;
		} else if (seconds == 0) {
			uint64_t nano = duration_int64_cast<std::nano>(duration);
			avaliableCapacity_ = std::min(kMaxAvaliableCapacity,
					avaliableCapacity_ +
					nano * static_cast<uint64_t>(speedLimit_) / (1000000000));
		} else {
			uint64_t milli = duration_int64_cast<std::milli>(duration);
			avaliableCapacity_ = std::min(kMaxAvaliableCapacity,
					avaliableCapacity_ +
					milli * static_cast<uint64_t>(speedLimit_) / (1000));
		}

		if (queue_.empty() || queue_.front().size <= speedLimit_ * 2) {
			avaliableCapacity_ = std::min(static_cast<uint64_t>(speedLimit_) * 2,
					avaliableCapacity_);
		}
	}

	std::vector<pid_t> result;

	while (!queue_.empty() && avaliableCapacity_ >= queue_.front().size) {
		result.push_back(queue_.front().pid);
		if (queue_.front().size <= speedLimit_ * 2) {
			avaliableCapacity_ -= queue_.front().size;
		} else {
			avaliableCapacity_ = 0;
		}
		queue_.pop();
	}
	return result;
}

SpeedLimitQueue::QueueEntry::QueueEntry(pid_t pid, uint32_t size)
		: pid(pid),
		  size(size) {
}
