#pragma once

#include <sys/types.h>
#include <vector>
#include <queue>

#include "common/time_utils.h"

class SpeedLimitQueue {
public:
	enum OperationType {
		kRead,
		kWrite
	};

	SpeedLimitQueue(uint32_t speedLimit);
	void setSpeedLimit(uint32_t speedLimit) { speedLimit_ = speedLimit; }
	void push(pid_t pid, OperationType operation, uint32_t size);
	std::vector<pid_t> pop(SteadyDuration duration);

private:
	static const uint64_t kMaxAvaliableCapacity;
	static const double kWriteMultiplier;
	static const double kReadMultiplier;

	struct QueueEntry {
		QueueEntry(pid_t pid, uint32_t size);
		pid_t pid;
		uint32_t size;
	};

	uint32_t speedLimit_;          // bytes per second
	uint64_t avaliableCapacity_;   // bytes
	std::queue<QueueEntry> queue_;
};
