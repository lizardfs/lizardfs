#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "common/exception.h"
#include "common/time_utils.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(WrongIoLimitQueueException, Exception);

class IoLimitQueue {
public:
	IoLimitQueue();

	/*
	 * Sets an I/O limit for a queue
	 */
	void setLimit(uint32_t kilobytesPerSecond);

	/*
	 * Blocks a thread in a queue until it is allowed to transfer declared amount of data
	 */
	void wait(uint64_t bytesToTransfer);

private:
	uint32_t kilobytesPerSecond_;
	int64_t maxBytesInReserve_;
	int64_t bytesInReserve_;
	Timer timer_;
	std::mutex mutex_;

	void updateReserve();
};

class IoLimiter {
public:
	/*
	 * Returns a queue or throws a WrongIoLimitQueueException if not exists
	 */
	IoLimitQueue& getQueue(const std::string& name);

	/*
	 * Creates a queue and sets it's speed or throws a WrongIoLimitQueueException if already exists
	 */
	void createQueue(const std::string& name, uint32_t kilobytesPerSecond);

private:
	std::map<std::string, std::unique_ptr<IoLimitQueue>> queues_;
	std::mutex mutex_;
};
