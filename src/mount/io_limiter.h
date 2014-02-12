#pragma once

#include <condition_variable>
#include <map>
#include <set>
#include <string>
#include <thread>

#include "mount/limit_queue.h"

class IoLimitQueue {
public:
	IoLimitQueue(uint32_t kilobytesPerSecond);

	/*
	 * Sets an I/O limit for a queue
	 */
	void setLimit(uint32_t kilobytesPerSecond);

	/*
	 * Blocks a thread in a queue until it is allowed to transfer declared amount of data
	 */
	void wait(uint64_t bytesToTransfer);

	/*
	 * Notifies the queue that a given number of nanosecods passed
	 */
	void pass(uint32_t nanoseconds);

protected:
	uint32_t kilobytesPerSecond_;
	uint64_t nextRequestId_;
	LimitQueue queue_;
	std::set<uint64_t> completedRequests_;
	std::mutex mutex_;
	std::condition_variable cond_;

	/*
	 * The same as pass, but requires caller to hold lock on mutex_
	 */
	void pass(uint32_t nanoseconds, const std::unique_lock<std::mutex>&);
};

class IoLimiter {
public:
	IoLimiter();
	~IoLimiter();

	/*
	 * Starts a thread managing the queue
	 */
	void init();

	/*
	 * Returns a queue creating it if necessary
	 */
	IoLimitQueue& getQueue(const std::string& name);

	/*
	 * Terminates the thread managing the queue
	 */
	void terminate();

	/*
	 * Wakes up thread managing the queue
	 */
	void notify();

	/*
	 * Function for a thread managing the queue
	 */
	void operator()();

protected:
	bool terminate_;
	std::map<std::string, std::unique_ptr<IoLimitQueue>> queues_;
	std::thread thread_;
	std::mutex mutex_;
	std::condition_variable cond_;

	void pass(uint32_t nanoseconds);
};

extern IoLimiter gIoLimiter;
