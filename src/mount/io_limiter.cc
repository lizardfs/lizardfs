#include "mount/io_limiter.h"

#include <unistd.h>

#include "common/massert.h"
#include "common/time_utils.h"

IoLimitQueue::IoLimitQueue()
		: kilobytesPerSecond_(0),
		  maxBytesInReserve_(0),
		  bytesInReserve_(0) {
}

void IoLimitQueue::setLimit(uint32_t kilobytesPerSecond) {
	std::unique_lock<std::mutex> lock(mutex_);
	sassert(kilobytesPerSecond > 0);
	kilobytesPerSecond_ = kilobytesPerSecond;
	maxBytesInReserve_ = static_cast<int64_t>(kilobytesPerSecond) * 1024 / 5;
	updateReserve();
}

void IoLimitQueue::wait(uint64_t bytesToTransfer) {
	std::unique_lock<std::mutex> lock(mutex_);
	updateReserve();
	bytesInReserve_ -= bytesToTransfer;
	if (bytesInReserve_ < 0) {
		// Limit exceeded, we have to wait
		uint64_t missingBytes = -bytesInReserve_;
		lock.unlock();
		double seconds = missingBytes / 1024.0 / kilobytesPerSecond_;
		usleep(seconds * 1000000);
	}
}

void IoLimitQueue::updateReserve() {
	uint64_t nanoseconds = timer_.lap_ns();
	int64_t bytes = static_cast<int64_t>(kilobytesPerSecond_) * nanoseconds / 125 * 128 / 1000000;
	bytesInReserve_ = std::min(bytesInReserve_ + bytes, maxBytesInReserve_);
}

IoLimitQueue& IoLimitQueueCollection::getQueue(const std::string& name) {
	std::unique_lock<std::mutex> lock(mutex_);
	if (queues_.count(name) == 0) {
		throw WrongIoLimitQueueException("No such queue: " + name);
	} else {
		return *queues_[name];
	}
}

void IoLimitQueueCollection::createQueue(const std::string& name, uint32_t kilobytesPerSecond) {
	std::unique_lock<std::mutex> lock(mutex_);
	if (queues_.count(name) != 0) {
		throw WrongIoLimitQueueException("Queue already exists: " + name);
	}
	std::unique_ptr<IoLimitQueue> queue(new IoLimitQueue());
	queue->setLimit(kilobytesPerSecond);
	queues_[name] = std::move(queue);
}

void IoLimiter::readConfiguration(const std::string& filename) {
	// TODO(msulikowski) finish in the future
}

void IoLimiter::waitForRead(pid_t pid, uint64_t bytes) {
	(void)pid;
	(void)bytes;
	// TODO(msulikowski) finish in the future
}

void IoLimiter::waitForWrite(pid_t pid, uint64_t bytes) {
	(void)pid;
	(void)bytes;
	// TODO(msulikowski) finish in the future
}
