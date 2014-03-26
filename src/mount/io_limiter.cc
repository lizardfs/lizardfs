#include "mount/io_limiter.h"

#include <unistd.h>
#include <fstream>

#include "common/io_limits_config_loader.h"
#include "common/massert.h"
#include "common/time_utils.h"
#include "mount/io_limit_group.h"

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
	auto it = queues_.find(name);
	if (it == queues_.end()) {
		throw WrongIoLimitQueueException("No such queue: " + name);
	} else {
		return *(it->second);
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

bool IoLimitQueueCollection::hasQueue(const std::string& name) const {
	return (queues_.count(name) != 0);
}

void IoLimiter::readConfiguration(const std::string& filename) {
	IoLimitsConfigLoader loader;
	loader.load(std::ifstream(filename));
	subsystem_ = loader.subsystem();
	for (const auto& entry : loader.limits()) {
		queues_.createQueue(entry.first, entry.second);
	}
	isEnabled_ = true;
	if (subsystem_ != "") {
		usesCgroups_ = true;
	}
}

void IoLimiter::waitForRead(pid_t pid, uint64_t bytes) {
	if (!isEnabled_) {
		// I/O limits are not used
		return;
	}

	// Find group for a given pid
	std::string group = "unclassified";
	if (usesCgroups_) {
		try {
			group = getIoLimitGroupId(pid, subsystem_);
		} catch (Exception& ex) {
			if (lastErrorTimer.elapsed_s() >= 1) {
				syslog(LOG_WARNING, "I/O group classification error: %s", ex.what());
				lastErrorTimer.reset();
			}
		}
	}

	// Limit I/O if some limit is specified
	if (queues_.hasQueue(group)) {
		queues_.getQueue(group).wait(bytes);
	} else if (queues_.hasQueue("unclassified")) {
		queues_.getQueue("unclassified").wait(bytes);
	}
}

void IoLimiter::waitForWrite(pid_t pid, uint64_t bytes) {
	waitForRead(pid, bytes); // TODO(msulikowski) in the future add some more configuration options
}
