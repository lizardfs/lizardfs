#include "mount/io_limiter.h"

#include "common/time_utils.h"

static constexpr uint32_t kPartOfSecondInReserve = 5;
IoLimiter gIoLimiter;

IoLimitQueue::IoLimitQueue(uint32_t kilobytesPerSecond)
		: kilobytesPerSecond_(kilobytesPerSecond),
		  nextRequestId_(0),
		  queue_(0, static_cast<uint64_t>(kilobytesPerSecond) * 1024 / kPartOfSecondInReserve) {
}

void IoLimitQueue::setLimit(uint32_t kilobytesPerSecond) {
	std::unique_lock<std::mutex> lock(mutex_);
	kilobytesPerSecond_ = kilobytesPerSecond;
	queue_.setMaxReserve(static_cast<uint64_t>(kilobytesPerSecond) * 1024 / kPartOfSecondInReserve);
}

void IoLimitQueue::wait(uint64_t bytesToTransfer) {
	std::unique_lock<std::mutex> lock(mutex_);
	uint64_t id = nextRequestId_++;
	queue_.push(id, bytesToTransfer);
	pass(0, lock);
	while (completedRequests_.count(id) == 0) {
		cond_.wait(lock);
	}
	completedRequests_.erase(id);
}

void IoLimitQueue::pass(uint32_t nanoseconds) {
	std::unique_lock<std::mutex> lock(mutex_);
	pass(nanoseconds, lock);
}

void IoLimitQueue::pass(uint32_t nanoseconds, const std::unique_lock<std::mutex>&) {
	uint64_t bytes = static_cast<uint64_t>(kilobytesPerSecond_) * nanoseconds / 125 * 128 / 1000000;
	auto newCompletedRequests_ = queue_.pop(bytes);
	for (auto request : newCompletedRequests_) {
		completedRequests_.insert(request);
	}
	if (!completedRequests_.empty()) {
		cond_.notify_all();
	}
}

IoLimiter::IoLimiter() : terminate_(false) {}

IoLimiter::~IoLimiter() {
	try {
		terminate();
	} catch (...) {
	}
}

void IoLimiter::init() {
	thread_ = std::thread(std::ref(*this));
}

IoLimitQueue& IoLimiter::getQueue(const std::string& name) {
	std::unique_lock<std::mutex> lock(mutex_);
	notify(); // make the managing thread wake up at least as frequent, as clients request data
	try {
		return *queues_.at(name);
	} catch (std::out_of_range&) {
		queues_[name] = std::unique_ptr<IoLimitQueue>(new IoLimitQueue(0));
		return *queues_[name];
	}
}

void IoLimiter::notify() {
	cond_.notify_one();
}

void IoLimiter::terminate() {
	std::unique_lock<std::mutex> lock(mutex_);
	terminate_ = true;
	notify();
	lock.unlock();
	thread_.join();
}

void IoLimiter::operator()() {
	std::unique_lock<std::mutex> lock(mutex_);
	Timer timer;
	while (!terminate_) {
		uint64_t nanoseconds = timer.lap_ns();
		const uint32_t slice = 1 << 30;
		while (nanoseconds > slice) {
			pass(slice);
			nanoseconds -= slice;
		}
		pass(nanoseconds);
		cond_.wait_for(lock, std::chrono::milliseconds(5));
	}
}

void IoLimiter::pass(uint32_t nanoseconds) {
	for (auto& queue : queues_) {
		queue.second->pass(nanoseconds);
	}
}
