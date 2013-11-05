#include "common/time_utils.h"

#include <chrono>
#include <ratio>

#ifdef LIZARDFS_TIME_UTILS_NO_STD_CHRONO_STEADY_CLOCK
#include <time.h>
SteadyClock::time_point SteadyClock::now() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	rep count = 0;
	count += ts.tv_sec;
	count *= 1000*1000*1000;
	count += ts.tv_nsec;
	return time_point(duration(count));
}
constexpr bool SteadyClock::is_steady;
#endif

template <class Ratio2, class Dur>
static int64_t duration_int64_cast(Dur duration) {
	return std::chrono::duration_cast<std::chrono::duration<int64_t, Ratio2>>(duration).count();
}

// Timer implementation

Timer::Timer() : startTime_(now()) {
}

SteadyTimePoint Timer::now() const {
	return SteadyClock::now();
}

void Timer::reset() {
	startTime_ = now();
}

SteadyDuration Timer::elapsedTime() const {
	return now() - startTime_;
}

int64_t Timer::elapsed_ns() const {
	return duration_int64_cast<std::nano>(elapsedTime());
}

int64_t Timer::elapsed_us() const {
	return duration_int64_cast<std::micro>(elapsedTime());
}

int64_t Timer::elapsed_ms() const {
	return duration_int64_cast<std::milli>(elapsedTime());
}

int64_t Timer::elapsed_s() const {
	return duration_int64_cast<std::ratio<1>>(elapsedTime());
}

// Timeout implementation

Timeout::Timeout(std::chrono::nanoseconds timeout) :
	timeout_(std::chrono::duration_cast<SteadyDuration>(timeout)) {
}

SteadyDuration Timeout::remainingTime() const {
	SteadyDuration elapsed = elapsedTime();
	if (elapsed >= timeout_) {
		return SteadyDuration(0);
	} else {
		return timeout_ - elapsed;
	}
}

int64_t Timeout::remaining_ns() const {
	return duration_int64_cast<std::nano>(remainingTime());
}

int64_t Timeout::remaining_us() const {
	return duration_int64_cast<std::micro>(remainingTime());
}

int64_t Timeout::remaining_ms() const {
	return duration_int64_cast<std::milli>(remainingTime());
}

int64_t Timeout::remaining_s() const {
	return duration_int64_cast<std::ratio<1>>(remainingTime());
}

bool Timeout::expired() const {
	return remainingTime() == SteadyDuration(0);
}
