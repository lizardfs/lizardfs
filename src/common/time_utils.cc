#include "common/platform.h"
#include "common/time_utils.h"

#include <chrono>
#include <ratio>

#ifdef LIZARDFS_TIME_UTILS_NO_STD_CHRONO_STEADY_CLOCK
#  include <sys/time.h>
#  include <time.h>
SteadyClock::time_point SteadyClock::now() {
	struct timespec ts;
#  ifdef CLOCK_MONOTONIC
	clock_gettime(CLOCK_MONOTONIC, &ts);
#  else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	ts.tv_sec = tv.tv_sec;
	ts.tv_nsec = tv.tv_usec * 1000;
#  endif
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

SteadyTimePoint Timer::startTime() const {
	return startTime_;
}

SteadyDuration Timer::elapsedTime() const {
	return now() - startTime_;
}

SteadyDuration Timer::lap() {
	SteadyTimePoint t = now();
	SteadyDuration elapsed = t - startTime_;
	startTime_ = t;
	return elapsed;
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

int64_t Timer::lap_ns() {
	return duration_int64_cast<std::nano>(lap());
}

int64_t Timer::lap_us() {
	return duration_int64_cast<std::micro>(lap());
}

int64_t Timer::lap_ms() {
	return duration_int64_cast<std::milli>(lap());
}

int64_t Timer::lap_s() {
	return duration_int64_cast<std::ratio<1>>(lap());
}


// Timeout implementation

Timeout::Timeout(std::chrono::nanoseconds timeout) :
	timeout_(std::chrono::duration_cast<SteadyDuration>(timeout)) {
}

SteadyTimePoint Timeout::deadline() const {
	return startTime() + timeout_;
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
