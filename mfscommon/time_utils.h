#ifndef LIZARDFS_MFSCOMMON_TIME_UTILS_H_
#define LIZARDFS_MFSCOMMON_TIME_UTILS_H_

#include <chrono>
#include <cstdint>

// Detect compilers that don't support std::chrono::steady_clock.
//
#if defined(__GNUC__) && defined(__GNUC_MINOR__)
#  if (__GNUC__ == 4 && __GNUC_MINOR__ <= 6)
#    define LIZARDFS_TIME_UTILS_GCC_MONOTONIC_FIX
#  endif
#endif

// SteadyClock is an alias for std::chrono::steady_clock or compatible class.
// SteadyTimePoint is a matching time_point, SteadyDuration - matching duration.
//
#ifdef LIZARDFS_TIME_UTILS_GCC_MONOTONIC_FIX
// GCC <= 4.6 calls it "monotonic_clock"
typedef std::chrono::monotonic_clock SteadyClock;
#else
// Default implementation, use std::chrono::steady_clock
typedef std::chrono::steady_clock SteadyClock;
#endif
typedef std::chrono::time_point<SteadyClock> SteadyTimePoint;
typedef SteadyClock::duration SteadyDuration;

// Measures time from creation or last call to reset()
//
class Timer {
public:
	Timer();
	void reset();
	SteadyDuration elapsedTime() const;
	int64_t elapsed_ns() const;
	int64_t elapsed_us() const;
	int64_t elapsed_ms() const;
	int64_t elapsed_s() const;
private:
	SteadyTimePoint now() const;
	SteadyTimePoint startTime_;
};

// Measures time from creation or reset, "expires" after predefined time.
//
class Timeout : public Timer {
public:
	Timeout(std::chrono::nanoseconds);
	SteadyDuration remainingTime() const;
	int64_t remaining_ns() const;
	int64_t remaining_us() const;
	int64_t remaining_ms() const;
	int64_t remaining_s() const;
	bool expired() const;
private:
	SteadyDuration timeout_;
};

#endif /* LIZARDFS_MFSCOMMON_TIME_UTILS_H_ */
