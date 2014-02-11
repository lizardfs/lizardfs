#pragma once

#include <chrono>
#include <cstdint>
#include <ratio>


// Detect compilers that don't support std::chrono::steady_clock.

#if defined(__GNUC__) && defined(__GNUC_MINOR__)
#  if (__GNUC__ == 4 && __GNUC_MINOR__ <= 6)
#    define LIZARDFS_TIME_UTILS_NO_STD_CHRONO_STEADY_CLOCK
#  endif
#endif


// SteadyClock is an alias for std::chrono::steady_clock or compatible class.
// SteadyTimePoint is a matching time_point, SteadyDuration - matching duration.

#ifdef LIZARDFS_TIME_UTILS_NO_STD_CHRONO_STEADY_CLOCK

// For platforms known to lack std::chrono::steady_clock support.
class SteadyClock {
public:
	typedef int64_t rep;
	typedef std::nano period;
	typedef std::chrono::duration<rep, period> duration;
	typedef std::chrono::time_point<SteadyClock> time_point;

	static constexpr bool is_steady = true;

	static time_point now();
};

#else

// Assume std::chrono::steady_clock is present.
typedef std::chrono::steady_clock SteadyClock;

#endif

typedef SteadyClock::time_point SteadyTimePoint;
typedef SteadyClock::duration SteadyDuration;

template <class Ratio2, class Dur>
static int64_t duration_int64_cast(Dur duration) {
	return std::chrono::duration_cast<std::chrono::duration<int64_t, Ratio2>>(duration).count();
}

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
