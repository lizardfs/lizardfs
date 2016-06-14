/*
   Copyright 2016 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"
#include "common/time_utils.h"

#include <signal.h>
#include <stdint.h>
#include <sys/time.h>
#include <cassert>
#include <chrono>
#include <stdexcept>

/*!
 * Loop Watchdog classes are used to control execution time of potentially long loops.
 * When loop duration exceeds initially specified maximum time, 'expired' function
 * returns 'true', otherwise it will return 'false'.
 */

constexpr uint64_t kDefaultMaxDuration_us = 500;

/*!
 * Signal LoopWatchdog uses signals to control time duration of the loop.
 */
class SignalLoopWatchdog {
public:
	/*!
	 * Construct SignalLoopWatchdog with maximum loop duration given as parameter.
	 * Default constructor sets its parameter value to 'kDefaultMaxDuration_us',
	 * which value is given in microseconds.
	 * \param max_loop_duration - value which specifies maximum loop duration
	 *                            time, given as std::chrono::duration.
	 */
	template <class Rep = typename std::chrono::microseconds::rep,
	          class Period = typename std::chrono::microseconds::period>
	SignalLoopWatchdog(std::chrono::duration<Rep, Period> max_loop_duration =
	                           std::chrono::microseconds(kDefaultMaxDuration_us))
	    : max_loop_duration_us_(
	              std::chrono::duration_cast<std::chrono::microseconds>(max_loop_duration)
	                      .count()) {
		assert(!refcount_++);
	}

	~SignalLoopWatchdog() {
		assert(!--refcount_);
	}

	/*!
	 * Sets maximum time duration for loop.
	 * \param duration - value which specifies new maximum loop duration time,
	 *                   given as std::chrono::duration.
	 */
	template <class Rep, class Period>
	void setMaxDuration(std::chrono::duration<Rep, Period> duration) {
		max_loop_duration_us_ =
		        std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
	}

	/*!
	 * Starts Watchdog timer to measure loop duration.
	 * Should be called just before entering the loop.
	 */
	void start() {
		exit_loop_ = false;
		struct itimerval val = {{0, 0}, {0, 0}};
		val.it_value.tv_usec = max_loop_duration_us_;
		setitimer(ITIMER_REAL, &val, nullptr);
	}

	/*!
	 * Returns true when loop duration time exceeds specified maximum.
	 * Should be included as a condition in every loop iteration.
	 */
	bool expired() const {
		return exit_loop_;
	}

private:
	static void alarmHandler(int signal);

	static bool initHandler() {
		if (signal(SIGALRM, &SignalLoopWatchdog::alarmHandler) == SIG_ERR) {
			throw std::runtime_error("SIGALRM handler registration failed");
		}
		return true;
	}

	static volatile bool exit_loop_;
	int64_t max_loop_duration_us_;
	static bool kHandlerInitialized;
#ifndef NDEBUG
	static int refcount_;
#endif
};

/*!
 * ActiveLoopWatchdog uses timer to check elapsed time of the loop in
 * every iteration and compares it to maximum loop duration time.
 */
class ActiveLoopWatchdog {
public:
	/*!
	 * Construct ActiveLoopWatchdog with maximum loop duration given as parameter.
	 * Default constructor sets its parameter value to 'kDefaultMaxDuration_us',
	 * which value is given in microseconds.
	 * \param max_loop_duration - value which specifies maximum loop duration
	 *                            time, given as std::chrono::duration.
	 */
	template <class Rep = typename std::chrono::microseconds::rep,
	          class Period = typename std::chrono::microseconds::period>
	ActiveLoopWatchdog(std::chrono::duration<Rep, Period> max_loop_duration =
	                           std::chrono::microseconds(kDefaultMaxDuration_us))
	    : max_loop_duration_us_(
	              std::chrono::duration_cast<std::chrono::microseconds>(max_loop_duration)
	                      .count()),
	      timer_() {
	}

	/*!
	 * Sets maximum time duration for loop.
	 * \param duration - value which specifies new maximum loop duration time,
	 *                   given as std::chrono::duration.
	 */
	template <class Rep, class Period>
	void setMaxDuration(std::chrono::duration<Rep, Period> duration) {
		max_loop_duration_us_ =
		        std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
	}

	/*!
	 * Starts Watchdog timer to measure loop duration.
	 * Should be called just before entering the loop.
	 */
	void start() {
		timer_.reset();
	}

	/*!
	 * Returns true when loop duration time exceeds specified maximum.
	 * Should be included as a condition in every loop iteration.
	 */
	bool expired() const {
		return timer_.elapsed_us() > max_loop_duration_us_;
	}

private:
	int64_t max_loop_duration_us_;
	Timer timer_;
};
