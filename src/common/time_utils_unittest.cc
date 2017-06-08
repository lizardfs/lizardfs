/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include "common/platform.h"
#include "common/time_utils.h"

#include <unistd.h>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

// without at least millisecond precision a timesource is pretty much useless for us
TEST(TimeUtilsTests, SteadyClockResolutionAndArithmetic) {
	const SteadyTimePoint now = SteadyClock::now();
	const SteadyDuration dur =
		std::chrono::duration_cast<SteadyDuration>(std::chrono::milliseconds(123));
	const SteadyTimePoint later = now + dur;
	EXPECT_EQ(dur, later - now);
}

TEST(TimeUtilsTests, TimerAndTimeout) {
	Timer timer;
	Timeout timeout(std::chrono::milliseconds(200));

#if defined(__APPLE__) || defined(__FreeBSD__)
	long long accuracy = 15;
#else
	long long accuracy = 10;
#endif

	std::this_thread::sleep_for(std::chrono::microseconds(50 * 1000 - timer.elapsed_us()));

	EXPECT_EQ(0, timer.elapsed_s());
	EXPECT_NEAR(timer.elapsed_ms(), 50, accuracy);
	EXPECT_NEAR(timer.elapsed_us(), 50 * 1000, accuracy * 1000);
	EXPECT_NEAR(timer.elapsed_ns(), 50 * 1000 * 1000, accuracy * 1000 * 1000);

	EXPECT_EQ(0, timeout.remaining_s());
	EXPECT_NEAR(timeout.remaining_ms(), 150, accuracy);
	EXPECT_NEAR(timeout.remaining_us(), 150 * 1000, accuracy * 1000);
	EXPECT_NEAR(timeout.remaining_ns(), 150 * 1000 * 1000, accuracy * 1000 * 1000);
	EXPECT_FALSE(timeout.expired());

	std::this_thread::sleep_for(std::chrono::microseconds(210 * 1000 - timer.elapsed_us()));

	EXPECT_EQ(0, timer.elapsed_s());
	EXPECT_NEAR(timer.elapsed_ms(), 210, accuracy);
	EXPECT_NEAR(timer.elapsed_us(), 210 * 1000, accuracy * 1000);
	EXPECT_NEAR(timer.elapsed_ns(), 210 * 1000 * 1000, accuracy * 1000 * 1000);

	EXPECT_EQ(0, timeout.remaining_s());
	EXPECT_EQ(0, timeout.remaining_ms());
	EXPECT_EQ(0, timeout.remaining_us());
	EXPECT_EQ(0, timeout.remaining_ns());
	EXPECT_TRUE(timeout.expired());
}
