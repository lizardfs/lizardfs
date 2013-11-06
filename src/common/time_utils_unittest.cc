#include "common/time_utils.h"

#include <chrono>
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

	usleep(50*1000);

	EXPECT_EQ(0, timer.elapsed_s());
	EXPECT_NEAR(timer.elapsed_ms(), 50,           10);
	EXPECT_NEAR(timer.elapsed_us(), 50*1000,      10*1000);
	EXPECT_NEAR(timer.elapsed_ns(), 50*1000*1000, 10*1000*1000);

	EXPECT_EQ(0, timeout.remaining_s());
	EXPECT_NEAR(timeout.remaining_ms(), 150,           10);
	EXPECT_NEAR(timeout.remaining_us(), 150*1000,      10*1000);
	EXPECT_NEAR(timeout.remaining_ns(), 150*1000*1000, 10*1000*1000);
	EXPECT_FALSE(timeout.expired());

	usleep(160*1000);

	EXPECT_EQ(0, timer.elapsed_s());
	EXPECT_NEAR(timer.elapsed_ms(), 210,           10);
	EXPECT_NEAR(timer.elapsed_us(), 210*1000,      10*1000);
	EXPECT_NEAR(timer.elapsed_ns(), 210*1000*1000, 10*1000*1000);

	EXPECT_EQ(0, timeout.remaining_s());
	EXPECT_EQ(0, timeout.remaining_ms());
	EXPECT_EQ(0, timeout.remaining_us());
	EXPECT_EQ(0, timeout.remaining_ns());
	EXPECT_TRUE(timeout.expired());
}
