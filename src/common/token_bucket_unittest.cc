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
#include "common/token_bucket.h"

#include <unistd.h>

#include <gtest/gtest.h>

namespace {

struct TestCase {
	double time;
	double request;
	double result;
};

SteadyTimePoint timepointInc(SteadyTimePoint tp, double seconds) {
	return tp + std::chrono::nanoseconds(int64_t(seconds * 1000 * 1000 * 1000));
}

void doTests(TokenBucket& tb, SteadyTimePoint t0, const std::vector<TestCase> cases) {
	for (const auto& tc : cases) {
		EXPECT_EQ(tc.result, tb.attempt(timepointInc(t0, tc.time), tc.request));
	}
}

void tester(double rate, double ceil, const std::vector<TestCase> cases) {
	SteadyTimePoint t0;
	TokenBucket tb(t0);
	tb.reconfigure(t0, rate, ceil);
	doTests(tb, t0, cases);
}

} // anonymous namespace

TEST(TokenBucketTests, GetNothingAtTheBeginning) {
	tester(10, 5, {
			{0, 123, 0}
		});
}

TEST(TokenBucketTests, GetLessThenAskedAfterWaitingForShortTime) {
	tester(10, 5, {
			{.1, 2, 1}
		});
}

TEST(TokenBucketTests,  GetLessThenAskedAfterHittingCeil) {
	// Get less then asked if waited too short:
	tester(10, 5, {
			{1, 10, 5}
		});
	// Get as much as asked if waited sufficiently long:
	tester(10, 5, {
			{2, 10, 5}
		});
}

TEST(TokenBucketTests, ManyAttemptsAccumulateProperly) {
	tester(10, 5, {
			{.1, 1, 1},
			{.2, 1, 1},
			{.25, 1, .5},
			{.25, 1, 0}
		});
}

TEST(TokenBucketTests, ReconfigurationChangingRate) {
	SteadyTimePoint t0;
	TokenBucket tb(t0);
	tb.reconfigure(t0, 1, 10);
	doTests(tb, t0, {{.5, 10, .5}});
	tb.reconfigure(timepointInc(t0, 1), 2, 10);
	doTests(tb, t0, {{2, 10, 2.5}});
}

TEST(TokenBucketTests, ReconfigurationReducingCeil) {
	SteadyTimePoint t0;
	TokenBucket tb(t0);
	tb.reconfigure(t0, 1, 10);
	tb.reconfigure(timepointInc(t0, 20), 2, 5);
	doTests(tb, t0, {{20, 10, 5}});
}

TEST(TokenBucketTests, ClockSteadiness) {
	SteadyTimePoint t0;
	TokenBucket tb(t0);
	ASSERT_NO_THROW(tb.reconfigure(t0, 10, 10));
	ASSERT_ANY_THROW(tb.reconfigure(timepointInc(t0, -1), 10, 10));
}
