#include "common/platform.h"
#include "common/goal.h"

#include <gtest/gtest.h>

TEST(GoalTests, IsGoalValid) {
	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		if (goal >= goal::kMinGoal && goal <= goal::kMaxGoal) {
			EXPECT_TRUE(goal::isGoalValid(goal));
		} else {
			EXPECT_FALSE(goal::isGoalValid(goal));
		}
	}
}
