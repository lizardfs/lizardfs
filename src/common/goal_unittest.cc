#include "common/goal.h"

#include <gtest/gtest.h>

TEST(GoalTests, XorLevelToGoal_GoalToXorLevel) {
	for (int level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
		SCOPED_TRACE("Testing xor level " + std::to_string(level));
		EXPECT_EQ(level, goalToXorLevel(xorLevelToGoal(level)));
	}
}

TEST(GoalTests, XorGoalOrdinaryGoalIntersection) {
	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		EXPECT_FALSE(isXorGoal(goal) && isOrdinaryGoal(goal));
	}
}

TEST(GoalTests, IsXorGoal) {
	std::set<uint8_t> xalidXorGoals;
	for (int level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
		uint8_t goal = xorLevelToGoal(level);
		EXPECT_EQ(0U, xalidXorGoals.count(goal));
		xalidXorGoals.insert(goal);
	}

	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		if (xalidXorGoals.count(goal)) {
			EXPECT_TRUE(isXorGoal(goal));
		} else {
			EXPECT_FALSE(isXorGoal(goal));
		}
	}
}

TEST(GoalTests, IsOrdinaryGoal) {
	std::set<uint8_t> xalidOrdinaryGoals;
	for (uint8_t goal = kMinOrdinaryGoal; goal <= kMaxOrdinaryGoal; ++goal) {
		xalidOrdinaryGoals.insert(goal);
	}

	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		if (xalidOrdinaryGoals.count(goal)) {
			EXPECT_TRUE(isOrdinaryGoal(goal));
			EXPECT_FALSE(isXorGoal(goal));
		} else {
			EXPECT_FALSE(isOrdinaryGoal(goal));
		}
	}
}

TEST(GoalTests, IsGoalValid) {
	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		if (isXorGoal(goal) || isOrdinaryGoal(goal)) {
			EXPECT_TRUE(isGoalValid(goal));
		} else {
			EXPECT_FALSE(isGoalValid(goal));
		}
	}
}

TEST(GoalTests, XorGoalConstants) {
	for (uint8_t level = kMinXorLevel; level <= kMaxXorLevel; level++) {
		uint8_t goal = xorLevelToGoal(level);
		EXPECT_GE(goal, kMinXorGoal);
		EXPECT_LE(goal, kMaxXorGoal);
	}
}
