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
#include "common/goal.h"

#include <gtest/gtest.h>

TEST(GoalTests, XorLevelToGoal_GoalToXorLevel) {
	for (int level = goal::kMinXorLevel; level <= goal::kMaxXorLevel; ++level) {
		SCOPED_TRACE("Testing xor level " + std::to_string(level));
		EXPECT_EQ(level, goal::toXorLevel(goal::xorLevelToGoal(level)));
	}
}

TEST(GoalTests, XorGoalOrdinaryGoalIntersection) {
	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		EXPECT_FALSE(goal::isXorGoal(goal) && goal::isOrdinaryGoal(goal));
	}
}

TEST(GoalTests, IsXorGoal) {
	std::set<uint8_t> xalidXorGoals;
	for (int level = goal::kMinXorLevel; level <= goal::kMaxXorLevel; ++level) {
		uint8_t goal = goal::xorLevelToGoal(level);
		EXPECT_EQ(0U, xalidXorGoals.count(goal));
		xalidXorGoals.insert(goal);
	}

	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		if (xalidXorGoals.count(goal)) {
			EXPECT_TRUE(goal::isXorGoal(goal));
		} else {
			EXPECT_FALSE(goal::isXorGoal(goal));
		}
	}
}

TEST(GoalTests, IsOrdinaryGoal) {
	std::set<uint8_t> xalidOrdinaryGoals;
	for (uint8_t goal = goal::kMinOrdinaryGoal; goal <= goal::kMaxOrdinaryGoal; ++goal) {
		xalidOrdinaryGoals.insert(goal);
	}

	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		if (xalidOrdinaryGoals.count(goal)) {
			EXPECT_TRUE(goal::isOrdinaryGoal(goal));
			EXPECT_FALSE(goal::isXorGoal(goal));
		} else {
			EXPECT_FALSE(goal::isOrdinaryGoal(goal));
		}
	}
}

TEST(GoalTests, IsGoalValid) {
	for (int goal = 0; goal <= std::numeric_limits<uint8_t>::max(); ++goal) {
		SCOPED_TRACE("Testing goal " + std::to_string(goal));
		if (goal::isXorGoal(goal) || goal::isOrdinaryGoal(goal)) {
			EXPECT_TRUE(goal::isGoalValid(goal));
		} else {
			EXPECT_FALSE(goal::isGoalValid(goal));
		}
	}
}

TEST(GoalTests, XorGoalConstants) {
	for (uint8_t level = goal::kMinXorLevel; level <= goal::kMaxXorLevel; level++) {
		uint8_t goal = goal::xorLevelToGoal(level);
		EXPECT_GE(goal, goal::kMinXorGoal);
		EXPECT_LE(goal, goal::kMaxXorGoal);
	}
}

