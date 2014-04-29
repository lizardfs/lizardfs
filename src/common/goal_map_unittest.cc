#include "config.h"
#include "common/goal_map.h"

#include <gtest/gtest.h>

TEST(GoalMapTests, GoalMapUint8t) {
	GoalMap<uint8_t> map;
	for (int goal = 0; goal < 256; goal++) {
		if (isGoalValid(goal) || goal == 0) {
			map[goal] = goal;
		} else {
			EXPECT_THROW(map[goal] = goal, GoalMapInvalidGoalException);
		}
	}
	for (int goal = 0; goal < 256; goal++) {
		if (isGoalValid(goal) || goal == 0) {
			EXPECT_EQ(goal, map[goal]);
		} else {
			EXPECT_THROW(map[goal], GoalMapInvalidGoalException);
		}
	}
}

TEST(GoalMapTests, Serialization) {
	std::vector<uint8_t> buffer;
	{
		GoalMap<uint8_t> map;
		for (int goal = 0; goal < 256; goal++) {
			if (isGoalValid(goal) || goal == 0) {
				map[goal] = goal;
			}
		}
		serialize(buffer, map);
		EXPECT_EQ(serializedSize(map), buffer.size());
	}
	{
		GoalMap<uint8_t> map;
		deserialize(buffer, map);
		for (int goal = 0; goal < 256; goal++) {
			if (isGoalValid(goal) || goal == 0) {
				EXPECT_EQ(goal, map[goal]);
			}
		}
	}
}
