#include "common/platform.h"
#include "common/goal_map.h"

#include <gtest/gtest.h>

#include "common/hashfn.h"

TEST(GoalMapTests, GoalMapUint8t) {
	GoalMap<uint64_t> map;
	for (int goal = 0; goal < 256; goal++) {
		if (goal::isGoalValid(goal) || goal == 0) {
			map[goal] = hash(goal);
		} else {
			EXPECT_THROW(map[goal] = hash(goal), GoalMapInvalidGoalException);
		}
	}
	for (int goal = 0; goal < 256; goal++) {
		if (goal::isGoalValid(goal) || goal == 0) {
			EXPECT_EQ(hash(goal), map[goal]);
		} else {
			EXPECT_THROW(map[goal], GoalMapInvalidGoalException);
		}
	}
}

TEST(GoalMapTests, Serialization) {
	std::vector<uint8_t> buffer;
	{
		GoalMap<uint64_t> map;
		for (int goal = 0; goal < 256; goal++) {
			if (goal::isGoalValid(goal) || goal == 0) {
				map[goal] = hash(goal);
			}
		}
		serialize(buffer, map);
		EXPECT_EQ(serializedSize(map), buffer.size());
	}
	{
		GoalMap<uint64_t> map;
		deserialize(buffer, map);
		for (int goal = 0; goal < 256; goal++) {
			if (goal::isGoalValid(goal) || goal == 0) {
				EXPECT_EQ(hash(goal), map[goal]);
			}
		}
	}
}
