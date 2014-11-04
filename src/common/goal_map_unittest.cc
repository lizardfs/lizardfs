#include "common/platform.h"
#include "common/goal_map.h"

#include <gtest/gtest.h>

#include "common/hashfn.h"
#include "unittests/serialization.h"

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

TEST(GoalMapTests, SoooComplicatedThoughStillSerializable) {
	GoalMap<std::map<uint32_t, std::map<int, std::set<uint32_t>>>> mapIn;
	mapIn[2][3][4] = {5, 6, 7};
	mapIn[8][9][10] = {11, 12};
	mapIn[13][14];
	std::vector<uint8_t> buf;
	serializeTest(mapIn);
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
