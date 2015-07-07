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
