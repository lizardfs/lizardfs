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
#include "master/chunk_goal_counters.h"
#include "master/goal_cache.h"
#include "master/goal_config_loader.h"

#include <algorithm>
#include <map>

#include <gtest/gtest.h>

#define EXPECT_FINDS(value, container) \
	EXPECT_NE(std::find(container.begin(), container.end(), (value)), container.end())

TEST(ChunkGoalCounters, Add) {
	ChunkGoalCounters counters;
	EXPECT_EQ(0U, counters.highestIdGoal());
	EXPECT_EQ(0U, counters.size());
	counters.addFile(1);
	EXPECT_EQ(1U, counters.size());
	EXPECT_EQ(1U, counters.highestIdGoal());
	counters.addFile(3);
	EXPECT_EQ(2U, counters.size());
	EXPECT_EQ(3U, counters.highestIdGoal());
	counters.addFile(2);
	EXPECT_EQ(3U, counters.size());
	EXPECT_EQ(3U, counters.highestIdGoal());
	ASSERT_THROW(counters.addFile(57), ChunkGoalCounters::InvalidOperation);
	EXPECT_EQ(3U, counters.size());
	EXPECT_EQ(3U, counters.highestIdGoal());

	counters.addFile(1);
	counters.addFile(1);
	counters.addFile(3);

	for (auto counter : std::vector<ChunkGoalCounters::GoalCounter>{{1,3}, {2,1}, {3,2}}) {
		EXPECT_FINDS(counter, counters);
	}
}

TEST(ChunkGoalCounters, Remove) {
	ChunkGoalCounters counters;
	counters.addFile(1);
	counters.addFile(4);
	counters.addFile(5);
	counters.addFile(7);

	for (auto counter : std::vector<ChunkGoalCounters::GoalCounter>{{1,1}, {4,1}, {5,1}, {7,1}}) {
		EXPECT_FINDS(counter, counters);
	}

	EXPECT_EQ(4U, counters.size());
	EXPECT_EQ(7U, counters.highestIdGoal());

	ASSERT_THROW(counters.removeFile(83), ChunkGoalCounters::InvalidOperation);
	EXPECT_EQ(7U, counters.highestIdGoal());

	counters.removeFile(1);
	EXPECT_EQ(3U, counters.size());
	EXPECT_EQ(7U, counters.highestIdGoal());

	counters.removeFile(7);
	EXPECT_EQ(2U, counters.size());
	EXPECT_EQ(5U, counters.highestIdGoal());

	counters.removeFile(4);
	EXPECT_EQ(1U, counters.size());
	EXPECT_EQ(5U, counters.highestIdGoal());

	counters.removeFile(5);
	EXPECT_EQ(0U, counters.size());
	EXPECT_EQ(0U, counters.highestIdGoal());

	counters.addFile(2);
	counters.addFile(3);
	EXPECT_EQ(2U, counters.size());
	EXPECT_EQ(3U, counters.highestIdGoal());
	counters.removeFile(3);
	EXPECT_EQ(1U, counters.size());
	EXPECT_EQ(2U, counters.highestIdGoal());

	EXPECT_FINDS((ChunkGoalCounters::GoalCounter{2,1}), counters);
}

TEST(ChunkGoalCounters, Change) {
	ChunkGoalCounters counters;
	counters.addFile(1);
	EXPECT_EQ(1U, counters.size());
	EXPECT_EQ(1U, counters.highestIdGoal());
	counters.changeFileGoal(1, 3);
	EXPECT_EQ(1U, counters.size());
	EXPECT_EQ(3U, counters.highestIdGoal());

	counters.addFile(4);
	EXPECT_EQ(2U, counters.size());
	EXPECT_EQ(4U, counters.highestIdGoal());
	counters.changeFileGoal(4, 2);
	EXPECT_EQ(2U, counters.size());
	EXPECT_EQ(3U, counters.highestIdGoal());

	counters.addFile(6);
	EXPECT_EQ(3U, counters.size());
	EXPECT_EQ(6U, counters.highestIdGoal());
	counters.changeFileGoal(6, 1);
	EXPECT_EQ(3U, counters.size());
	EXPECT_EQ(3U, counters.highestIdGoal());

	for (auto counter : std::vector<ChunkGoalCounters::GoalCounter>{{3,1}, {2,1}, {1,1}}) {
		EXPECT_FINDS(counter, counters);
	}
}

TEST(ChunkGoalCounters, LotsOfGoals) {
	ChunkGoalCounters counters;
	std::map<unsigned, unsigned> quantity;
	// Number of different goals added
	const unsigned goalnum = 4;
	// Number of files for each goal
	const unsigned filenum = 536;
	// Number of counters needed to keep information about files per goal
	const unsigned counternum = (filenum + std::numeric_limits<uint8_t>::max() - 1) / std::numeric_limits<uint8_t>::max();


	for (unsigned i = 0; i < goalnum * filenum; ++i) {
		counters.addFile(i % goalnum + 1);
	}

	EXPECT_EQ(counters.size(), goalnum * counternum);

	for (auto &counter : counters) {
		quantity[counter.goal] += counter.count;
	}

	for (unsigned i = 1; i <= goalnum; ++i) {
		EXPECT_EQ(quantity[i], filenum);
	}

	for (unsigned i = 0; i < goalnum * filenum; ++i) {
		counters.removeFile(i % goalnum + 1);
	}

	EXPECT_EQ(counters.size(), 0U);
}

TEST(ChunkGoalCounters, Cache) {
	ChunkGoalCounters counters;
	GoalCache cache(2);
	Goal goal;
	std::pair<GoalCache::iterator,bool> it;

	counters.addFile(1);

	it = cache.insert(counters, goal);
	EXPECT_EQ(it, std::make_pair(cache.begin(), true));
	it = cache.insert(counters, goal);
	EXPECT_EQ(it, std::make_pair(cache.begin(), false));
	EXPECT_EQ(1U, cache.size());

	ChunkGoalCounters orig = counters;
	counters.addFile(2);
	cache.emplace(counters, goal);
	EXPECT_EQ(2U, cache.size());

	// Cache capacity is 2, so adding a third entry should end up in size 2
	ChunkGoalCounters orig2 = counters;
	counters.addFile(3);
	cache.insert(counters, goal);
	EXPECT_EQ(2U, cache.size());

	// First object should be invalidated from cache
	EXPECT_EQ(cache.find(orig), cache.end());
	EXPECT_NE(cache.find(orig2), cache.end());
	EXPECT_NE(cache.find(counters), cache.end());

	cache.clear();

	EXPECT_EQ(0U, cache.size());
}
