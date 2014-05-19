#include "config.h"
#include "master/chunk_goal_counters.h"

#include <gtest/gtest.h>

TEST(ChunkGoalCounters, Add) {
	ChunkGoalCounters counters;
	EXPECT_EQ(0U, counters.combinedGoal());
	counters.addFile(1);
	EXPECT_EQ(1U, counters.combinedGoal());
	counters.addFile(3);
	EXPECT_EQ(3U, counters.combinedGoal());
	counters.addFile(2);
	EXPECT_EQ(3U, counters.combinedGoal());
	ASSERT_THROW(counters.addFile(57), ChunkGoalCounters::InvalidOperation);
	EXPECT_EQ(3U, counters.combinedGoal());
}

TEST(ChunkGoalCounters, Remove) {
	ChunkGoalCounters counters;
	counters.addFile(1);
	counters.addFile(4);
	counters.addFile(5);
	counters.addFile(7);
	EXPECT_EQ(7U, counters.combinedGoal());

	ASSERT_THROW(counters.removeFile(83), ChunkGoalCounters::InvalidOperation);
	EXPECT_EQ(7U, counters.combinedGoal());

	counters.removeFile(1);
	EXPECT_EQ(7U, counters.combinedGoal());

	counters.removeFile(7);
	EXPECT_EQ(5U, counters.combinedGoal());

	counters.removeFile(4);
	EXPECT_EQ(5U, counters.combinedGoal());

	counters.removeFile(5);
	EXPECT_EQ(0U, counters.combinedGoal());

	counters.addFile(2);
	counters.addFile(3);
	EXPECT_EQ(3U, counters.combinedGoal());
	counters.removeFile(3);
	EXPECT_EQ(2U, counters.combinedGoal());
}

TEST(ChunkGoalCounters, Change) {
	ChunkGoalCounters counters;
	counters.addFile(1);
	EXPECT_EQ(1U, counters.combinedGoal());
	counters.changeFileGoal(1, 3);
	EXPECT_EQ(3U, counters.combinedGoal());

	counters.addFile(4);
	EXPECT_EQ(4U, counters.combinedGoal());
	counters.changeFileGoal(4, 2);
	EXPECT_EQ(3U, counters.combinedGoal());

	counters.addFile(6);
	EXPECT_EQ(6U, counters.combinedGoal());
	counters.changeFileGoal(6, 1);
	EXPECT_EQ(3U, counters.combinedGoal());
}
