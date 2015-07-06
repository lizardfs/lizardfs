#include "common/platform.h"
#include "master/chunk_goal_counters.h"

#include <algorithm>

#include <gtest/gtest.h>

#define EXPECT_FINDS(value, container) \
	EXPECT_NE(std::find(container.begin(), container.end(), (value)), container.end())

static uint8_t xor2 = goal::xorLevelToGoal(2);
static uint8_t xor3 = goal::xorLevelToGoal(3);
static uint8_t xor5 = goal::xorLevelToGoal(5);
static uint8_t xor9 = goal::xorLevelToGoal(9);
typedef std::vector<uint8_t> Goals;
typedef Goals ExpectedGoals;

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

#define EXPECT_GOAL(expected, goals) do { \
			ChunkGoalCounters counters; \
			for (int goal : goals) { \
				counters.addFile(goal); \
			} \
			EXPECT_EQ(expected, counters.highestIdGoal()); \
		} while(0)

TEST(ChunkGoalCounters, XorAdd) {
	EXPECT_GOAL(xor2, Goals({1, xor2}));
	EXPECT_GOAL(xor2, Goals({xor2, 1}));
	EXPECT_GOAL(3U, Goals({3, xor2}));
	EXPECT_GOAL(3U, Goals({xor2, 3}));
	EXPECT_GOAL(2U, Goals({2, xor5}));
	EXPECT_GOAL(2U, Goals({xor5, 2}));
	EXPECT_GOAL(xor5, Goals({1, xor5}));
	EXPECT_GOAL(xor5, Goals({xor5, 1}));
	EXPECT_GOAL(xor5, Goals({xor2, xor5, xor3}));
	EXPECT_GOAL(2, Goals({xor2, 2, xor5, 1, xor3, 1}));
	EXPECT_GOAL(xor5, Goals({xor2, 1, xor5, 1, xor3, 1}));
	EXPECT_GOAL(4U, Goals({xor2, 4, xor5, 1, xor9, 3}));
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

#define EXPECT_GOAL_AND_REMOVE(expected, goals) do { \
			ChunkGoalCounters counters; \
			EXPECT_EQ(goals.size(), expected.size()); \
			for (size_t indx = 0; indx < goals.size(); ++indx) { \
				counters.addFile(goals[indx]); \
				EXPECT_EQ(expected[indx], counters.highestIdGoal()); \
			} \
			for (ssize_t indx = goals.size() - 1; indx >= 0; --indx) { \
				EXPECT_EQ(expected[indx], counters.highestIdGoal()); \
				counters.removeFile(goals[indx]); \
			} \
			EXPECT_EQ(0, counters.highestIdGoal()); \
		} while(0)

/*
 * This test first adds goals from the `goals' vector and verifies that the goal is equal to
 * the one provided in `expected' vector. Then it iterates backwards through the vectors removing
 * goals and verifying the result. At the very end goal should be 0.
 */
TEST(ChunkGoalCounters, XorRemove) {
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({1, xor2}), Goals({1, xor2}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({xor2, xor2}), Goals({xor2, 1}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({3, 3}), Goals({3, xor2}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({xor2, 3}), Goals({xor2, 3}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({2, 2}), Goals({2, xor5}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({xor5, xor5}), Goals({xor5, 1}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({1, xor5}), Goals({1, xor5}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({xor5, 2}), Goals({xor5, 2}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({xor2, xor5, xor5}), Goals({xor2, xor5, xor3}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({1, xor2, 2, 2, 2, 2, 2}),
			Goals({1, xor2, 2, xor5, 1, xor3, 1}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({1, xor2, xor2, xor5, xor5, xor5, xor5}),
				Goals({1, xor2, 1, xor5, 1, xor3, 1}));
	EXPECT_GOAL_AND_REMOVE(ExpectedGoals({xor2, 4U, 4U, 4U, 4U, 4U}),
			Goals({xor2, 4, xor5, 1, xor9, 3}));
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
#define EXPECT_GOAL_AND_CHANGE(expected, goals, changes) do { \
	ChunkGoalCounters counters; \
	EXPECT_EQ(goals.size() + changes.size(), expected.size()); \
	for (size_t indx = 0; indx < goals.size(); ++indx) { \
		counters.addFile(goals[indx]); \
		EXPECT_EQ(expected[indx], counters.highestIdGoal()); \
	} \
	for (size_t indx = 0; indx < changes.size(); ++indx) { \
		counters.changeFileGoal(changes[indx].first, changes[indx].second); \
		EXPECT_EQ(expected[indx + goals.size()], counters.highestIdGoal()); \
	} \
} while(0)

/*
 * This test first adds goals from the `goals' vector and verifies that the goal is equal to
 * the one provided in `expected' vector (just like in XorRemove).
 * Then it iterates through `changes' and changes the goal from `change.first' to `change.second'
 * (change is std::pair). `expected' contains expected goals after each operation
 * (i.e. `addFile's and `changeFile's collectively).
 */
TEST(ChunkGoalCounters, XorChange) {
	typedef std::vector<std::pair<uint8_t, uint8_t>> Changes;
	EXPECT_GOAL_AND_CHANGE(ExpectedGoals({1, xor2, 3, xor3, xor5, xor3}), Goals({1, xor2}),
			Changes({{1, 3}, {3, xor3}, {xor2, xor5}, {xor5, 1}}));
	EXPECT_GOAL_AND_CHANGE(ExpectedGoals({xor5, xor5, 3, 3, xor9}), Goals({xor5, 1}),
				Changes({{1, 3}, {xor5, xor9}, {3, 1}}));
	EXPECT_GOAL_AND_CHANGE(ExpectedGoals({xor5, 2, 3, 3, xor9}), Goals({xor5, 2}),
				Changes({{2, 3}, {xor5, xor9}, {3, 1}}));
	EXPECT_GOAL_AND_CHANGE(ExpectedGoals({xor2, 4, 4, 4, 4, 4, 5, 5, 5, 5, xor5, xor5}),
			Goals({xor2, 4, xor5, 1, xor9, 3}),
			Changes({{4, 5}, {xor2, xor5}, {xor9, xor5}, {3, 1}, {5, 1}, {xor5, xor2}}));
}

TEST(ChunkGoalCounters, LotsOfGoals) {
	ChunkGoalCounters counters;
	std::map<unsigned, unsigned> quantity;
	/* Number of different goals added */
	const unsigned goalnum = 4;
	/* Number of files for each goal */
	const unsigned filenum = 536;
	/* Number of counters needed to keep information about files per goal */
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
