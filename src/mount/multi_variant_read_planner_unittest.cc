#include "common/platform.h"
#include "mount/multi_variant_read_planner.h"

#include <algorithm>
#include <sstream>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "common/MFSCommunication.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"
#include "unittests/plan_tester.h"

typedef std::set<ChunkType> Set;
typedef std::vector<ChunkType> Vector;

// For the given planner, returns a set of parts which would be used in the basic variant of a plan
static Set getPartsToUseInBasicPlan(const ReadPlanner& planner) {
	auto plan = planner.buildPlanFor(0, 20);
	Set ret;
	for (const auto& chunkTypeAndReadOperation : plan->basicReadOperations) {
		ret.insert(chunkTypeAndReadOperation.first);
	}
	return ret;
}

// For the given set of parts, returns a set of parts which would be used in the basic variant
static Set getPartsToUseInBasicPlan(std::vector<ChunkType> availableParts,
		std::map<ChunkType, float> scores = std::map<ChunkType, float>()) {
	MultiVariantReadPlanner planner;
	planner.setScores(std::move(scores));
	planner.prepare(availableParts);
	return (getPartsToUseInBasicPlan(planner));
}

TEST(MultiVariantReadPlannerTests, IsReadingPossible) {
	MultiVariantReadPlanner planner;
	auto isReadingPossible = [&](const Vector& parts) {
		planner.prepare(parts);
		return planner.isReadingPossible();
	};

	// simple tests
	EXPECT_TRUE(isReadingPossible({xor_1_of_2, xor_2_of_2, xor_p_of_2}));
	EXPECT_TRUE(isReadingPossible({xor_1_of_2, xor_2_of_2}));
	EXPECT_TRUE(isReadingPossible({xor_1_of_2, xor_p_of_2}));
	EXPECT_TRUE(isReadingPossible({xor_2_of_2, xor_p_of_2}));
	EXPECT_FALSE(isReadingPossible({xor_1_of_2}));
	EXPECT_FALSE(isReadingPossible({xor_2_of_2}));
	EXPECT_FALSE(isReadingPossible({xor_p_of_2}));

	EXPECT_TRUE(isReadingPossible({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3}));
	EXPECT_TRUE(isReadingPossible({xor_1_of_3, xor_2_of_3, xor_3_of_3}));
	EXPECT_TRUE(isReadingPossible({xor_1_of_3, xor_2_of_3, xor_p_of_3}));
	EXPECT_FALSE(isReadingPossible({xor_1_of_3, xor_2_of_3}));
	EXPECT_FALSE(isReadingPossible({xor_p_of_2}));

	// simple test with some scores
	planner.setScores({{xor_1_of_2, 0.0}});
	EXPECT_TRUE(isReadingPossible({xor_1_of_2, xor_2_of_2, xor_p_of_2}));
	EXPECT_TRUE(isReadingPossible({xor_1_of_2, xor_2_of_2}));
	EXPECT_TRUE(isReadingPossible({xor_1_of_2, xor_p_of_2}));
	EXPECT_TRUE(isReadingPossible({xor_2_of_2, xor_p_of_2}));
	EXPECT_FALSE(isReadingPossible({xor_1_of_2}));
	EXPECT_FALSE(isReadingPossible({xor_2_of_2}));
	EXPECT_FALSE(isReadingPossible({xor_p_of_2}));

	// some xor parts + one standard part
	planner.setScores({});
	EXPECT_TRUE(isReadingPossible({xor_1_of_3, xor_2_of_3, standard}));
	EXPECT_TRUE(isReadingPossible({xor_p_of_2, standard}));

	planner.setScores({{standard, 0.0}});
	EXPECT_TRUE(isReadingPossible({xor_1_of_3, xor_2_of_3, standard}));
	EXPECT_TRUE(isReadingPossible({xor_p_of_2, standard}));

	// two sets of xor parts, one incomplete, one with a 0-scored part
	planner.setScores({{xor_1_of_2, 0.0}});
	EXPECT_TRUE(isReadingPossible({xor_1_of_3, xor_2_of_3, xor_1_of_2, xor_2_of_2}));
}

TEST(MultiVariantReadPlannerTests, BasicReadOperations1) {
	// With default scores

	EXPECT_EQ(Set({standard}),
			getPartsToUseInBasicPlan({standard}));

	EXPECT_EQ(Set({standard}),
			getPartsToUseInBasicPlan({standard, xor_1_of_2, xor_2_of_2, xor_p_of_2}));

	EXPECT_EQ(Set({xor_1_of_2, xor_2_of_2}),
			getPartsToUseInBasicPlan({xor_1_of_2, xor_2_of_2, xor_p_of_2}));

	EXPECT_EQ(Set({xor_1_of_2, xor_p_of_2}),
			getPartsToUseInBasicPlan({xor_1_of_2, xor_p_of_2}));

	EXPECT_EQ(Set({xor_1_of_2, xor_p_of_2}),
			getPartsToUseInBasicPlan({xor_1_of_2, xor_p_of_2, xor_2_of_3}));

	EXPECT_EQ(Set({xor_1_of_3, xor_2_of_3, xor_3_of_3}),
			getPartsToUseInBasicPlan({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3}));

	// Now we will use some special scores

	EXPECT_EQ(Set({xor_1_of_2, xor_p_of_2}), getPartsToUseInBasicPlan(
					{xor_1_of_2, xor_2_of_2, xor_p_of_2},
					{{xor_2_of_2, 0.5}}));

	EXPECT_EQ(Set({xor_1_of_3, xor_3_of_3, xor_p_of_3}), getPartsToUseInBasicPlan(
					{xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3},
					{{xor_2_of_3, 0.5}}));

	EXPECT_EQ(Set({xor_1_of_2, xor_2_of_2}), getPartsToUseInBasicPlan(
					{xor_1_of_2, xor_2_of_2, xor_p_of_2, standard},
					{{standard, 0.5}}));

	EXPECT_EQ(Set({xor_1_of_2, xor_p_of_2}), getPartsToUseInBasicPlan(
					{xor_1_of_2, xor_p_of_2, standard},
					{{standard, 0.5}}));
}

TEST(MultiVariantReadPlannerTests, BuildPlanForStandard) {
	Vector availableParts{standard};
	MultiVariantReadPlanner planner;
	planner.prepare(availableParts);
	auto plan = planner.buildPlanFor(1, 9);
	ASSERT_EQ(unittests::PlanTester::expectedAnswer(standard, 1, 9),
			unittests::PlanTester::executePlan(*plan, availableParts, 9));
}

TEST(MultiVariantReadPlannerTests, BuildPlanForMissingXorPart1) {
	Vector availableParts{xor_1_of_3, xor_2_of_3, xor_p_of_3};
	MultiVariantReadPlanner planner;
	planner.prepare(availableParts);
	auto plan = planner.buildPlanFor(1, 9);
	ASSERT_EQ(unittests::PlanTester::expectedAnswer(standard, 1, 9),
			unittests::PlanTester::executePlan(*plan, availableParts, 9));
}

TEST(MultiVariantReadPlannerTests, BuildPlanForMissingXorPart2) {
	Vector availableParts{xor_1_of_3, xor_2_of_3, xor_3_of_3};
	MultiVariantReadPlanner planner;
	planner.prepare(availableParts);
	auto plan = planner.buildPlanFor(1, 9);
	ASSERT_EQ(unittests::PlanTester::expectedAnswer(standard, 1, 9),
			unittests::PlanTester::executePlan(*plan, availableParts, 9));
}

// Tests the given planner.
// It will also verify if 'partNotUsedInTheBasicVariant' is not being read in the basic variant.
static void testPlanner(const ReadPlanner& planner, ChunkType partNotUsedInTheBasicVariant) {
	auto parts = planner.partsToUse();
	SCOPED_TRACE("Testing planner which uses " + ::testing::PrintToString(parts));
	SCOPED_TRACE("Unused (in the basic variant) part is "
			+ ::testing::PrintToString(partNotUsedInTheBasicVariant));

	// A list of ranges to be tested in the form of (firstBlock, blockCount)
	std::vector<std::pair<uint32_t, uint32_t>> ranges = {
			{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {0, 6}, {0, 7}, {0, 8}, {0, 9}, {0, 10},
			{1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}, {1, 6}, {1, 7}, {1, 8}, {1, 9}, {1, 10},
			{2, 1}, {2, 2}, {2, 4}, {2, 7}, {2, 10},
			{3, 1}, {3, 2}, {3, 4}, {3, 7}, {3, 10},
			{5, 1}, {5, 2}, {5, 4}, {5, 7}, {5, 10},
			{9, 1}, {9, 2}, {9, 4}, {9, 7}, {9, 10},
			// last block, two last blocks, ..., 5 last blocks, 10 last blocks
			{MFSBLOCKSINCHUNK - 1, 1}, {MFSBLOCKSINCHUNK - 2, 2}, {MFSBLOCKSINCHUNK - 3, 3},
			{MFSBLOCKSINCHUNK - 4, 4}, {MFSBLOCKSINCHUNK - 5, 5}, {MFSBLOCKSINCHUNK - 10, 10},
			// 1, 2, 3, 6, 7, 8 blocks starting from 10th from the end
			{MFSBLOCKSINCHUNK - 10, 1}, {MFSBLOCKSINCHUNK - 10, 2}, {MFSBLOCKSINCHUNK - 10, 3},
			{MFSBLOCKSINCHUNK - 10, 6}, {MFSBLOCKSINCHUNK - 10, 7}, {MFSBLOCKSINCHUNK - 10, 8},
			// a couple of blocks somewhere in the middle
			{MFSBLOCKSINCHUNK / 2, 1}, {MFSBLOCKSINCHUNK / 2, 2}, {MFSBLOCKSINCHUNK / 2, 3},
			{MFSBLOCKSINCHUNK / 2, 4}, {MFSBLOCKSINCHUNK / 2, 5}, {MFSBLOCKSINCHUNK / 2, 8},
			{MFSBLOCKSINCHUNK / 3, 1}, {MFSBLOCKSINCHUNK / 3, 2}, {MFSBLOCKSINCHUNK / 3, 3},
			// the whole chunk
			{0, MFSBLOCKSINCHUNK},
			// first half, second half
			{0, MFSBLOCKSINCHUNK / 2}, {MFSBLOCKSINCHUNK / 2, MFSBLOCKSINCHUNK / 2},
			// first 1/5, second 1/5
			{0, MFSBLOCKSINCHUNK / 5}, {MFSBLOCKSINCHUNK / 5, MFSBLOCKSINCHUNK / 5},
	};
	for (const auto& range : ranges) {
		uint32_t firstBlock = range.first;
		uint32_t blockCount = range.second;
		SCOPED_TRACE("Testing first block " + ::testing::PrintToString(firstBlock));
		SCOPED_TRACE("Testing block count " + ::testing::PrintToString(blockCount));

		// Prepare a plan
		std::unique_ptr<ReadPlan> plan;
		ASSERT_NO_THROW(plan = planner.buildPlanFor(firstBlock, blockCount));
		std::vector<unittests::Block> expectedResult =
				unittests::PlanTester::expectedAnswer(standard, firstBlock, blockCount);
		SCOPED_TRACE("Testing plan " + ::testing::PrintToString(*plan));
		ASSERT_EQ(0U, plan->basicReadOperations.count(partNotUsedInTheBasicVariant));

		// Test the basic variant (where nothing fails)
		std::vector<unittests::Block> actualResult;
		ASSERT_NO_THROW(actualResult =
				unittests::PlanTester::executePlan(*plan, parts, blockCount, {/* no failures */}));
		ASSERT_EQ(expectedResult, actualResult);

		// For each part test also the extended variant in which this part fails
		for (ChunkType failingPart : parts) {
			SCOPED_TRACE("Testing variant where fails " + ::testing::PrintToString(failingPart));
			ASSERT_TRUE(plan->isReadingFinished({failingPart}));
			ASSERT_NO_THROW(actualResult =
					unittests::PlanTester::executePlan(*plan, parts, blockCount, {failingPart}));
			ASSERT_EQ(expectedResult, actualResult);
		}
	}
}

TEST(MultiVariantReadPlannerTests, BuildPlanForXor) {
	MultiVariantReadPlanner planner;
	// We will test each xor level
	for (uint32_t xorLevel = goal::kMinXorLevel; xorLevel <= goal::kMaxXorLevel; ++xorLevel) {
		// Prepare a list of all parts for the current level, ie;
		// parts = { xor_p_of_N, xor_1_of_N, xor_2_of_N, ...., xor_N_of_N }
		Vector parts{ChunkType::getXorParityChunkType(xorLevel)};
		for (uint32_t xorPart = 1; xorPart <= xorLevel; ++xorPart) {
			parts.push_back(ChunkType::getXorChunkType(xorLevel, xorPart));
		}

		// Prepare scores for parts, 1.0 for each part
		std::map<ChunkType, float> scores;
		for (ChunkType part : parts) {
			scores[part] = 1.0;
		}

		// Basic scenario, parity should be the unused part
		planner.setScores(scores);
		ASSERT_NO_THROW(planner.prepare(parts));
		testPlanner(planner, parts[0]);

		// xor_1_of_N unused
		scores[parts[1]] = 0.5; // set very low score for xor_1_of_N, so that parity has to be used
		planner.setScores(scores);
		ASSERT_NO_THROW(planner.prepare(parts));
		testPlanner(planner, parts[1]);

		// xor_N_of_N unused
		scores[parts[xorLevel]] = 0.2; // set even lower score for xor_N_of_N
		planner.setScores(scores);
		ASSERT_NO_THROW(planner.prepare(parts));
		testPlanner(planner, parts[xorLevel]);
	}
}

TEST(MultiVariantReadPlannerTests, StartAvoidingPart1) {
	MultiVariantReadPlanner planner;
	Vector parts{xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3};
	planner.prepare(parts);
	for (auto part : parts) {
		SCOPED_TRACE("Avoiding part" + ::testing::PrintToString(part));
		planner.startAvoidingPart(part);
		testPlanner(planner, part);
	}
}

TEST(MultiVariantReadPlannerTests, StartAvoidingPart2) {
	MultiVariantReadPlanner planner;
	Vector parts{xor_1_of_3, xor_2_of_3, xor_p_of_3};
	planner.prepare(parts);
	for (auto part : parts) {
		SCOPED_TRACE("Avoiding part" + ::testing::PrintToString(part));
		planner.startAvoidingPart(part);
		EXPECT_EQ(Set({xor_1_of_3, xor_2_of_3, xor_p_of_3}), getPartsToUseInBasicPlan(planner));
	}
}

TEST(MultiVariantReadPlannerTests, StartAvoidingPart3) {
	MultiVariantReadPlanner planner;
	planner.prepare({standard});
	planner.startAvoidingPart(standard);
	EXPECT_EQ(Set({standard}), getPartsToUseInBasicPlan(planner));
}
