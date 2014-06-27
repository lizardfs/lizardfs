#include "common/platform.h"
#include "common/xor_chunk_read_planner.h"

#include <gtest/gtest.h>

#include "common/goal.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"
#include "unittests/plan_tester.h"

#define VERIFY_PLAN_FOR(plannerChunkType, planner, firstBlock, count) \
	ASSERT_EQ(\
		unittests::PlanTester::expectedAnswer(plannerChunkType, firstBlock, count), \
		unittests::PlanTester::executePlan(\
				planner.buildPlanFor(firstBlock, count), \
				planner.partsToUse(), count))

class XorChunkReadPlannerTests: public testing::Test {
protected:
	void checkChoice(ChunkType plannerChunkType,
			const std::vector<ChunkType>& availableParts, const std::vector<ChunkType>& expected) {
		for (ChunkType part : availableParts) {
			scores[part] = 1.0;
		}
		XorChunkReadPlanner planner(plannerChunkType);
		planner.prepare(availableParts, scores);
		EXPECT_EQ(expected, planner.partsToUse());
	}

	void checkImpossibleness(ChunkType plannerChunkType, const std::vector<ChunkType>& availableParts) {
		for (ChunkType part : availableParts) {
			scores[part] = 1.0;
		}
		XorChunkReadPlanner planner(plannerChunkType);
		planner.prepare(availableParts, scores);
		EXPECT_FALSE(planner.isReadingPossible());
	}

	void verifyPlanner(ChunkType plannerChunkType, const std::vector<ChunkType>& availableParts) {
		SCOPED_TRACE("Testing recovery of " + ::testing::PrintToString(plannerChunkType));
		SCOPED_TRACE("Testing reading from " + ::testing::PrintToString(availableParts));
		for (ChunkType part : availableParts) {
			scores[part] = 1.0;
		}
		uint32_t blocksInPart = plannerChunkType.getNumberOfBlocks(MFSBLOCKSINCHUNK);

		XorChunkReadPlanner planner(plannerChunkType);
		planner.prepare(availableParts, scores);
		VERIFY_PLAN_FOR(plannerChunkType, planner, 0, 1); // blocks: 0
		VERIFY_PLAN_FOR(plannerChunkType, planner, 0, 2); // blocks: 0, 1
		VERIFY_PLAN_FOR(plannerChunkType, planner, 0, 3); // blocks: 0, 1, 2
		VERIFY_PLAN_FOR(plannerChunkType, planner, 0, 4); // blocks: 0, 1, 2, 3
		VERIFY_PLAN_FOR(plannerChunkType, planner, 1, 1); // blocks: 1
		VERIFY_PLAN_FOR(plannerChunkType, planner, 1, 2); // blocks: 1, 2
		VERIFY_PLAN_FOR(plannerChunkType, planner, 1, 3); // blocks: 1, 2, 3
		VERIFY_PLAN_FOR(plannerChunkType, planner, 1, 4); // blocks: 1, 2, 3, 4
		VERIFY_PLAN_FOR(plannerChunkType, planner, 2, 1); // blocks: 2
		VERIFY_PLAN_FOR(plannerChunkType, planner, 2, 2); // blocks: 2, 3
		VERIFY_PLAN_FOR(plannerChunkType, planner, 2, 3); // blocks: 2, 3, 4
		VERIFY_PLAN_FOR(plannerChunkType, planner, 2, 4); // blocks: 2, 3, 4, 5
		VERIFY_PLAN_FOR(plannerChunkType, planner, 3, 1); // blocks: 3
		VERIFY_PLAN_FOR(plannerChunkType, planner, 3, 2); // blocks: 3, 4
		VERIFY_PLAN_FOR(plannerChunkType, planner, 3, 3); // blocks: 3, 4, 5
		VERIFY_PLAN_FOR(plannerChunkType, planner, 3, 4); // blocks: 3, 4, 5, 6
		VERIFY_PLAN_FOR(plannerChunkType, planner, blocksInPart - 4, 4); // last four blocks
		VERIFY_PLAN_FOR(plannerChunkType, planner, blocksInPart - 3, 3); // last three blocks
		VERIFY_PLAN_FOR(plannerChunkType, planner, blocksInPart - 2, 2); // last two blocks
		VERIFY_PLAN_FOR(plannerChunkType, planner, blocksInPart - 1, 1); // last block
		VERIFY_PLAN_FOR(plannerChunkType, planner, blocksInPart - 4, 1);
		VERIFY_PLAN_FOR(plannerChunkType, planner, blocksInPart - 3, 1);
		VERIFY_PLAN_FOR(plannerChunkType, planner, blocksInPart - 2, 1);
		VERIFY_PLAN_FOR(plannerChunkType, planner, 0, blocksInPart); // all blocks
		VERIFY_PLAN_FOR(plannerChunkType, planner, 1, blocksInPart - 1);
		VERIFY_PLAN_FOR(plannerChunkType, planner, 2, blocksInPart - 2);
		VERIFY_PLAN_FOR(plannerChunkType, planner, 3, blocksInPart - 3);
		VERIFY_PLAN_FOR(plannerChunkType, planner, 4, blocksInPart - 4);
	}

protected:
	std::map<ChunkType, float> scores;
};

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseStandard1) {
	checkChoice(xor_1_of_4,
			{standard, standard, standard},
			{standard});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseStandard2) {
	checkChoice(xor_1_of_3,
			{standard, xor_1_of_2, xor_2_of_2},
			{standard});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseStandard3) {
	checkChoice(xor_p_of_4,
			{xor_1_of_6, xor_2_of_6, xor_3_of_6, xor_4_of_6, xor_5_of_6, xor_6_of_6, standard},
			{standard});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseCopy1) {
	checkChoice(xor_1_of_2,
			{standard, xor_1_of_2, xor_2_of_2},
			{xor_1_of_2});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseCopy2) {
	checkChoice(xor_1_of_3,
			{standard, xor_1_of_2, xor_2_of_2},
			{standard});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseCopy3) {
	checkChoice(xor_p_of_2,
			{xor_1_of_2, xor_p_of_2, xor_1_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4, xor_p_of_4},
			{xor_p_of_2});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseAvoidParity1) {
	checkChoice(xor_1_of_3,
			{xor_1_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4, xor_p_of_4},
			{xor_1_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseAvoidParity2) {
	checkChoice(xor_p_of_3,
			{xor_1_of_7, xor_2_of_7, xor_3_of_7, xor_4_of_7, xor_5_of_7, xor_6_of_7, xor_7_of_7, xor_p_of_7},
			{xor_1_of_7, xor_2_of_7, xor_3_of_7, xor_4_of_7, xor_5_of_7, xor_6_of_7, xor_7_of_7});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseAvoidParity3) {
	checkChoice(xor_1_of_3,
			{xor_1_of_2, xor_p_of_2, xor_1_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4, xor_p_of_4},
			{xor_1_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseLowestXor) {
	checkChoice(xor_1_of_3,
			{xor_1_of_2, xor_2_of_2, xor_p_of_2, xor_1_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4, xor_p_of_4},
			{xor_1_of_2, xor_2_of_2});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseImpossible1) {
	checkImpossibleness(xor_p_of_4, {xor_1_of_4, xor_2_of_4, xor_3_of_4});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseImpossible2) {
	checkImpossibleness(xor_p_of_2, {xor_1_of_4, xor_2_of_4, xor_p_of_4});
}

TEST_F(XorChunkReadPlannerTests, ChoosePartsToUseImpossible3) {
	checkImpossibleness(xor_p_of_4, { xor_p_of_2 });
}

TEST_F(XorChunkReadPlannerTests, GetPlanRecoverPartWithoutParity) {
	std::vector<ChunkType> plannedChunkTypes = {
			xor_1_of_2, xor_2_of_2,
			xor_1_of_3, xor_2_of_3, xor_3_of_3,
			ChunkType::getXorChunkType(kMaxXorLevel, 1),
			ChunkType::getXorChunkType(kMaxXorLevel, kMaxXorLevel),
	};
	std::vector<std::vector<ChunkType>> availablePartsSets = {
			{standard},
			{xor_1_of_2, xor_2_of_2},
			{xor_1_of_3, xor_2_of_3, xor_3_of_3},
	};
	for (ChunkType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}

TEST_F(XorChunkReadPlannerTests, GetPlanRecoverPartFromParity) {
	std::vector<ChunkType> plannedChunkTypes = {
			xor_1_of_2, xor_2_of_2,
			xor_1_of_3, xor_2_of_3, xor_3_of_3,
			ChunkType::getXorChunkType(kMaxXorLevel, 1),
			ChunkType::getXorChunkType(kMaxXorLevel, kMaxXorLevel),
	};
	std::vector<std::vector<ChunkType>> availablePartsSets = {
			{xor_1_of_2, xor_p_of_2},
			{xor_2_of_2, xor_p_of_2},
			{xor_1_of_3, xor_p_of_3, xor_3_of_3},
	};
	for (ChunkType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}

TEST_F(XorChunkReadPlannerTests, GetPlanRecoverParityWithoutParity) {
	std::vector<ChunkType> plannedChunkTypes = {
			xor_p_of_2, xor_p_of_3, xor_p_of_6,
			ChunkType::getXorParityChunkType(kMaxXorLevel),
	};
	std::vector<std::vector<ChunkType>> availablePartsSets = {
			{standard},
			{xor_1_of_2, xor_2_of_2},
			{xor_1_of_3, xor_2_of_3, xor_3_of_3},
	};
	for (ChunkType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}

TEST_F(XorChunkReadPlannerTests, GetPlanRecoverParityFromParity) {
	std::vector<ChunkType> plannedChunkTypes = {
			xor_p_of_2, xor_p_of_3, xor_p_of_6,
			ChunkType::getXorParityChunkType(kMaxXorLevel),
	};
	std::vector<std::vector<ChunkType>> availablePartsSets = {
			{xor_1_of_2, xor_p_of_2},
			{xor_2_of_2, xor_p_of_2},
			{xor_1_of_3, xor_p_of_3, xor_3_of_3},
	};
	for (ChunkType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}
