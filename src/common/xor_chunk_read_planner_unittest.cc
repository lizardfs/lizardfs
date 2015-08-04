/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include "common/xor_chunk_read_planner.h"

#include <gtest/gtest.h>

#include "common/goal.h"
#include "common/slice_traits.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"
#include "unittests/plan_tester.h"

#define VERIFY_PLAN_FOR(plannerChunkType, planner, firstBlock, count) \
	do {\
		auto plan = planner.buildPlanFor(firstBlock, count); \
		SCOPED_TRACE("Veryfing plan " + ::testing::PrintToString(*plan)); \
		ASSERT_EQ(\
				unittests::PlanTester::expectedAnswer(plannerChunkType, firstBlock, count), \
				unittests::PlanTester::executePlan(*plan, planner.partsToUse(), count)); \
	} while (0)

class XorChunkReadPlannerTests: public testing::Test {
protected:
	void checkChoice(ChunkPartType plannerChunkType,
			const std::vector<ChunkPartType>& availableParts, const std::vector<ChunkPartType>& expected) {
		XorChunkReadPlanner planner(plannerChunkType);
		planner.prepare(availableParts);
		EXPECT_EQ(expected, planner.partsToUse());
	}

	void checkImpossibleness(ChunkPartType plannerChunkType, const std::vector<ChunkPartType>& availableParts) {
		XorChunkReadPlanner planner(plannerChunkType);
		planner.prepare(availableParts);
		EXPECT_FALSE(planner.isReadingPossible());
	}

	void verifyPlanner(ChunkPartType plannerChunkType, const std::vector<ChunkPartType>& availableParts) {
		SCOPED_TRACE("Testing recovery of " + ::testing::PrintToString(plannerChunkType));
		SCOPED_TRACE("Testing reading from " + ::testing::PrintToString(availableParts));
		uint32_t blocksInPart = slice_traits::getNumberOfBlocks(plannerChunkType, MFSBLOCKSINCHUNK);

		XorChunkReadPlanner planner(plannerChunkType);
		planner.prepare(availableParts);
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
	std::vector<ChunkPartType> plannedChunkTypes = {
			xor_1_of_2, xor_2_of_2,
			xor_1_of_3, xor_2_of_3, xor_3_of_3,
			slice_traits::xors::ChunkPartType(slice_traits::xors::kMaxXorLevel, 1),
			slice_traits::xors::ChunkPartType(slice_traits::xors::kMaxXorLevel, slice_traits::xors::kMaxXorLevel),
	};
	std::vector<std::vector<ChunkPartType>> availablePartsSets = {
			{standard},
			{xor_1_of_2, xor_2_of_2},
			{xor_1_of_3, xor_2_of_3, xor_3_of_3},
	};
	for (ChunkPartType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}

TEST_F(XorChunkReadPlannerTests, GetPlanRecoverPartFromParity) {
	std::vector<ChunkPartType> plannedChunkTypes = {
			xor_1_of_2, xor_2_of_2,
			xor_1_of_3, xor_2_of_3, xor_3_of_3,
			slice_traits::xors::ChunkPartType(slice_traits::xors::kMaxXorLevel, 1),
			slice_traits::xors::ChunkPartType(slice_traits::xors::kMaxXorLevel, slice_traits::xors::kMaxXorLevel),
	};
	std::vector<std::vector<ChunkPartType>> availablePartsSets = {
			{xor_1_of_2, xor_p_of_2},
			{xor_2_of_2, xor_p_of_2},
			{xor_1_of_3, xor_p_of_3, xor_3_of_3},
	};
	for (ChunkPartType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}

TEST_F(XorChunkReadPlannerTests, GetPlanRecoverParityWithoutParity) {
	std::vector<ChunkPartType> plannedChunkTypes = {
			xor_p_of_2, xor_p_of_3, xor_p_of_6,
			slice_traits::xors::ChunkPartType(slice_traits::xors::kMaxXorLevel, slice_traits::xors::kXorParityPart),
	};
	std::vector<std::vector<ChunkPartType>> availablePartsSets = {
			{standard},
			{xor_1_of_2, xor_2_of_2},
			{xor_1_of_3, xor_2_of_3, xor_3_of_3},
	};
	for (ChunkPartType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}

TEST_F(XorChunkReadPlannerTests, GetPlanRecoverParityFromParity) {
	std::vector<ChunkPartType> plannedChunkTypes = {
			xor_p_of_2, xor_p_of_3, xor_p_of_6,
			slice_traits::xors::ChunkPartType(slice_traits::xors::kMaxXorLevel, slice_traits::xors::kXorParityPart),
	};
	std::vector<std::vector<ChunkPartType>> availablePartsSets = {
			{xor_1_of_2, xor_p_of_2},
			{xor_2_of_2, xor_p_of_2},
			{xor_1_of_3, xor_p_of_3, xor_3_of_3},
	};
	for (ChunkPartType plannedChunkType : plannedChunkTypes) {
		for (const auto& availableParts : availablePartsSets) {
			EXPECT_NO_THROW(verifyPlanner(plannedChunkType, availableParts));
		}
	}
}
