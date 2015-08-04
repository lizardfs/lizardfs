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
#include "common/standard_chunk_read_planner.h"

#include <algorithm>
#include <sstream>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "common/slice_traits.h"
#include "protocol/MFSCommunication.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"
#include "unittests/plan_tester.h"

#define VERIFY_PLAN_FOR(planner, firstBlock, count) \
	do {\
		auto plan = planner.buildPlanFor(firstBlock, count); \
		SCOPED_TRACE("Veryfing plan " + ::testing::PrintToString(*plan)); \
		ASSERT_EQ(\
				unittests::PlanTester::expectedAnswer(standard, firstBlock, count), \
				unittests::PlanTester::executePlan(*plan, planner.partsToUse(), count)); \
	} while (0)

class StandardReadPlannerTests : public testing::Test {
public:
	StandardReadPlannerTests() {
		auto partsUsedInTests = std::vector<ChunkPartType>{
				standard,
				xor_p_of_2, xor_1_of_2, xor_2_of_2,
				xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_3_of_3,
				xor_p_of_6, xor_1_of_6, xor_2_of_6, xor_3_of_6, xor_4_of_6, xor_5_of_6, xor_6_of_6,
		};
	}

	void verifyPlanner(const std::vector<ChunkPartType>& availableParts) {
		StandardChunkReadPlanner planner;
		planner.prepare(availableParts);
		VERIFY_PLAN_FOR(planner, 0, 1); // blocks: 0
		VERIFY_PLAN_FOR(planner, 0, 2); // blocks: 0, 1
		VERIFY_PLAN_FOR(planner, 0, 3); // blocks: 0, 1, 2
		VERIFY_PLAN_FOR(planner, 0, 4); // blocks: 0, 1, 2, 3
		VERIFY_PLAN_FOR(planner, 1, 1); // blocks: 1
		VERIFY_PLAN_FOR(planner, 1, 2); // blocks: 1, 2
		VERIFY_PLAN_FOR(planner, 1, 3); // blocks: 1, 2, 3
		VERIFY_PLAN_FOR(planner, 1, 4); // blocks: 1, 2, 3, 4
		VERIFY_PLAN_FOR(planner, 2, 1); // blocks: 2
		VERIFY_PLAN_FOR(planner, 2, 2); // blocks: 2, 3
		VERIFY_PLAN_FOR(planner, 2, 3); // blocks: 2, 3, 4
		VERIFY_PLAN_FOR(planner, 2, 4); // blocks: 2, 3, 4, 5
		VERIFY_PLAN_FOR(planner, 3, 1); // blocks: 3
		VERIFY_PLAN_FOR(planner, 3, 2); // blocks: 3, 4
		VERIFY_PLAN_FOR(planner, 3, 3); // blocks: 3, 4, 5
		VERIFY_PLAN_FOR(planner, 3, 4); // blocks: 3, 4, 5, 6
		VERIFY_PLAN_FOR(planner, MFSBLOCKSINCHUNK - 4, 4); // last four blocks
		VERIFY_PLAN_FOR(planner, MFSBLOCKSINCHUNK - 3, 3); // last three blocks
		VERIFY_PLAN_FOR(planner, MFSBLOCKSINCHUNK - 2, 2); // last two blocks
		VERIFY_PLAN_FOR(planner, MFSBLOCKSINCHUNK - 1, 1); // last block
		VERIFY_PLAN_FOR(planner, MFSBLOCKSINCHUNK - 4, 1);
		VERIFY_PLAN_FOR(planner, MFSBLOCKSINCHUNK - 3, 1);
		VERIFY_PLAN_FOR(planner, MFSBLOCKSINCHUNK - 2, 1);
		VERIFY_PLAN_FOR(planner, 0, MFSBLOCKSINCHUNK); // all blocks
		VERIFY_PLAN_FOR(planner, 1, MFSBLOCKSINCHUNK - 1);
		VERIFY_PLAN_FOR(planner, 2, MFSBLOCKSINCHUNK - 2);
		VERIFY_PLAN_FOR(planner, 3, MFSBLOCKSINCHUNK - 3);
		VERIFY_PLAN_FOR(planner, 4, MFSBLOCKSINCHUNK - 4);
	}
};

TEST_F(StandardReadPlannerTests, ChoosePartsToUseStandard1) {
	std::vector<ChunkPartType> chunks{standard, standard, standard};
	std::vector<ChunkPartType> expected{standard};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseStandard2) {
	std::vector<ChunkPartType> chunks{standard, xor_1_of_2};
	std::vector<ChunkPartType> expected{standard};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseStandard3) {
	std::vector<ChunkPartType> chunks{standard, xor_p_of_3};
	std::vector<ChunkPartType> expected{standard};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseStandard4) {
	std::vector<ChunkPartType> chunks{standard, xor_1_of_2, xor_p_of_2};
	std::vector<ChunkPartType> expected{standard};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseXor1) {
	std::vector<ChunkPartType> chunks{xor_p_of_2, xor_1_of_2, xor_2_of_2};
	std::vector<ChunkPartType> expected{xor_1_of_2, xor_2_of_2};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseXor2) {
	std::vector<ChunkPartType> chunks{xor_1_of_2, xor_2_of_2};
	std::vector<ChunkPartType> expected{xor_1_of_2, xor_2_of_2};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseXor3) {
	std::vector<ChunkPartType> chunks{xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_3_of_3};
	std::vector<ChunkPartType> expected{xor_1_of_3, xor_2_of_3, xor_3_of_3};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseXorParity1) {
	std::vector<ChunkPartType> chunks{xor_p_of_2, xor_2_of_2};
	std::vector<ChunkPartType> expected{xor_p_of_2, xor_2_of_2};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseXorParity2) {
	std::vector<ChunkPartType> chunks{xor_p_of_3, xor_1_of_3, xor_1_of_2, xor_p_of_2};
	std::vector<ChunkPartType> expected{xor_p_of_2, xor_1_of_2};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseHighestXor1) {
	std::vector<ChunkPartType> chunks{xor_1_of_2, xor_2_of_2, xor_1_of_3, xor_2_of_3, xor_3_of_3};
	std::vector<ChunkPartType> expected{xor_1_of_3, xor_2_of_3, xor_3_of_3};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseHighestXor2) {
	std::vector<ChunkPartType> chunks{xor_1_of_2, xor_2_of_2, xor_1_of_3, xor_2_of_3, xor_p_of_3};
	std::vector<ChunkPartType> expected{xor_1_of_2, xor_2_of_2};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseHighestXorParity1) {
	std::vector<ChunkPartType> chunks{xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_1_of_2, xor_p_of_2};
	std::vector<ChunkPartType> expected{xor_p_of_3, xor_1_of_3, xor_2_of_3};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_EQ(expected, planner.partsToUse());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseImpossible1) {
	std::vector<ChunkPartType> chunks { };
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_FALSE(planner.isReadingPossible());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseImpossible2) {
	std::vector<ChunkPartType> chunks{xor_1_of_2, xor_2_of_3, xor_3_of_3};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_FALSE(planner.isReadingPossible());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseImpossible3) {
	std::vector<ChunkPartType> chunks{xor_1_of_6, xor_2_of_6, xor_3_of_6, xor_5_of_6, xor_6_of_6};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_FALSE(planner.isReadingPossible());
}

TEST_F(StandardReadPlannerTests, ChoosePartsToUseImpossible4) {
	std::vector<ChunkPartType> chunks{xor_p_of_6, xor_2_of_6, xor_3_of_6, xor_5_of_6, xor_6_of_6};
	StandardChunkReadPlanner planner;
	planner.prepare(chunks);
	EXPECT_FALSE(planner.isReadingPossible());
}

TEST_F(StandardReadPlannerTests, GetPlanForStandard) {
	verifyPlanner({standard});
}

TEST_F(StandardReadPlannerTests, GetPlanForXorLevel2) {
	verifyPlanner({xor_1_of_2, xor_2_of_2});
}

TEST_F(StandardReadPlannerTests, GetPlanForXorLevel2WithoutPart2) {
	verifyPlanner({xor_1_of_2, xor_p_of_2});
}

TEST_F(StandardReadPlannerTests, GetPlanForXorLevel2WithoutPart1) {
	verifyPlanner({xor_2_of_2, xor_p_of_2});
}

TEST_F(StandardReadPlannerTests, GetPlanForXorLevel6WithoutPart3) {
	verifyPlanner({xor_1_of_6, xor_6_of_6, xor_2_of_6, xor_5_of_6, xor_p_of_6, xor_4_of_6});
}

TEST_F(StandardReadPlannerTests, GetPlanForXorLevel3) {
	verifyPlanner({xor_1_of_3, xor_2_of_3, xor_3_of_3});
}

TEST_F(StandardReadPlannerTests, GetPlanForXorLevel3WithoutPart2) {
	verifyPlanner({xor_1_of_3, xor_3_of_3, xor_p_of_3});
}

TEST_F(StandardReadPlannerTests, GetPlanForMaxXorLevel) {
	int level = slice_traits::xors::kMaxXorLevel;
	std::vector<ChunkPartType> parts;
	for (int part = 1; part <= level; ++part) {
		parts.push_back(slice_traits::xors::ChunkPartType(level, part));
	}
	verifyPlanner(parts);
}
