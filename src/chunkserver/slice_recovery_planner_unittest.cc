/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include <gtest/gtest.h>

#include "chunkserver/slice_recovery_planner.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/plan_tester.h"

static ChunkPartType xor_part(int level, int part) {
	return slice_traits::xors::ChunkPartType(level, part);
}

static void checkPartRecovery(std::map<ChunkPartType, std::vector<uint8_t>> part_data,
		ChunkPartType chunk_type, int first_block, int block_count,
		const SliceRecoveryPlanner::PartsContainer &available_parts) {
	SliceRecoveryPlanner planner;

	if (block_count <= 0) {
		block_count = slice_traits::getNumberOfBlocks(chunk_type) - first_block;
	}

	planner.prepare(chunk_type, first_block, block_count, available_parts);

	ASSERT_TRUE(planner.isReadingPossible());

	std::unique_ptr<ReadPlan> plan = planner.buildPlan();

	unittests::ReadPlanTester tester;

	std::cout << to_string(*plan) << std::endl;

	ASSERT_TRUE(tester.executePlan(std::move(plan), part_data) >= 0);

	EXPECT_TRUE(unittests::ReadPlanTester::compareBlocks(
	    tester.output_buffer_, 0, part_data[chunk_type], first_block * MFSBLOCKSIZE, block_count));
}

static void checkPartRecovery(ChunkPartType chunk_type, int first_block, int block_count,
		const SliceRecoveryPlanner::PartsContainer &available_parts) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	unittests::ReadPlanTester::buildData(part_data, available_parts);
	unittests::ReadPlanTester::buildData(part_data, std::vector<ChunkPartType>{chunk_type});

	checkPartRecovery(part_data, chunk_type, first_block, block_count, available_parts);
}

TEST(SliceRecoveryPlannerTests, VerifyRecovery1) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	unittests::ReadPlanTester::buildData(part_data, std::vector<ChunkPartType>{xor_part(3, 1)});

	for (int part = 0; part <= 3; ++part) {
		checkPartRecovery(part_data, xor_part(3, part), 0, -1,
		                  {xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_3_of_3});
	}
}

TEST(SliceRecoveryPlannerTests, VerifyRecovery2) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	unittests::ReadPlanTester::buildData(part_data, std::vector<ChunkPartType>{xor_part(3, 1)});
	unittests::ReadPlanTester::buildData(
	    part_data, std::vector<ChunkPartType>{slice_traits::standard::ChunkPartType()});

	for (int part = 1; part <= 3; ++part) {
		checkPartRecovery(part_data, xor_part(3, part), 0, -1,
		                  {slice_traits::standard::ChunkPartType()});
	}
}

TEST(SliceRecoveryPlannerTests, VerifyRecovery3) {
	checkPartRecovery(xor_p_of_3, 0, -1, {xor_1_of_3, xor_2_of_3, xor_3_of_3});
}

TEST(SliceRecoveryPlannerTests, VerifyRecovery4) {
	checkPartRecovery(xor_p_of_2, 0, -1, {xor_1_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4});
}
