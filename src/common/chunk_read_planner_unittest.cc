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

#include <gtest/gtest.h>

#include "common/chunk_read_planner.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/plan_tester.h"

static ChunkPartType xor_part(int level, int part) {
	return slice_traits::xors::ChunkPartType(level, part);
}

static void checkReadingChunk(std::map<ChunkPartType, std::vector<uint8_t>> &part_data,
		int first_block, int block_count,
		const ChunkReadPlanner::PartsContainer &available_parts) {
	ChunkReadPlanner planner;

	planner.prepare(first_block, block_count, available_parts);

	ASSERT_TRUE(planner.isReadingPossible());

	std::unique_ptr<ReadPlan> plan = planner.buildPlan();

	unittests::ReadPlanTester tester;

	std::cout << to_string(*plan) << std::endl;

	ASSERT_TRUE(tester.executePlan(std::move(plan), part_data) >= 0);

	EXPECT_TRUE(unittests::ReadPlanTester::compareBlocks(
	    tester.output_buffer_, 0, part_data[slice_traits::standard::ChunkPartType()],
	    first_block * MFSBLOCKSIZE, block_count));
}

static void checkReadingChunk(int first_block, int block_count,
		const ChunkReadPlanner::PartsContainer &available_parts) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	unittests::ReadPlanTester::buildData(part_data, available_parts);
	unittests::ReadPlanTester::buildData(
	    part_data, std::vector<ChunkPartType>{slice_traits::standard::ChunkPartType()});

	checkReadingChunk(part_data, first_block, block_count, available_parts);
}

/*
TEST(ChunkReadPlannerTests, Unrecoverable1) {
	checkUnrecoverable(xor_p_of_4, {xor_1_of_4, xor_2_of_4, xor_3_of_4});
}

TEST(ChunkReadPlannerTests, Unrecoverable2) {
	checkUnrecoverable(xor_p_of_4, {xor_1_of_4, xor_2_of_4, xor_3_of_4});
}

TEST(ChunkReadPlannerTests, Unrecoverable3) {
	checkUnrecoverable(xor_p_of_2, {xor_1_of_4, xor_2_of_4, xor_p_of_4});
}

TEST(ChunkReadPlannerTests, Unrecoverable4) {
	checkUnrecoverable(xor_p_of_4, {xor_p_of_2});
}
*/

TEST(ChunkReadPlannerTests, VerifyRead1) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	unittests::ReadPlanTester::buildData(
	    part_data, std::vector<ChunkPartType>{slice_traits::xors::ChunkPartType(5, 1)});
	unittests::ReadPlanTester::buildData(
	    part_data, std::vector<ChunkPartType>{slice_traits::standard::ChunkPartType()});

	for (int i = 1; i <= 10; ++i) {
		checkReadingChunk(part_data, 0, i, {xor_part(5, 0), xor_part(5, 1), xor_part(5, 2),
		                                    xor_part(5, 3), xor_part(5, 4), xor_part(5, 5)});
	}
}

TEST(ChunkReadPlannerTests, VerifyRead2) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	unittests::ReadPlanTester::buildData(
	    part_data, std::vector<ChunkPartType>{slice_traits::xors::ChunkPartType(5, 1)});
	unittests::ReadPlanTester::buildData(
	    part_data, std::vector<ChunkPartType>{slice_traits::standard::ChunkPartType()});

	for (int i = 1; i <= 10; ++i) {
		checkReadingChunk(part_data, i, 2, {xor_part(5, 0), xor_part(5, 1), xor_part(5, 2),
		                                    xor_part(5, 3), xor_part(5, 4), xor_part(5, 5)});
	}
}

TEST(ChunkReadPlannerTests, VerifyRead3) {
	checkReadingChunk(0, 10, {xor_p_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4});
}

TEST(ChunkReadPlannerTests, VerifyRead4) {
	checkReadingChunk(10, 100, {xor_p_of_7, xor_1_of_7, xor_2_of_7, xor_3_of_7, xor_4_of_7,
	                            xor_5_of_7, xor_6_of_7, xor_7_of_7});
}
