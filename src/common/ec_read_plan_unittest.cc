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

#include "common/slice_read_planner.h"
#include "common/chunk_read_planner.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/plan_tester.h"

ChunkPartType ec(int data_part_count, int parity_part_count, int part_index) {
	return slice_traits::ec::ChunkPartType(data_part_count, parity_part_count, part_index);
}

static void checkReadingParts(int first_block, int block_count,
		const SliceReadPlanner::PartsContainer &target_parts,
		const SliceReadPlanner::PartsContainer &available_parts) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	for (const auto &part : target_parts) {
		block_count =
		    std::min((int)slice_traits::getNumberOfBlocks(part) - first_block, block_count);
	}
	unittests::ReadPlanTester::buildData(part_data, available_parts);

	SliceReadPlanner planner;
	SliceReadPlanner::PartIndexContainer parts;

	for (const auto &part : target_parts) {
		parts.push_back(part.getSlicePart());
	}
	planner.prepare(target_parts[0].getSliceType(), parts, available_parts);

	ASSERT_TRUE(planner.isReadingPossible());

	std::unique_ptr<ReadPlan> plan = planner.buildPlanFor(first_block, block_count);

	unittests::ReadPlanTester tester;
	int buffer_step = dynamic_cast<SliceReadPlan *>(plan.get())->buffer_part_size;

	std::cout << to_string(*plan) << "\n";

	ASSERT_TRUE(tester.executePlan(std::move(plan), part_data) >= 0);

	int buffer_offset = 0;
	for (const auto &part : target_parts) {
		EXPECT_TRUE(unittests::ReadPlanTester::compareBlocks(
		    tester.output_buffer_, buffer_offset, part_data[part], first_block * MFSBLOCKSIZE,
		    block_count));
		buffer_offset += buffer_step;
	}
}

static void checkReadingChunk(int first_block, int block_count,
		const ChunkReadPlanner::PartsContainer &available_parts) {
	std::map<ChunkPartType, std::vector<uint8_t>> part_data;

	unittests::ReadPlanTester::buildData(part_data, available_parts);
	unittests::ReadPlanTester::buildData(
	    part_data, std::vector<ChunkPartType>{slice_traits::standard::ChunkPartType()});

	ChunkReadPlanner planner;

	planner.prepare(first_block, block_count, available_parts);

	ASSERT_TRUE(planner.isReadingPossible());

	std::unique_ptr<ReadPlan> plan = planner.buildPlan();

	unittests::ReadPlanTester tester;

	std::cout << to_string(*plan) << "\n";

	ASSERT_TRUE(tester.executePlan(std::move(plan), part_data) >= 0);

	EXPECT_TRUE(unittests::ReadPlanTester::compareBlocks(
	    tester.output_buffer_, 0, part_data[slice_traits::standard::ChunkPartType()],
	    first_block * MFSBLOCKSIZE, block_count));
}

TEST(ECReadPlanTests, VerifyRead1) {
	checkReadingParts(0, 10, {ec(3, 2, 0)}, {ec(3, 2, 1), ec(3, 2, 2), ec(3, 2, 3), ec(3, 2, 4)});
}

TEST(ECReadPlanTests, VerifyRead2) {
	checkReadingParts(0, 10, {ec(3, 2, 0), ec(3, 2, 1)}, {ec(3, 2, 0), ec(3, 2, 1)});
}

TEST(ECReadPlanTests, VerifyRead3) {
	checkReadingParts(10, 11, {ec(3, 2, 0), ec(3, 2, 1)}, {ec(3, 2, 1), ec(3, 2, 2), ec(3, 2, 4)});
}

TEST(ECReadPlanTests, VerifyRead4) {
	checkReadingParts(10, 11, {ec(3, 2, 4), ec(3, 2, 0)}, {ec(3, 2, 1), ec(3, 2, 2), ec(3, 2, 3)});
}

TEST(ECReadPlanTests, VerifyRead5) {
	checkReadingParts(10, 11, {ec(3, 2, 4)}, {ec(3, 2, 1), ec(3, 2, 2), ec(3, 2, 3)});
}

TEST(ECReadPlanTests, VerifyChunkRead1) {
	checkReadingChunk(0, 10, {ec(3, 2, 1), ec(3, 2, 2), ec(3, 2, 4)});
}
