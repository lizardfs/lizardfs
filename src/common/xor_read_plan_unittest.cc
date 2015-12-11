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

#include "common/slice_read_planner.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/plan_tester.h"

void checkUnrecoverable(ChunkPartType target_part,
		const SliceReadPlanner::PartsContainer &available_parts) {
	SliceReadPlanner planner;
	SliceReadPlanner::PartIndexContainer parts;

	parts.push_back(target_part.getSlicePart());

	planner.prepare(target_part.getSliceType(), parts, available_parts);
	EXPECT_FALSE(planner.isReadingPossible());
}

void checkReadingParts(int first_block, int block_count,
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

	std::cout << to_string(*plan) << std::endl;

	ASSERT_TRUE(tester.executePlan(std::move(plan), part_data) >= 0);

	int buffer_offset = 0;
	for (const auto &part : target_parts) {
		EXPECT_TRUE(unittests::ReadPlanTester::compareBlocks(
		    tester.output_buffer_, buffer_offset, part_data[part], first_block * MFSBLOCKSIZE,
		    block_count));
		buffer_offset += buffer_step;
	}
}

TEST(XorReadPlanTests, Unrecoverable1) {
	checkUnrecoverable(xor_p_of_4, {xor_1_of_4, xor_2_of_4, xor_3_of_4});
}

TEST(XorReadPlanTests, Unrecoverable2) {
	checkUnrecoverable(xor_p_of_4, {xor_1_of_4, xor_2_of_4, xor_3_of_4});
}

TEST(XorReadPlanTests, Unrecoverable3) {
	checkUnrecoverable(xor_p_of_2, {xor_1_of_4, xor_2_of_4, xor_p_of_4});
}

TEST(XorReadPlanTests, Unrecoverable4) {
	checkUnrecoverable(xor_p_of_4, {xor_p_of_2});
}

TEST(XorReadPlanTests, VerifyRead1) {
	checkReadingParts(0, 10, {xor_1_of_4}, {xor_p_of_4, xor_2_of_4, xor_3_of_4, xor_4_of_4});
}

TEST(XorReadPlanTests, VerifyRead2) {
	checkReadingParts(0, 10, {xor_2_of_4, xor_1_of_4}, {xor_1_of_4, xor_2_of_4});
}

TEST(XorReadPlanTests, VerifyRead3) {
	checkReadingParts(
	    10, 11, {xor_5_of_7, xor_1_of_7},
	    {xor_p_of_7, xor_2_of_7, xor_3_of_7, xor_4_of_7, xor_5_of_7, xor_6_of_7, xor_7_of_7});
}

TEST(XorReadPlanTests, VerifyRead4) {
	checkReadingParts(10, 11, {xor_5_of_7, xor_1_of_7},
	                  {xor_p_of_7, xor_1_of_7, xor_2_of_7, xor_3_of_7, xor_4_of_7, xor_5_of_7,
	                   xor_6_of_7, xor_7_of_7});
}
