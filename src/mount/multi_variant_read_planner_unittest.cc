#include "common/platform.h"
#include "mount/multi_variant_read_planner.h"

#include <algorithm>
#include <sstream>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"
#include "unittests/plan_tester.h"

typedef std::set<ChunkType> Set;

// For the given set of parts, returns set of parts which should be used in the basic
// variant of a plan of reading the whole chunk
static Set getPartsToUseInBasicPlan(std::vector<ChunkType> availableParts,
		std::map<ChunkType, float> scores = {}) {
	MultiVariantReadPlanner planner;
	planner.setScores(std::move(scores));
	planner.prepare(availableParts);
	auto plan = planner.buildPlanFor(0, MFSBLOCKSINCHUNK);
	Set ret;
	for (const auto& chunkTypeAndReadOperation : plan->basicReadOperations) {
		ret.insert(chunkTypeAndReadOperation.first);
	}
	return ret;
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
