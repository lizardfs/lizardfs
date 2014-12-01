#include "common/platform.h"
#include "master/chunk_copies_calculator.h"

#include <algorithm>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"

static const Goal& getDefaultGoal(uint8_t goalId) {
	static std::map<uint8_t, Goal> defaultGoals;
	if (defaultGoals.count(goalId) == 0) {
		defaultGoals[goalId] = Goal::getDefaultGoal(goalId);
	}
	return defaultGoals[goalId];
}

// Some custom goals for these tests
static Goal us_us("us_us", {
		{"us", 2},
});
static Goal eu_eu("eu_eu", {
		{"eu", 2},
});
static Goal us_eu("us_eu", {
		{"us", 1},
		{"eu", 1},
});
static Goal us_eu_any("us_eu_any", {
		{"us", 1},
		{"eu", 1},
		{"_", 1},
});
static Goal us_us_any("us_us_any", {
		{"us", 2},
		{"_", 1},
});
static Goal us_any_any("us_any_any", {
		{"us", 1},
		{"_", 2},
});
static Goal us_eu_any_any("us_eu_any_any", {
		{"us", 1},
		{"eu", 1},
		{"_", 2},
});
static Goal any_any("any_any", {
		{"_", 2},
});

static void checkPartsToRecoverWithLabels(
		std::vector<std::pair<ChunkType, MediaLabel>> available,
		Goal goal,
		std::vector<ChunkType> expectedPartsToRecover) {
	SCOPED_TRACE("Testing goal " + goal.name());
	SCOPED_TRACE("Available parts: " + ::testing::PrintToString(available));
	ChunkCopiesCalculator calculator(&goal);
	for (const auto& part : available) {
		calculator.addPart(part.first, &part.second);
	}
	std::vector<ChunkType> actualPartsToRecover = calculator.getPartsToRecover();
	std::sort(expectedPartsToRecover.begin(), expectedPartsToRecover.end());
	std::sort(actualPartsToRecover.begin(), actualPartsToRecover.end());
	EXPECT_EQ(expectedPartsToRecover, actualPartsToRecover);
	EXPECT_EQ(expectedPartsToRecover.size(), calculator.countPartsToRecover());
}

static void checkPartsToRecover(
		std::vector<ChunkType> available,
		uint8_t goalId,
		std::vector<ChunkType> expectedPartsToRemove) {
	std::vector<std::pair<ChunkType, MediaLabel>> availablePartsWithLabel;
	for (const auto& part : available) {
		availablePartsWithLabel.emplace_back(part, kMediaLabelWildcard);
	}
	checkPartsToRecoverWithLabels(availablePartsWithLabel,
			getDefaultGoal(goalId),
			expectedPartsToRemove);
}

static ChunkCopiesCalculator calculator(const std::vector<ChunkType>& parts, uint8_t goal = 2) {
	ChunkCopiesCalculator calculator(&getDefaultGoal(goal));
	for (const auto& part : parts) {
		calculator.addPart(part, &kMediaLabelWildcard);
	}
	return calculator;
}

static void checkPartsToRemoveWithLabels(
		std::vector<std::pair<ChunkType, MediaLabel>> available,
		Goal goal,
		std::vector<ChunkType> expectedPartsToRemove) {
	SCOPED_TRACE("Testing goal " + goal.name());
	SCOPED_TRACE("Available parts: " + ::testing::PrintToString(available));
	ChunkCopiesCalculator calculator(&goal);
	for (const auto& part : available) {
		calculator.addPart(part.first, &part.second);
	}
	std::vector<ChunkType> actualPartsToRemove = calculator.getPartsToRemove();
	std::sort(expectedPartsToRemove.begin(), expectedPartsToRemove.end());
	std::sort(actualPartsToRemove.begin(), actualPartsToRemove.end());
	EXPECT_EQ(expectedPartsToRemove, actualPartsToRemove);
	EXPECT_EQ(expectedPartsToRemove.size(), calculator.countPartsToRemove());
}

static void checkPartsToRemove(
		std::vector<ChunkType> available,
		uint8_t goalId,
		std::vector<ChunkType> expectedPartsToRemove) {
	std::vector<std::pair<ChunkType, MediaLabel>> availablePartsWithLabel;
	for (const auto& part : available) {
		availablePartsWithLabel.emplace_back(part, kMediaLabelWildcard);
	}
	checkPartsToRemoveWithLabels(availablePartsWithLabel,
			getDefaultGoal(goalId),
			expectedPartsToRemove);
}

TEST(ChunkCopiesCalculatorTests, GetPartsToRecover) {
	checkPartsToRecover({standard, standard, standard, standard, standard}, 0, {});
	checkPartsToRecover({xor_2_of_2, xor_p_of_2}, 0, {});
	checkPartsToRecover({xor_1_of_3}, 0, {});

	checkPartsToRecover({standard, standard, standard, standard, standard}, 3, {});
	checkPartsToRecover({standard, standard, standard, standard}, 3, {});
	checkPartsToRecover({standard, standard, standard}, 3, {});
	checkPartsToRecover({standard, standard}, 3, {standard});
	checkPartsToRecover({standard}, 3, {standard, standard});
	checkPartsToRecover({}, 3, {standard, standard, standard});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2, xor_p_of_2}, goal::xorLevelToGoal(2), {});
	checkPartsToRecover({xor_2_of_2, xor_p_of_2}, goal::xorLevelToGoal(2), {xor_1_of_2});
	checkPartsToRecover({xor_1_of_2, xor_p_of_2}, goal::xorLevelToGoal(2), {xor_2_of_2});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, goal::xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({}, goal::xorLevelToGoal(2), {xor_1_of_2, xor_2_of_2, xor_p_of_2});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 1, {standard});
	checkPartsToRecover({xor_1_of_2, xor_p_of_2}, 1, {standard});
	checkPartsToRecover({xor_2_of_2, xor_p_of_2}, 1, {standard});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 1, {standard});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 2, {standard, standard});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 3, {standard, standard, standard});

	checkPartsToRecover({standard}, 1, {});
	checkPartsToRecover({standard}, goal::xorLevelToGoal(2), {xor_1_of_2, xor_2_of_2, xor_p_of_2});
	checkPartsToRecover({standard}, goal::xorLevelToGoal(3), {xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2, standard},   goal::xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2, xor_p_of_3}, goal::xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2, xor_2_of_2}, goal::xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_2, xor_1_of_2, xor_2_of_2}, goal::xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3}, goal::xorLevelToGoal(2),
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});

	uint8_t goalXorMax = goal::xorLevelToGoal(goal::kMaxXorLevel);
	std::vector<ChunkType> chunkTypesMax = {ChunkType::getXorParityChunkType(goal::kMaxXorLevel)};
	for (ChunkType::XorPart part = 1; part <= goal::kMaxXorLevel; ++part) {
		chunkTypesMax.push_back(ChunkType::getXorChunkType(goal::kMaxXorLevel, part));
	}

	checkPartsToRecover(chunkTypesMax, goalXorMax, {});
	checkPartsToRecover({standard}, goalXorMax, chunkTypesMax);
	checkPartsToRecover({xor_1_of_2, xor_2_of_2, xor_p_of_2}, goalXorMax, chunkTypesMax);
	for (unsigned i = 0; i < chunkTypesMax.size(); ++i) {
		std::vector<ChunkType> expected = {chunkTypesMax[i]};
		std::vector<ChunkType> available = chunkTypesMax;
		available.erase(available.begin() + i);
		checkPartsToRecover(available, goalXorMax, expected);
	}
}

TEST(ChunkCopiesCalculatorTests, GetPartsToRecoverWithcustomGoals) {
#define check checkPartsToRecoverWithLabels
	check({{standard, "us"}}, us_us, {standard});
	check({{standard, "us"}}, us_eu, {standard});
	check({{standard, "us"}}, eu_eu, {standard, standard});
	check({{standard, "us"}}, us_eu_any, {standard, standard});
	check({{standard, "eu"}}, us_eu_any, {standard, standard});
	check({{standard, "cn"}}, us_eu_any, {standard, standard});
	check({{standard, "us"}}, any_any, {standard});

	check({{standard, "us"}, {xor_1_of_2, "us"}}, us_us, {standard});
	check({{standard, "us"}, {xor_1_of_2, "us"}}, us_eu, {standard});
	check({{standard, "us"}, {xor_1_of_2, "us"}}, eu_eu, {standard, standard});
	check({{standard, "us"}, {xor_1_of_2, "us"}}, us_eu_any, {standard, standard});
	check({{standard, "eu"}, {xor_1_of_2, "us"}}, us_eu_any, {standard, standard});
	check({{standard, "cn"}, {xor_1_of_2, "us"}}, us_eu_any, {standard, standard});
	check({{standard, "us"}, {xor_1_of_2, "us"}}, any_any, {standard});

	check({{standard, "us"}, {standard, "us"}}, us_us, {});
	check({{standard, "us"}, {standard, "us"}}, any_any, {});
	check({{standard, "us"}, {standard, "us"}}, us_eu, {standard});
	check({{standard, "us"}, {standard, "us"}}, eu_eu, {standard, standard});
	check({{standard, "us"}, {standard, "us"}}, us_eu_any, {standard});
	check({{standard, "us"}, {standard, "us"}}, us_eu_any_any, {standard, standard});

	check({{standard, "cn"}}, us_eu_any_any, {standard, standard, standard});
	check({{standard, "cn"}, {standard, "cn"}}, us_eu_any_any, {standard, standard});
	check({{standard, "cn"}, {standard, "cn"}, {standard, "cn"}}, us_eu_any_any, {standard, standard});

	check({{standard, "cn"}}, us_eu_any_any, {standard, standard, standard});
	check({{standard, "cn"}, {xor_1_of_2, "us"}}, us_eu_any_any, {standard, standard, standard});
	check({{standard, "cn"}, {xor_1_of_2, "us"}, {xor_2_of_2, "us"}}, us_eu_any_any, {standard, standard, standard});
#undef check
}

TEST(ChunkCopiesCalculatorTests, GetPartsToRemoveWithcustomGoals) {
#define check checkPartsToRemoveWithLabels
	check({{standard, "eu"}}, us_us, {standard});
	check({{standard, "eu"}}, us_eu, {});
	check({{standard, "eu"}}, eu_eu, {});
	check({{standard, "us"}}, eu_eu, {standard});

	check({{standard, "eu"}, {xor_1_of_2, "us"}}, us_us, {standard, xor_1_of_2});
	check({{standard, "eu"}, {xor_1_of_2, "cn"}}, us_us, {standard, xor_1_of_2});
	check({{standard, "eu"}, {xor_1_of_2, "us"}}, us_eu, {xor_1_of_2});
	check({{standard, "eu"}, {xor_1_of_2, "us"}}, eu_eu, {xor_1_of_2});
	check({{standard, "us"}, {xor_1_of_2, "us"}}, eu_eu, {standard, xor_1_of_2});
	check({{standard, "cn"}, {xor_1_of_2, "us"}}, us_eu_any, {xor_1_of_2});

	check({{standard, "cn"}}, us_eu_any, {});
	check({{standard, "cn"}, {standard, "cn"}}, us_eu_any, {standard});
	check({{standard, "cn"}, {standard, "cn"}, {standard, "cn"}}, us_eu_any, {standard, standard});
	check({{xor_1_of_2, "us"}, {xor_2_of_2, "eu"}, {xor_p_of_2, "cn"}},
			us_eu_any, {xor_1_of_2, xor_2_of_2, xor_p_of_2});

	check({{standard, "eu"}, {standard, "us"}}, us_eu, {});
	check({{standard, "eu"}, {standard, "cn"}}, us_eu, {standard});
	check({{standard, "eu"}, {standard, "cn"}}, us_eu_any, {});
	check({{standard, "eu"}, {standard, "cn"}}, us_eu_any_any, {});
	check({{standard, "cn"}, {standard, "cn"}}, us_eu_any_any, {});
	check({{standard, "cn"}, {standard, "cn"}, {standard, "us"}}, us_eu_any_any, {});
	check({{standard, "cn"}, {standard, "cn"}, {standard, "cn"}}, us_eu_any_any, {standard});
	check({{standard, "cn"}, {standard, "cn"}, {standard, "cn"}, {standard, "cn"}},
			us_eu_any_any, {standard, standard});

	check({{standard, "eu"}, {standard, "us"}, {xor_1_of_2, "eu"}}, us_eu, {xor_1_of_2});
	check({{standard, "eu"}, {standard, "cn"}, {xor_1_of_2, "eu"}}, us_eu, {standard, xor_1_of_2});
	check({{standard, "eu"}, {standard, "cn"}, {xor_1_of_2, "eu"}}, us_eu_any, {xor_1_of_2});
	check({{standard, "eu"}, {standard, "cn"}, {xor_1_of_2, "eu"}}, us_eu_any_any, {xor_1_of_2});
	check({{standard, "cn"}, {standard, "cn"}, {xor_1_of_2, "eu"}}, us_eu_any_any, {xor_1_of_2});
	check({{standard, "cn"}, {standard, "cn"}, {standard, "us"}, {xor_1_of_2, "eu"}},
			us_eu_any_any, {xor_1_of_2});
	check({{standard, "cn"}, {standard, "cn"}, {standard, "cn"}, {xor_1_of_2, "eu"}},
			us_eu_any_any, {standard, xor_1_of_2});

#undef check
}

TEST(ChunkCopiesCalculatorTests, IsRecoveryPossible) {
	EXPECT_FALSE(calculator({}).isRecoveryPossible());
	EXPECT_TRUE(calculator({standard}).isRecoveryPossible());
	EXPECT_TRUE(calculator({standard, standard}).isRecoveryPossible());
	EXPECT_TRUE(calculator({standard, standard, standard}).isRecoveryPossible());

	EXPECT_TRUE(calculator({xor_1_of_2, xor_2_of_2, xor_p_of_2}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, xor_2_of_2}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, xor_p_of_2}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_2_of_2, xor_p_of_2}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_1_of_2}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_2_of_2}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_p_of_2}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_1_of_2, xor_1_of_2}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_2_of_2, xor_2_of_2}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_p_of_2, xor_p_of_2}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, standard}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_2_of_2, standard}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_p_of_2, standard}).isRecoveryPossible());

	EXPECT_TRUE(calculator({xor_1_of_3, xor_2_of_3, xor_p_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_1_of_3, xor_2_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_1_of_3, xor_p_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_1_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_2_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_3_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_p_of_3}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_1_of_3, standard}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_2_of_3, standard}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_3_of_3, standard}).isRecoveryPossible());
	EXPECT_TRUE(calculator({xor_p_of_3, standard}).isRecoveryPossible());

	EXPECT_FALSE(calculator({xor_1_of_2, xor_2_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_1_of_2, xor_p_of_3}).isRecoveryPossible());
	EXPECT_FALSE(calculator({xor_2_of_2, xor_p_of_3}).isRecoveryPossible());

	std::vector<ChunkType> chunkTypesMax = {ChunkType::getXorParityChunkType(goal::kMaxXorLevel)};
	for (ChunkType::XorPart part = 1; part <= goal::kMaxXorLevel; ++part) {
		chunkTypesMax.push_back(ChunkType::getXorChunkType(goal::kMaxXorLevel, part));
	}

	EXPECT_TRUE(calculator(chunkTypesMax).isRecoveryPossible());
	for (unsigned i = 0; i < chunkTypesMax.size(); ++i) {
		std::vector<ChunkType> available = chunkTypesMax;
		available.erase(available.begin() + i); // Only i'th part is missing
		EXPECT_TRUE(calculator(available).isRecoveryPossible());
		available.erase(available.begin()); // Now two parts are missing
		EXPECT_FALSE(calculator(available).isRecoveryPossible());
		available.push_back(standard);
		EXPECT_TRUE(calculator(available).isRecoveryPossible());
	}
}

TEST(ChunkCopiesCalculatorTests, GetPartsToRemove) {
	checkPartsToRemove({standard}, 0, {standard});
	checkPartsToRemove({standard, standard}, 0, {standard, standard});
	checkPartsToRemove({xor_1_of_2}, 0, {xor_1_of_2});
	checkPartsToRemove({xor_1_of_2, standard}, 0, {xor_1_of_2, standard});
	checkPartsToRemove({xor_1_of_2, standard}, 0, {xor_1_of_2, standard});
	checkPartsToRemove({xor_1_of_2, standard}, 0, {xor_1_of_2, standard});

	checkPartsToRemove({standard}, 1, {});
	checkPartsToRemove({standard, standard}, 1, {standard});
	checkPartsToRemove({standard, standard, standard}, 1, {standard, standard});
	checkPartsToRemove({standard}, 4, {});
	checkPartsToRemove({standard, standard}, 4, {});
	checkPartsToRemove({standard, standard, standard}, 4, {});
	checkPartsToRemove({standard, standard, standard, standard}, 4, {});
	checkPartsToRemove({standard, standard, standard, standard, standard}, 4, {standard});
	checkPartsToRemove({standard}, goal::xorLevelToGoal(2), {standard});

	checkPartsToRemove({xor_1_of_2}, 1, {xor_1_of_2});
	checkPartsToRemove({xor_1_of_2, xor_1_of_3}, goal::xorLevelToGoal(2), {xor_1_of_3});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_1_of_3}, goal::xorLevelToGoal(2), {xor_1_of_3});
	checkPartsToRemove({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3,
			xor_1_of_2, xor_2_of_2, xor_p_of_2},
			goal::xorLevelToGoal(2),
			{xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3});
	checkPartsToRemove({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3,
			xor_1_of_2, xor_2_of_2, xor_p_of_2},
			goal::xorLevelToGoal(3),
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_p_of_2, xor_1_of_3},
			goal::xorLevelToGoal(3),
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});

	checkPartsToRemove({xor_1_of_2, standard}, 1, {xor_1_of_2});
	checkPartsToRemove({xor_1_of_2, standard}, goal::xorLevelToGoal(2), {standard});
	checkPartsToRemove({xor_1_of_2, standard}, goal::xorLevelToGoal(3), {xor_1_of_2, standard});
	checkPartsToRemove({xor_1_of_2, standard, standard}, 1, {xor_1_of_2, standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, standard}, 1, {xor_1_of_2, xor_2_of_2});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, standard}, goal::xorLevelToGoal(2), {standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_p_of_2, standard},
			1,
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_p_of_2, standard},
			goal::xorLevelToGoal(2),
			{standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			1,
			{xor_1_of_2, xor_2_of_3, xor_p_of_7});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			goal::xorLevelToGoal(2),
			{xor_2_of_3, xor_p_of_7, standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			goal::xorLevelToGoal(3),
			{xor_1_of_2, xor_p_of_7, standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			goal::xorLevelToGoal(7),
			{xor_1_of_2, xor_2_of_3, standard});
}

TEST(ChunkCopiesCalculatorTests, IsWritingPossible) {
	EXPECT_FALSE(calculator({}).isWritingPossible());
	EXPECT_TRUE(calculator({standard}).isWritingPossible());
	EXPECT_TRUE(calculator({standard, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({standard, standard, standard}).isWritingPossible());

	EXPECT_TRUE(calculator({xor_1_of_2, xor_2_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, xor_2_of_2}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_2_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_2_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_p_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_2, xor_1_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_2_of_2, xor_2_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_p_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_2_of_2, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_p_of_2, standard}).isWritingPossible());

	EXPECT_TRUE(calculator({xor_1_of_3, xor_2_of_3, xor_p_of_3}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_1_of_3, xor_2_of_3, xor_3_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_3, xor_2_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_3, xor_p_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_2_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_3_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_p_of_3}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_1_of_3, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_2_of_3, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_3_of_3, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_p_of_3, standard}).isWritingPossible());

	EXPECT_FALSE(calculator({xor_1_of_2, xor_2_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_2, xor_p_of_3}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_2_of_2, xor_p_of_3}).isWritingPossible());

	std::vector<ChunkType> chunkTypesMax = {ChunkType::getXorParityChunkType(goal::kMaxXorLevel)};
	for (ChunkType::XorPart part = 1; part <= goal::kMaxXorLevel; ++part) {
		chunkTypesMax.push_back(ChunkType::getXorChunkType(goal::kMaxXorLevel, part));
	}

	EXPECT_TRUE(calculator(chunkTypesMax).isWritingPossible());
	for (unsigned i = 0; i < chunkTypesMax.size(); ++i) {
		std::vector<ChunkType> available = chunkTypesMax;
		available.erase(available.begin() + i); // One part is missing
		EXPECT_TRUE(calculator(available).isWritingPossible());
		available.erase(available.begin()); // Now two parts are missing
		EXPECT_FALSE(calculator(available).isWritingPossible());
		available.push_back(standard);
		EXPECT_TRUE(calculator(available).isWritingPossible());
	}
}

TEST(ChunkCopiesCalculatorTests, GetState) {
	/* Simple scenarios */
	EXPECT_EQ(ChunksAvailabilityState::kSafe,
			calculator({}, 0).getState());
	EXPECT_EQ(ChunksAvailabilityState::kSafe,
			calculator({xor_1_of_2, xor_2_of_2, xor_p_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kSafe,
			calculator({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({xor_1_of_2, xor_p_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({xor_1_of_2, xor_2_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({xor_1_of_3, xor_2_of_3, xor_3_of_3}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({xor_1_of_3, xor_2_of_3, xor_p_of_3}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kLost,
			calculator({}, goal::xorLevelToGoal(2)).getState());
	EXPECT_EQ(ChunksAvailabilityState::kLost,
			calculator({}, 1).getState());
	EXPECT_EQ(ChunksAvailabilityState::kLost,
			calculator({xor_1_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kLost,
			calculator({xor_1_of_3, xor_2_of_3}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kLost,
			calculator({xor_1_of_3, xor_p_of_3}).getState());

	/* More complicated */
	EXPECT_EQ(ChunksAvailabilityState::kSafe,
			calculator({standard, standard, xor_1_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kSafe,
			calculator({xor_1_of_3, xor_1_of_2, xor_2_of_2, xor_p_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_1_of_3}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_1_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_1_of_2, xor_2_of_2}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kEndangered,
			calculator({standard, xor_1_of_2, xor_1_of_3, xor_p_of_3}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kLost,
			calculator({xor_1_of_2, xor_2_of_3}).getState());
	EXPECT_EQ(ChunksAvailabilityState::kLost,
			calculator({xor_p_of_2, xor_1_of_3}).getState());
}
