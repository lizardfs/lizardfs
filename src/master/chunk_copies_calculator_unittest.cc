#include "master/chunk_copies_calculator.h"

#include <algorithm>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/operators.h"

static void checkPartsToRecover(
		std::vector<ChunkType> available,
		uint8_t goal,
		std::vector<ChunkType> expectedPartsToRecover) {
	SCOPED_TRACE("Testing goal " + (isXorGoal(goal)
			? "xor" + std::to_string(goalToXorLevel(goal))
			: std::to_string(goal)));
	SCOPED_TRACE("Available parts: " + ::testing::PrintToString(available));
	ChunkCopiesCalculator calculator(goal);
	for (auto part : available) {
		calculator.addPart(part);
	}
	std::vector<ChunkType> actualPartsToRecover = calculator.getPartsToRecover();
	std::sort(expectedPartsToRecover.begin(), expectedPartsToRecover.end());
	std::sort(actualPartsToRecover.begin(), actualPartsToRecover.end());
	EXPECT_EQ(expectedPartsToRecover, actualPartsToRecover);
}

static ChunkCopiesCalculator calculator(const std::vector<ChunkType>& parts, uint8_t goal = 1) {
	ChunkCopiesCalculator calculator(goal);
	for (auto part : parts) {
		calculator.addPart(part);
	}
	return calculator;
}

static void checkPartsToRemove(
		std::vector<ChunkType> available,
		uint8_t goal,
		std::vector<ChunkType> expectedPartsToRemove) {
	SCOPED_TRACE("Testing goal " + (isXorGoal(goal)
			? "xor" + std::to_string(goalToXorLevel(goal))
			: std::to_string(goal)));
	SCOPED_TRACE("Available parts: " + ::testing::PrintToString(available));
	ChunkCopiesCalculator calculator(goal);
	for (auto part : available) {
		calculator.addPart(part);
	}
	std::vector<ChunkType> actualPartsToRemove = calculator.getPartsToRemove();
	std::sort(expectedPartsToRemove.begin(), expectedPartsToRemove.end());
	std::sort(actualPartsToRemove.begin(), actualPartsToRemove.end());
	EXPECT_EQ(expectedPartsToRemove, actualPartsToRemove);
}

TEST(ChunkCopiesCalculatorTests, GetPartsToRecover) {
	checkPartsToRecover({standard, standard, standard, standard, standard}, 3, {});
	checkPartsToRecover({standard, standard, standard, standard}, 3, {});
	checkPartsToRecover({standard, standard, standard}, 3, {});
	checkPartsToRecover({standard, standard}, 3, {standard});
	checkPartsToRecover({standard}, 3, {standard, standard});
	checkPartsToRecover({}, 3, {standard, standard, standard});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2, xor_p_of_2}, xorLevelToGoal(2), {});
	checkPartsToRecover({xor_2_of_2, xor_p_of_2}, xorLevelToGoal(2), {xor_1_of_2});
	checkPartsToRecover({xor_1_of_2, xor_p_of_2}, xorLevelToGoal(2), {xor_2_of_2});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({}, xorLevelToGoal(2), {xor_1_of_2, xor_2_of_2, xor_p_of_2});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 1, {standard});
	checkPartsToRecover({xor_1_of_2, xor_p_of_2}, 1, {standard});
	checkPartsToRecover({xor_2_of_2, xor_p_of_2}, 1, {standard});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 1, {standard});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 2, {standard, standard});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2}, 3, {standard, standard, standard});

	checkPartsToRecover({standard}, 1, {});
	checkPartsToRecover({standard}, xorLevelToGoal(2), {xor_1_of_2, xor_2_of_2, xor_p_of_2});
	checkPartsToRecover({standard}, xorLevelToGoal(3), {xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3});

	checkPartsToRecover({xor_1_of_2, xor_2_of_2, standard},   xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2, xor_p_of_3}, xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_2, xor_2_of_2, xor_2_of_2}, xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_2, xor_1_of_2, xor_2_of_2}, xorLevelToGoal(2), {xor_p_of_2});
	checkPartsToRecover({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3}, xorLevelToGoal(2),
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});

	uint8_t goalXorMax = xorLevelToGoal(kMaxXorLevel);
	std::vector<ChunkType> chunkTypesMax = {ChunkType::getXorParityChunkType(kMaxXorLevel)};
	for (ChunkType::XorPart part = 1; part <= kMaxXorLevel; ++part) {
		chunkTypesMax.push_back(ChunkType::getXorChunkType(kMaxXorLevel, part));
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

	std::vector<ChunkType> chunkTypesMax = {ChunkType::getXorParityChunkType(kMaxXorLevel)};
	for (ChunkType::XorPart part = 1; part <= kMaxXorLevel; ++part) {
		chunkTypesMax.push_back(ChunkType::getXorChunkType(kMaxXorLevel, part));
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
	checkPartsToRemove({standard}, 1, {});
	checkPartsToRemove({standard, standard}, 1, {standard});
	checkPartsToRemove({standard, standard, standard}, 1, {standard, standard});
	checkPartsToRemove({standard}, 4, {});
	checkPartsToRemove({standard, standard}, 4, {});
	checkPartsToRemove({standard, standard, standard}, 4, {});
	checkPartsToRemove({standard, standard, standard, standard}, 4, {});
	checkPartsToRemove({standard, standard, standard, standard, standard}, 4, {standard});
	checkPartsToRemove({standard}, xorLevelToGoal(2), {standard});

	checkPartsToRemove({xor_1_of_2}, 1, {xor_1_of_2});
	checkPartsToRemove({xor_1_of_2, xor_1_of_3}, xorLevelToGoal(2), {xor_1_of_3});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_1_of_3}, xorLevelToGoal(2), {xor_1_of_3});
	checkPartsToRemove({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3,
			xor_1_of_2, xor_2_of_2, xor_p_of_2},
			xorLevelToGoal(2),
			{xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3});
	checkPartsToRemove({xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3,
			xor_1_of_2, xor_2_of_2, xor_p_of_2},
			xorLevelToGoal(3),
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_p_of_2, xor_1_of_3},
			xorLevelToGoal(3),
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});

	checkPartsToRemove({xor_1_of_2, standard}, 1, {xor_1_of_2});
	checkPartsToRemove({xor_1_of_2, standard}, xorLevelToGoal(2), {standard});
	checkPartsToRemove({xor_1_of_2, standard}, xorLevelToGoal(3), {xor_1_of_2, standard});
	checkPartsToRemove({xor_1_of_2, standard, standard}, 1, {xor_1_of_2, standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, standard}, 1, {xor_1_of_2, xor_2_of_2});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, standard}, xorLevelToGoal(2), {standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_p_of_2, standard},
			1,
			{xor_1_of_2, xor_2_of_2, xor_p_of_2});
	checkPartsToRemove({xor_1_of_2, xor_2_of_2, xor_p_of_2, standard},
			xorLevelToGoal(2),
			{standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			1,
			{xor_1_of_2, xor_2_of_3, xor_p_of_7});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			xorLevelToGoal(2),
			{xor_2_of_3, xor_p_of_7, standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			xorLevelToGoal(3),
			{xor_1_of_2, xor_p_of_7, standard});
	checkPartsToRemove({xor_1_of_2, xor_2_of_3, xor_p_of_7, standard},
			xorLevelToGoal(7),
			{xor_1_of_2, xor_2_of_3, standard});
}

TEST(ChunkCopiesCalculatorTests, IsWritingPossible) {
	EXPECT_FALSE(calculator({}).isWritingPossible());
	EXPECT_TRUE(calculator({standard}).isWritingPossible());
	EXPECT_TRUE(calculator({standard, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({standard, standard, standard}).isWritingPossible());

	EXPECT_TRUE(calculator({xor_1_of_2, xor_2_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, xor_2_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_2_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_2_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_p_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_1_of_2, xor_1_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_2_of_2, xor_2_of_2}).isWritingPossible());
	EXPECT_FALSE(calculator({xor_p_of_2, xor_p_of_2}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_1_of_2, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_2_of_2, standard}).isWritingPossible());
	EXPECT_TRUE(calculator({xor_p_of_2, standard}).isWritingPossible());

	EXPECT_FALSE(calculator({xor_1_of_3, xor_2_of_3, xor_p_of_3}).isWritingPossible());
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

	std::vector<ChunkType> chunkTypesMax = {ChunkType::getXorParityChunkType(kMaxXorLevel)};
	for (ChunkType::XorPart part = 1; part <= kMaxXorLevel; ++part) {
		chunkTypesMax.push_back(ChunkType::getXorChunkType(kMaxXorLevel, part));
	}

	EXPECT_TRUE(calculator(chunkTypesMax).isWritingPossible());
	for (unsigned i = 0; i < chunkTypesMax.size(); ++i) {
		std::vector<ChunkType> available = chunkTypesMax;
		available.erase(available.begin() + i);
		if (i == 0) {
			// Parity is missing
			EXPECT_TRUE(calculator(available).isWritingPossible());
		} else {
			// Non-parity is missing
			EXPECT_FALSE(calculator(available).isWritingPossible());
		}
		available.erase(available.begin()); // Now two parts are missing
		EXPECT_FALSE(calculator(available).isWritingPossible());
		available.push_back(standard);
		EXPECT_TRUE(calculator(available).isWritingPossible());
	}
}
