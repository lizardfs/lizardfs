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

static void checkRecoveryPossible(std::vector<ChunkType> availableParts, bool expectedAnswer) {
	SCOPED_TRACE("Available parts: " + ::testing::PrintToString(availableParts));
	ChunkCopiesCalculator calculator(1);
	for (auto part : availableParts) {
		calculator.addPart(part);
	}
	EXPECT_EQ(expectedAnswer, calculator.isRecoveryPossible());
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
	checkRecoveryPossible({}, false);
	checkRecoveryPossible({standard}, true);
	checkRecoveryPossible({standard, standard}, true);
	checkRecoveryPossible({standard, standard, standard}, true);

	checkRecoveryPossible({xor_1_of_2, xor_2_of_2, xor_p_of_2}, true);
	checkRecoveryPossible({xor_1_of_2, xor_2_of_2}, true);
	checkRecoveryPossible({xor_1_of_2, xor_p_of_2}, true);
	checkRecoveryPossible({xor_2_of_2, xor_p_of_2}, true);
	checkRecoveryPossible({xor_1_of_2}, false);
	checkRecoveryPossible({xor_2_of_2}, false);
	checkRecoveryPossible({xor_p_of_2}, false);
	checkRecoveryPossible({xor_1_of_2, xor_1_of_2}, false);
	checkRecoveryPossible({xor_2_of_2, xor_2_of_2}, false);
	checkRecoveryPossible({xor_p_of_2, xor_p_of_2}, false);
	checkRecoveryPossible({xor_1_of_2, standard}, true);
	checkRecoveryPossible({xor_2_of_2, standard}, true);
	checkRecoveryPossible({xor_p_of_2, standard}, true);

	checkRecoveryPossible({xor_1_of_3, xor_2_of_3, xor_p_of_3}, true);
	checkRecoveryPossible({xor_1_of_3, xor_2_of_3}, false);
	checkRecoveryPossible({xor_1_of_3, xor_p_of_3}, false);
	checkRecoveryPossible({xor_1_of_3}, false);
	checkRecoveryPossible({xor_2_of_3}, false);
	checkRecoveryPossible({xor_3_of_3}, false);
	checkRecoveryPossible({xor_p_of_3}, false);
	checkRecoveryPossible({xor_1_of_3, standard}, true);
	checkRecoveryPossible({xor_2_of_3, standard}, true);
	checkRecoveryPossible({xor_3_of_3, standard}, true);
	checkRecoveryPossible({xor_p_of_3, standard}, true);

	checkRecoveryPossible({xor_1_of_2, xor_2_of_3}, false);
	checkRecoveryPossible({xor_1_of_2, xor_p_of_3}, false);
	checkRecoveryPossible({xor_2_of_2, xor_p_of_3}, false);

	std::vector<ChunkType> chunkTypesMax = {ChunkType::getXorParityChunkType(kMaxXorLevel)};
	for (ChunkType::XorPart part = 1; part <= kMaxXorLevel; ++part) {
		chunkTypesMax.push_back(ChunkType::getXorChunkType(kMaxXorLevel, part));
	}

	checkRecoveryPossible(chunkTypesMax, true);
	for (unsigned i = 0; i < chunkTypesMax.size(); ++i) {
		std::vector<ChunkType> available = chunkTypesMax;
		available.erase(available.begin() + i);
		checkRecoveryPossible(available, true);
		available.erase(available.begin());
		checkRecoveryPossible(available, false);
		available.push_back(standard);
		checkRecoveryPossible(available, true);
	}
}
