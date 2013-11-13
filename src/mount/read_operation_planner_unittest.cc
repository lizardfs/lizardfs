#include "mount/read_operation_planner.h"

#include <algorithm>
#include <sstream>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "common/MFSCommunication.h"
#include "unittests/operators.h"

class ReadOperationPlannerTests : public testing::Test {
public:
	ReadOperationPlannerTests()
			: standard(ChunkType::getStandardChunkType()),
			  xor_1_of_2(ChunkType::getXorChunkType(2, 1)),
			  xor_2_of_2(ChunkType::getXorChunkType(2, 2)),
			  xor_p_of_2(ChunkType::getXorParityChunkType(2)),
			  xor_1_of_3(ChunkType::getXorChunkType(3, 1)),
			  xor_2_of_3(ChunkType::getXorChunkType(3, 2)),
			  xor_3_of_3(ChunkType::getXorChunkType(3, 3)),
			  xor_p_of_3(ChunkType::getXorParityChunkType(3)),
			  xor_1_of_6(ChunkType::getXorChunkType(6, 1)),
			  xor_2_of_6(ChunkType::getXorChunkType(6, 2)),
			  xor_3_of_6(ChunkType::getXorChunkType(6, 3)),
			  xor_4_of_6(ChunkType::getXorChunkType(6, 4)),
			  xor_5_of_6(ChunkType::getXorChunkType(6, 5)),
			  xor_6_of_6(ChunkType::getXorChunkType(6, 6)),
			  xor_p_of_6(ChunkType::getXorParityChunkType(6)) {

		scores[standard] = 1;

		scores[xor_1_of_2] = 1;
		scores[xor_2_of_2] = 1;
		scores[xor_p_of_2] = 1;

		scores[xor_1_of_3] = 1;
		scores[xor_2_of_3] = 1;
		scores[xor_3_of_3] = 1;
		scores[xor_p_of_3] = 1;

		scores[xor_1_of_6] = 1;
		scores[xor_2_of_6] = 1;
		scores[xor_3_of_6] = 1;
		scores[xor_4_of_6] = 1;
		scores[xor_5_of_6] = 1;
		scores[xor_6_of_6] = 1;
		scores[xor_p_of_6] = 1;
	}

protected:
	const ChunkType standard;
	const ChunkType xor_1_of_2, xor_2_of_2, xor_p_of_2;
	const ChunkType xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3;
	const ChunkType xor_1_of_6, xor_2_of_6, xor_3_of_6, xor_4_of_6, xor_5_of_6, xor_6_of_6, xor_p_of_6;

	std::map<ChunkType, float> scores;

	void verifyRead(const ReadOperationPlanner::Plan& plan, ChunkType part,
			uint32_t expectedOffset, const std::vector<uint32_t>& expectedOffsetsOfBlocks) {
		std::stringstream ss;
		ss << "Checking readOperation for chunk type " << part;
		SCOPED_TRACE(ss.str());

		ASSERT_EQ(1U, plan.readOperations.count(part));
		const ReadOperationPlanner::ReadOperation& readOperation = plan.readOperations.at(part);
		uint32_t expectedSize = expectedOffsetsOfBlocks.size() * MFSBLOCKSIZE;
		EXPECT_EQ(expectedOffset, readOperation.requestOffset);
		EXPECT_EQ(expectedSize, readOperation.requestSize);
		EXPECT_EQ(expectedOffsetsOfBlocks, readOperation.destinationOffsets);
	}

	void verifyXor(const ReadOperationPlanner::Plan& plan,
			uint32_t expectedOffset, const std::vector<uint32_t>& expectedOffsetsOfBlocks) {
		SCOPED_TRACE("Checking xor operation at offset " + std::to_string(expectedOffset));
		bool found = false;
		std::vector<uint32_t> sortedExpected = expectedOffsetsOfBlocks;
		std::sort(sortedExpected.begin(), sortedExpected.end());

		for (const ReadOperationPlanner::XorBlockOperation& xorOp : plan.xorOperations) {
			if (xorOp.destinationOffset != expectedOffset) {
				continue;
			}
			found = true;

			std::vector<uint32_t> sortedXorOffsets = xorOp.blocksToXorOffsets;
			std::sort(sortedXorOffsets.begin(), sortedXorOffsets.end());
			EXPECT_EQ(sortedExpected, sortedXorOffsets);
		}
		EXPECT_TRUE(found) << "Xor operation at offset " << expectedOffset << " not found";
	}
};

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard1) {
	std::vector<ChunkType> chunks { standard, standard, standard };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard2) {
	std::vector<ChunkType> chunks { standard, xor_1_of_2 };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard3) {
	std::vector<ChunkType> chunks { standard, xor_p_of_3 };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard4) {
	std::vector<ChunkType> chunks { standard, xor_1_of_2, xor_p_of_2 };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXor1) {
	std::vector<ChunkType> chunks { xor_p_of_2, xor_1_of_2, xor_2_of_2 };
	std::vector<ChunkType> expected { xor_1_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXor2) {
	std::vector<ChunkType> chunks { xor_1_of_2, xor_2_of_2 };
	std::vector<ChunkType> expected { xor_1_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXor3) {
	std::vector<ChunkType> chunks { xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	std::vector<ChunkType> expected { xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXorParity1) {
	std::vector<ChunkType> chunks { xor_p_of_2, xor_2_of_2 };
	std::vector<ChunkType> expected { xor_p_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXorParity2) {
	std::vector<ChunkType> chunks { xor_p_of_3, xor_1_of_3, xor_1_of_2, xor_p_of_2 };
	std::vector<ChunkType> expected { xor_p_of_2, xor_1_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseHighestXor1) {
	std::vector<ChunkType> chunks { xor_1_of_2, xor_2_of_2, xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	std::vector<ChunkType> expected { xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseHighestXor2) {
	std::vector<ChunkType> chunks { xor_1_of_2, xor_2_of_2, xor_1_of_3, xor_2_of_3, xor_p_of_3 };
	std::vector<ChunkType> expected { xor_1_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseHighestXorParity1) {
	std::vector<ChunkType> chunks { xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_1_of_2, xor_p_of_2 };
	std::vector<ChunkType> expected { xor_p_of_3, xor_1_of_3, xor_2_of_3 };
	EXPECT_EQ(expected, ReadOperationPlanner(chunks, scores).partsToUse());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseImpossible1) {
	std::vector<ChunkType> chunks { };
	EXPECT_FALSE(ReadOperationPlanner(chunks, scores).isReadingPossible());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseImpossible2) {
	std::vector<ChunkType> chunks { xor_1_of_2, xor_2_of_3, xor_3_of_3 };
	EXPECT_FALSE(ReadOperationPlanner(chunks, scores).isReadingPossible());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseImpossible3) {
	std::vector<ChunkType> chunks { xor_1_of_6, xor_2_of_6, xor_3_of_6, xor_5_of_6, xor_6_of_6 };
	EXPECT_FALSE(ReadOperationPlanner(chunks, scores).isReadingPossible());
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseImpossible4) {
	std::vector<ChunkType> chunks { xor_p_of_6, xor_2_of_6, xor_3_of_6, xor_5_of_6, xor_6_of_6 };
	EXPECT_FALSE(ReadOperationPlanner(chunks, scores).isReadingPossible());
}

TEST_F(ReadOperationPlannerTests, GetPlanForStandard) {
	ReadOperationPlanner planner({
			standard            // read from standard chunk
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,       // beginning of the second block
			2);      // two blocks
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType  offset        offsets of blocks
	verifyRead(plan, standard,  MFSBLOCKSIZE, {0, MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXor1) {
	ReadOperationPlanner planner({
			xor_1_of_2,                 // read from xor level 2
			xor_2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,               // beginning of the second block
			1);              // one block
	EXPECT_EQ(1U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_2_of_2, 0,      {0});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXor2) {
	ReadOperationPlanner planner({
			xor_1_of_2,                 // read from xor level 2
			xor_2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,               // beginning of the second block
			3);              // three blocks
	EXPECT_EQ(3U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType   offset            offsets of blocks
	verifyRead(plan, xor_1_of_2, 1 * MFSBLOCKSIZE, {MFSBLOCKSIZE});
	verifyRead(plan, xor_2_of_2, 0,                {0, 2 * MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForMaxXorLevel) {
	ChunkType::XorLevel level = kMaxXorLevel;
	std::vector<ChunkType> parts;
	for (ChunkType::XorPart part = 1; part <= level; ++part) {
		ChunkType type = ChunkType::getXorChunkType(level, part);
		parts.push_back(type);
		scores[type] = 1;
	}

	ReadOperationPlanner planner(parts, scores);  // read from highest available level
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			2,     // beginning of the third block
			4);    // four blocks
	ASSERT_GE(kMaxXorLevel, 6);
	EXPECT_EQ(4U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(0U, plan.xorOperations.size());
	EXPECT_EQ(4U, plan.readOperations.size());
	//               chunkType  offset  offsets of blocks
	verifyRead(plan, parts[2],  0,      {0 * MFSBLOCKSIZE});
	verifyRead(plan, parts[3],  0,      {1 * MFSBLOCKSIZE});
	verifyRead(plan, parts[4],  0,      {2 * MFSBLOCKSIZE});
	verifyRead(plan, parts[5],  0,      {3 * MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity1) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_2_of_2                  // read from xor level 2 without 1_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			0,                          // beginning of the chunk
			1);                         // one block
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_p_of_2, 0,      {0});
	verifyRead(plan, xor_2_of_2, 0,      {MFSBLOCKSIZE});
	verifyXor(plan,              0,      {MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity2) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_2_of_2                  // read from xor level 2 without 1_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,               // beginning of the second block
			1);              // one block
	EXPECT_EQ(1U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_2_of_2, 0,      {0});

}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity3) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_2_of_2                  // read from xor level 2 without 1_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			0,                          // offset = beginning of the chunk
			2);                         // two blocks
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_p_of_2, 0,      {0});
	verifyRead(plan, xor_2_of_2, 0,      {MFSBLOCKSIZE});
	verifyXor(plan,              0,      {MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity4) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_2_of_2                  // read from xor level 2 without 1_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,               // beginning of the second block
			2);              // two blocks
	EXPECT_EQ(3U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset        offsets of blocks
	verifyRead(plan, xor_p_of_2, MFSBLOCKSIZE, {MFSBLOCKSIZE});
	verifyRead(plan, xor_2_of_2, 0,            {0, 2 * MFSBLOCKSIZE});
	verifyXor(plan,              MFSBLOCKSIZE, {2 * MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity5) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_1_of_2                  // read from xor level 2 without 2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			0,                          // offset = beginning of the chunk
			1);                         // one block
	EXPECT_EQ(1U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_1_of_2, 0,      {0});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity6) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_1_of_2                  // read from xor level 2 without 2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,               // beginning of the second block
			1);              // one block
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_1_of_2, 0,      {MFSBLOCKSIZE});
	verifyRead(plan, xor_p_of_2, 0,      {0});
	verifyXor(plan,              0,      {MFSBLOCKSIZE});

}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity7) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_1_of_2                  // read from xor level 2 without 2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			0,                          // offset = beginning of the chunk
			2);                         // two blocks
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset        offsets of blocks
	verifyRead(plan, xor_1_of_2, 0,            {0});
	verifyRead(plan, xor_p_of_2, 0,            {MFSBLOCKSIZE});
	verifyXor(plan,              MFSBLOCKSIZE, {0});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity8) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_1_of_2                  // read from xor level 2 without 2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,               // beginning of the second block
			2 );             // two blocks
	EXPECT_EQ(3U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_p_of_2, 0,      {0});
	verifyRead(plan, xor_1_of_2, 0,      {2 * MFSBLOCKSIZE, MFSBLOCKSIZE});
	verifyXor(plan,              0,      {2 * MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity9) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_2_of_2                  // read from xor level 2 without 1_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1023,        // last block
			1);          // one block
	EXPECT_EQ(1U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType   offset              offsets of blocks
	verifyRead(plan, xor_2_of_2, 511 * MFSBLOCKSIZE, {0});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity10) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_1_of_2                  // read from xor level 2 without 2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1023,           // last block
			1);             // one block
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset              offsets of blocks
	verifyRead(plan, xor_p_of_2, 511 * MFSBLOCKSIZE, {0});
	verifyRead(plan, xor_1_of_2, 511 * MFSBLOCKSIZE, {MFSBLOCKSIZE});
	verifyXor(plan,              0,                  {MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity11) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_2_of_2                  // read from xor level 2 without 1_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1022,        // two last blocks
			2);          // two blocks
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset              offsets of blocks
	verifyRead(plan, xor_p_of_2, 511 * MFSBLOCKSIZE, {0});
	verifyRead(plan, xor_2_of_2, 511 * MFSBLOCKSIZE, {MFSBLOCKSIZE});
	verifyXor(plan,              0,                  {MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorParity12) {
	ReadOperationPlanner planner({
			xor_p_of_2,
			xor_1_of_2                  // read from xor level 2 without 2_of_2
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1022,        // two last blocks
			2);          // two blocks
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(1U, plan.xorOperations.size());
	//               chunkType   offset              offsets of blocks
	verifyRead(plan, xor_1_of_2, 511 * MFSBLOCKSIZE, {0});
	verifyRead(plan, xor_p_of_2, 511 * MFSBLOCKSIZE, {MFSBLOCKSIZE});
	verifyXor(plan,              MFSBLOCKSIZE,       {0});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorLevel6Parity1) {
	ReadOperationPlanner planner({
			xor_1_of_6,	// read from xor level 6 without 3_of_6
			xor_6_of_6,
			xor_2_of_6,
			xor_5_of_6,
			xor_p_of_6,
			xor_4_of_6
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			2,   // beginning of the third block
			4);  // four blocks
	EXPECT_EQ(6U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.xorOperations.size());
	EXPECT_EQ(6U, plan.readOperations.size());
	//               chunkType  offset  offsets of blocks
	verifyRead(plan, xor_p_of_6,  0,    {0 * MFSBLOCKSIZE});
	verifyRead(plan, xor_4_of_6,  0,    {1 * MFSBLOCKSIZE});
	verifyRead(plan, xor_5_of_6,  0,    {2 * MFSBLOCKSIZE});
	verifyRead(plan, xor_6_of_6,  0,    {3 * MFSBLOCKSIZE});
	verifyRead(plan, xor_1_of_6,  0,    {4 * MFSBLOCKSIZE});
	verifyRead(plan, xor_2_of_6,  0,    {5 * MFSBLOCKSIZE});

	verifyXor(plan,               0,      {
	                         4 * MFSBLOCKSIZE,
	                         5 * MFSBLOCKSIZE,
	                         1 * MFSBLOCKSIZE,
	                         2 * MFSBLOCKSIZE,
	                         3 * MFSBLOCKSIZE,
	});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorLevel6Parity2) {
	ReadOperationPlanner planner({
			xor_1_of_6,	// read from xor level 6 without 3_of_6
			xor_6_of_6,
			xor_2_of_6,
			xor_5_of_6,
			xor_p_of_6,
			xor_4_of_6
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			3,   // beginning of the fourth block
			4);  // four blocks
	EXPECT_EQ(4U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(0U, plan.xorOperations.size());
	EXPECT_EQ(4U, plan.readOperations.size());
	//               chunkType    offset               offsets of blocks
	verifyRead(plan, xor_4_of_6,    0,                {0 * MFSBLOCKSIZE});
	verifyRead(plan, xor_5_of_6,    0,                {1 * MFSBLOCKSIZE});
	verifyRead(plan, xor_6_of_6,    0,                {2 * MFSBLOCKSIZE});
	verifyRead(plan, xor_1_of_6,    1 * MFSBLOCKSIZE, {3 * MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorLevel6Parity3) {
	ReadOperationPlanner planner({
			xor_1_of_6,  // read from xor level 6 without 3_of_6
			xor_6_of_6,
			xor_2_of_6,
			xor_5_of_6,
			xor_p_of_6,
			xor_4_of_6
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,   // beginning of the second block
			4);  // four blocks
	EXPECT_EQ(6U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.xorOperations.size());
	EXPECT_EQ(6U, plan.readOperations.size());
	//               chunkType  offset    offsets of blocks
	verifyRead(plan, xor_2_of_6,  0,      {0 * MFSBLOCKSIZE});
	verifyRead(plan, xor_p_of_6,  0,      {1 * MFSBLOCKSIZE});
	verifyRead(plan, xor_4_of_6,  0,      {2 * MFSBLOCKSIZE});
	verifyRead(plan, xor_5_of_6,  0,      {3 * MFSBLOCKSIZE});
	verifyRead(plan, xor_6_of_6,  0,      {4 * MFSBLOCKSIZE});
	verifyRead(plan, xor_1_of_6,  0,      {5 * MFSBLOCKSIZE});

	verifyXor(plan,     1 * MFSBLOCKSIZE, {
	                    0 * MFSBLOCKSIZE,
	                    2 * MFSBLOCKSIZE,
	                    3 * MFSBLOCKSIZE,
	                    4 * MFSBLOCKSIZE,
	                    5 * MFSBLOCKSIZE,
	});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorLevel3MultiParity1) {
	ReadOperationPlanner planner({
			xor_1_of_3,  // read from xor level 3 without 2_of_3
			xor_3_of_3,
			xor_p_of_3
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			0,   // beginning of the first block
			6);  // six blocks
	EXPECT_EQ(6U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.xorOperations.size());
	EXPECT_EQ(3U, plan.readOperations.size());
	//               chunkType  offset    offsets of blocks
	verifyRead(plan, xor_1_of_3,  0,      {0 * MFSBLOCKSIZE, 3 * MFSBLOCKSIZE});
	verifyRead(plan, xor_p_of_3,  0,      {1 * MFSBLOCKSIZE, 4 * MFSBLOCKSIZE});
	verifyRead(plan, xor_3_of_3,  0,      {2 * MFSBLOCKSIZE, 5 * MFSBLOCKSIZE});

	verifyXor(plan,     1 * MFSBLOCKSIZE, {  // Unsorted elements
	                    2 * MFSBLOCKSIZE,
	                    0 * MFSBLOCKSIZE,
	});
	verifyXor(plan,     4 * MFSBLOCKSIZE, {  // Sorted elements
	                    3 * MFSBLOCKSIZE,
	                    5 * MFSBLOCKSIZE,
	});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorLevel3MultiParity2) {
	ReadOperationPlanner planner({
			xor_1_of_3,  // read from xor level 3 without 2_of_3
			xor_3_of_3,
			xor_p_of_3
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			1,   // beginning of the second block
			6);  // six blocks
	EXPECT_EQ(7U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.xorOperations.size());
	EXPECT_EQ(3U, plan.readOperations.size());
	//               chunkType  offset    offsets of blocks
	verifyRead(plan, xor_1_of_3,  0,      {6 * MFSBLOCKSIZE, 2 * MFSBLOCKSIZE, 5 * MFSBLOCKSIZE});
	verifyRead(plan, xor_p_of_3,  0,      {0 * MFSBLOCKSIZE, 3 * MFSBLOCKSIZE});
	verifyRead(plan, xor_3_of_3,  0,      {1 * MFSBLOCKSIZE, 4 * MFSBLOCKSIZE});

	verifyXor(plan,     0 * MFSBLOCKSIZE, {  // Unsorted elements
	                    6 * MFSBLOCKSIZE,
	                    1 * MFSBLOCKSIZE,
	});
	verifyXor(plan,     3 * MFSBLOCKSIZE, {  // Sorted elements
	                    2 * MFSBLOCKSIZE,
	                    4 * MFSBLOCKSIZE,
	});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorLevel3MultiParity3) {
	ReadOperationPlanner planner({
			xor_1_of_3,  // read from xor level 3 without 2_of_3
			xor_3_of_3,
			xor_p_of_3
			}, scores);
	ReadOperationPlanner::Plan plan = planner.buildPlanFor(
			2,   // beginning of the third block
			6);  // six blocks
	EXPECT_EQ(7U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.xorOperations.size());
	EXPECT_EQ(3U, plan.readOperations.size());
	//               chunkType    offset            offsets of blocks
	verifyRead(plan, xor_1_of_3,  1 * MFSBLOCKSIZE, {1 * MFSBLOCKSIZE, 4 * MFSBLOCKSIZE});
	verifyRead(plan, xor_p_of_3,  1 * MFSBLOCKSIZE, {2 * MFSBLOCKSIZE, 5 * MFSBLOCKSIZE});
	verifyRead(plan, xor_3_of_3,  0,                {0 * MFSBLOCKSIZE, 3 * MFSBLOCKSIZE, 6 * MFSBLOCKSIZE});

	// XOR verification starting from last parity block
	verifyXor(plan,     5 * MFSBLOCKSIZE, {  // Sorted elements
	                    4 * MFSBLOCKSIZE,
	                    6 * MFSBLOCKSIZE,
	});
	verifyXor(plan,     2 * MFSBLOCKSIZE, {  // Unsorted elements
	                    3 * MFSBLOCKSIZE,
	                    1 * MFSBLOCKSIZE,
	});
}
