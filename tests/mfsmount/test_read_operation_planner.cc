#include "mfsmount/read_operation_planner.h"

#include <sstream>
#include <gtest/gtest.h>

#include "mfscommon/goal.h"
#include "mfscommon/MFSCommunication.h"
#include "tests/common/operators.h"

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
			  xor_p_of_3(ChunkType::getXorParityChunkType(3)) {
	}

protected:
	const ChunkType standard;
	const ChunkType xor_1_of_2, xor_2_of_2, xor_p_of_2;
	const ChunkType xor_1_of_3, xor_2_of_3, xor_3_of_3, xor_p_of_3;

	void verifyRead(const ReadOperationPlanner::Plan& plan, ChunkType part,
			uint32_t expectedOffset, const std::vector<uint32_t>& expectedOffsetsOfBlocks) {
		std::stringstream ss;
		ss << "Checking readOperation for chunk type " << part;
		SCOPED_TRACE(ss.str());

		ASSERT_EQ(1U, plan.readOperations.count(part));
		const ReadOperationPlanner::ReadOperation& readOperation = plan.readOperations.at(part);
		uint32_t expectedSize = expectedOffsetsOfBlocks.size() * MFSBLOCKSIZE;
		EXPECT_EQ(expectedOffset, readOperation.offset);
		EXPECT_EQ(expectedSize, readOperation.size);
		EXPECT_EQ(expectedOffsetsOfBlocks, readOperation.offsetsOfBlocks);
	}
};

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard1) {
	std::vector<ChunkType> chunks { standard, standard, standard };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard2) {
	std::vector<ChunkType> chunks { standard, xor_1_of_2 };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard3) {
	std::vector<ChunkType> chunks { standard, xor_p_of_3 };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseStandard4) {
	std::vector<ChunkType> chunks { standard, xor_1_of_2, xor_p_of_2 };
	std::vector<ChunkType> expected { standard };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXor1) {
	std::vector<ChunkType> chunks { xor_p_of_2, xor_1_of_2, xor_2_of_2 };
	std::vector<ChunkType> expected { xor_1_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXor2) {
	std::vector<ChunkType> chunks { xor_1_of_2, xor_2_of_2 };
	std::vector<ChunkType> expected { xor_1_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXor3) {
	std::vector<ChunkType> chunks { xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	std::vector<ChunkType> expected { xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXorParity1) {
	std::vector<ChunkType> chunks { xor_p_of_2, xor_2_of_2 };
	std::vector<ChunkType> expected { xor_p_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseXorParity2) {
	std::vector<ChunkType> chunks { xor_p_of_3, xor_1_of_3, xor_1_of_2, xor_p_of_2 };
	std::vector<ChunkType> expected { xor_p_of_2, xor_1_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseHighestXor1) {
	std::vector<ChunkType> chunks { xor_1_of_2, xor_2_of_2, xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	std::vector<ChunkType> expected { xor_1_of_3, xor_2_of_3, xor_3_of_3 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseHighestXor2) {
	std::vector<ChunkType> chunks { xor_1_of_2, xor_2_of_2, xor_1_of_3, xor_2_of_3, xor_p_of_3 };
	std::vector<ChunkType> expected { xor_1_of_2, xor_2_of_2 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, ChoosePartsToUseHighestXorParity1) {
	std::vector<ChunkType> chunks { xor_p_of_3, xor_1_of_3, xor_2_of_3, xor_1_of_2, xor_p_of_2 };
	std::vector<ChunkType> expected { xor_p_of_3, xor_1_of_3, xor_2_of_3 };
	EXPECT_EQ(expected, ReadOperationPlanner().choosePartsToUse(chunks));
}

TEST_F(ReadOperationPlannerTests, GetPlanForStandard) {
	ReadOperationPlanner::Plan plan = ReadOperationPlanner().getPlanFor(
			{ standard },       // read from standard chunk
			MFSBLOCKSIZE,       // offset = beginning of the second block
			2 * MFSBLOCKSIZE);  // size = two blocks
	EXPECT_EQ(2U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType  offset        offsets of blocks
	verifyRead(plan, standard,  MFSBLOCKSIZE, {0, MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXor1) {
	ReadOperationPlanner::Plan plan = ReadOperationPlanner().getPlanFor(
			{ xor_1_of_2, xor_2_of_2 }, // read from xor level 2
			MFSBLOCKSIZE,               // offset = beginning of the second block
			MFSBLOCKSIZE);              // size   = one block
	EXPECT_EQ(1U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(1U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType   offset  offsets of blocks
	verifyRead(plan, xor_2_of_2, 0,      {0});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXor2) {
	ReadOperationPlanner::Plan plan = ReadOperationPlanner().getPlanFor(
			{ xor_1_of_2, xor_2_of_2 }, // read from xor level 2
			MFSBLOCKSIZE,               // offset = beginning of the second block
			3 * MFSBLOCKSIZE);          // size   = three blocks
	EXPECT_EQ(3U * MFSBLOCKSIZE, plan.requiredBufferSize);
	EXPECT_EQ(2U, plan.readOperations.size());
	EXPECT_EQ(0U, plan.xorOperations.size());
	//               chunkType   offset            offsets of blocks
	verifyRead(plan, xor_1_of_2, 1 * MFSBLOCKSIZE, {MFSBLOCKSIZE});
	verifyRead(plan, xor_2_of_2, 0,                {0, 2 * MFSBLOCKSIZE});
}

TEST_F(ReadOperationPlannerTests, GetPlanForXorMaxLevel) {
	ChunkType::XorLevel level = kMaxXorLevel;
	std::vector<ChunkType> parts;
	for (ChunkType::XorPart part = 1; part < level; ++part) {
		parts.push_back(ChunkType::getXorChunkType(level, part));
	}

	ReadOperationPlanner::Plan plan = ReadOperationPlanner().getPlanFor(
			parts,              // read from highest available level
			2 * MFSBLOCKSIZE,   // offset = beginning of the third block
			4 * MFSBLOCKSIZE);  // size   = four blocks
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
