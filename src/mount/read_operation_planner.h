#ifndef LIZARDFS_MFSCOMMON_READ_OPERATION_PLANNER_H_
#define LIZARDFS_MFSCOMMON_READ_OPERATION_PLANNER_H_

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "common/chunk_type.h"

class ReadOperationPlanner {
public:
	struct ReadOperation {
		/*
		 * Offset to be sent in READ request
		 */
		uint32_t requestOffset;

		/*
		 * Size to be sent in READ request
		 */
		uint32_t requestSize;

		/*
		 * offsetsOfBlocks[i] is equal to the offset in the buffer used to realize the plan,
		 * where the i'th (starting from 0) block from this request should be placed
		 */
		std::vector<uint32_t> destinationOffsets;
	};

	struct XorBlockOperation {
		/*
		 * Offset of a block, which will be xored in-place with the blocks from offsetsToXor
		 * */
		uint32_t destinationOffset;

		std::vector<uint32_t> blocksToXorOffsets;
	};

	struct Plan {
		uint32_t requiredBufferSize;
		std::map<ChunkType, ReadOperation> readOperations;
		std::vector<XorBlockOperation> xorOperations;
		Plan() : requiredBufferSize(0) {}
	};

	/*
	 * Chooses which parts of a chunk should be used to read this chunk, eg.
	 * - given: chunk, chunk_xor_1_of_2, chunk_xor_2_of_2, chunk_xor_parity_of_2
	 *   chooses: chunk_xor_1_of_2, chunk_xor_2_of_2
	 * - given: chunk_xor_1_of_2, chunk_xor_2_of_2, chunk_xor_parity_of_2
	 *   chooses: chunk_xor_1_of_2, chunk_xor_2_of_2
	 * - given: chunk, chunk, chunk
	 *   chooses: chunk
	 * - given: chunk_xor_1_of_2, chunk_xor_parity_of_2, chunk_xor_parity_of_3
	 *   chooses: chunk_xor_1_of_2, chunk_xor_parity_of_2
	 */
	ReadOperationPlanner(const std::vector<ChunkType>& availableParts,
			const std::map<ChunkType, float> serverScores);

	/*
	 * Return stored chosen parts.
	 */
	std::vector<ChunkType> partsToUse() const;

	/*
	 * Is it possible to read from chunk using these parts?
	 */
	bool isReadingPossible() const;

	/*
	 * Generate plan of a read operation
	 */
	Plan buildPlanFor(uint32_t firstBlock, uint32_t blockCount) const;

	class PlanBuilder {
	public:
		PlanBuilder(uint32_t type) : type_(type) {}
		virtual ~PlanBuilder() {}

		virtual Plan buildPlan(uint32_t firstBlock, uint32_t blockCount) const = 0;

		uint32_t type() const {
			return type_;
		}

	private:
		const uint32_t type_;
	};

private:

	class StandardPlanBuilder;
	class XorPlanBuilder;

	void setCurrentBuilderToStandard();
	void setCurrentBuilderToXor(ChunkType::XorLevel level, ChunkType::XorPart missingPart);
	void unsetCurrentBuilder();

	enum PlanBuilders {
		BUILDER_NONE,
		BUILDER_STANDARD,
		BUILDER_XOR,
	};

	std::map<PlanBuilders, std::unique_ptr<PlanBuilder>> planBuilders_;
	PlanBuilder* currentBuilder_;
};

#endif // LIZARDFS_MFSCOMMON_READ_OPERATION_PLANNER_H_
