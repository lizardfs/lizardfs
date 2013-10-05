#ifndef LIZARDFS_MFSCOMMON_READ_OPERATION_PLANNER_H_
#define LIZARDFS_MFSCOMMON_READ_OPERATION_PLANNER_H_

#include <cstdint>
#include <map>
#include <vector>

#include "mfscommon/chunk_type.h"

class ReadOperationPlanner {
public:
	struct ReadOperation {
		/*
		 * Offset which should be sent in READ request
		 */
		uint32_t offset;

		/*
		 * Size which should be sent in READ request
		 */
		uint32_t size;

		/*
		 * offsetsOfBlocks[i] is equal to the offset in the buffer used to realize the plan,
		 * where the i'th (starting from 0) block from this request should be placed
		 */
		std::vector<uint32_t> offsetsOfBlocks;
	};

	struct XorBlockOperation {
		/*
		 * Offset of a block, which will be xored in-place with the blocks from offsetsToXor
		 * */
		uint32_t offset;

		std::vector<uint32_t> offsetsToXor;
	};

	struct Plan {
		uint32_t requiredBufferSize;
		std::map<ChunkType, ReadOperation> readOperations;
		std::vector<XorBlockOperation> xorOperations;
	};

	/**
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
	std::vector<ChunkType> choosePartsToUse(const std::vector<ChunkType>& parts);

	/*
	 * Given a set of parts returned by choosePartsToUse generates a plan of a read operation
	 */
	Plan getPlanFor(const std::vector<ChunkType>& chosenParts, uint32_t offset, uint32_t size);
};

#endif // LIZARDFS_MFSCOMMON_READ_OPERATION_PLANNER_H_
