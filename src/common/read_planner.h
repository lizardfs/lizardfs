#pragma once

#include "config.h"

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "common/chunk_type.h"

class ReadPlanner {
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
		 * Local offsets of source data (related to requestOffset)
		 *    - interpretation dependent on implementation (watch its comments)
		 */
		std::vector<uint32_t> readDataOffsets;
	};

	struct XorBlockOperation {
		/*
		 * Offset of a block, which will be xored in-place with the blocks from ::offsetsToXor
		 * */
		uint32_t destinationOffset;
		/*
		 * Vector of offsets to each blocks which will be xored to block in ::destinationOffset
		 */
		std::vector<uint32_t> blocksToXorOffsets;
	};

	struct Plan {
		/*
		 * Buffer size in bytes
		 */
		uint32_t requiredBufferSize;

		std::map<ChunkType, ReadOperation> readOperations;
		std::vector<XorBlockOperation> xorOperations;

		Plan() : requiredBufferSize(0) {}
	};

	virtual ~ReadPlanner() {}
	virtual void prepare(const std::vector<ChunkType>& availableParts,
			const std::map<ChunkType, float>& serverScores) = 0;
	virtual std::vector<ChunkType> partsToUse() const = 0;
	virtual bool isReadingPossible() const = 0;
	virtual Plan buildPlanFor(uint32_t firstBlock, uint32_t blockCount) const = 0;

};
