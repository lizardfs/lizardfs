#pragma once

#include "common/platform.h"

#include <cstdint>
#include <map>
#include <memory>
#include <vector>
#include <set>

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

	struct PostProcessOperation {
		/*
		 * Offset of a block, which will be:
		 * - memcpy'ed from sourceOffset (if destinationOffset != sourceOffset), and then:
		 * - xored in-place with the blocks from ::blocksToXorOffsets
		 */
		uint32_t destinationOffset;

		/*
		 * Offset of block which will be copied to ::destinationOffset before xor operations
		 */
		uint32_t sourceOffset;

		/*
		 * Vector of offsets to each blocks which will be xored to block in ::destinationOffset
		 */
		std::vector<uint32_t> blocksToXorOffsets;
	};

	/**
	 * Base Plan class.
	 * Plans handle different variants of a planned read operation.
	 * For example if we have the following parts: xor 1/2, xor 2/2, xor p/2 and we read
	 * all three of them, we can stop reading after receiving any two of them, but have to
	 * post-process the buffer in different ways, depending on which of the read operations
	 * were finished.
	 */
	class Plan {
	public:
		Plan() : requiredBufferSize(0) {}

		virtual ~Plan() {}

		/**
		 * True, iff no more reading is needed.
		 * This method should be used if \p additionalReadOperations were started to check
		 * if we can stop reading and proceed to post-processing. If \p basicReadOperations are
		 * all done, reading is always finished and this function shouldn't be used.
		 * \param unfinished    set of chunk types from which reading isn't yet finished.
		 */
		virtual bool isReadingFinished(const std::set<ChunkType>& unfinished) const = 0;

		/**
		 * Get list of post-process operations.
		 * This method should be used if all the \p basicReadOperations did finish.
		 */
		virtual std::vector<PostProcessOperation> getPostProcessOperationsForBasicPlan() const = 0;

		/**
		 * Get list of post-process operations.
		 * If \p additionalReadOperations were started, this method should be used to get the right
		 * list of post-processing operations.
		 * \param unfinished    set of chunk types from which reading wasn't finished
		 */
		virtual std::vector<PostProcessOperation> getPostProcessOperationsForExtendedPlan(
				const std::set<ChunkType>& unfinished) const = 0;

		/**
		 * All read operations.
		 * \return sum of \p basicReadOperations and \p additionalReadOperations
		 */
		std::vector<std::pair<ChunkType, ReadOperation>> getAllReadOperations() const {
			std::vector<std::pair<ChunkType, ReadOperation>> ret(
					basicReadOperations.begin(), basicReadOperations.end());
			std::copy(additionalReadOperations.begin(), additionalReadOperations.end(),
					std::back_inserter(ret));
			return ret;
		}

		/**
		 * Buffer size in bytes.
		 */
		uint32_t requiredBufferSize;

		/**
		 * List of read operations.
		 * These operations should be performed to fulfill the read request
		 */
		std::map<ChunkType, ReadOperation> basicReadOperations;

		/**
		 * Additional list of read operations.
		 * These redundant operations can be started additionally to \p basicReadOperations
		 * in order to make the read more reliable.
		 */
		std::map<ChunkType, ReadOperation> additionalReadOperations;
	};

	virtual ~ReadPlanner() {}
	virtual void prepare(const std::vector<ChunkType>& availableParts) = 0;
	virtual std::vector<ChunkType> partsToUse() const = 0;
	virtual bool isReadingPossible() const = 0;
	virtual std::unique_ptr<Plan> buildPlanFor(uint32_t firstBlock, uint32_t blockCount) const = 0;
};
