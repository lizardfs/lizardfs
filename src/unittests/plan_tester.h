#pragma once

#include "common/platform.h"

#include <cstdint>
#include <ostream>
#include <set>

#include "common/read_planner.h"

namespace unittests {

class Block {
public:
	Block() : isInitialized_(false) {}
	Block(ChunkType chunkType, uint32_t blocknum);
	bool isInitialized() const { return isInitialized_; }
	void xorWith(const Block& block);
	bool operator==(const Block& block) const;
	friend std::ostream& operator<<(std::ostream& out, const Block& block);

private:
	bool isInitialized_;
	std::set<uint32_t> xoredBlocks_;

	void toggle(uint32_t blocknum);
};

class PlanTester {
public:
	/**
	 * Executes a plan.
	 * If there no parts from \p basicReadOperations are included in \p failingParts, it will
	 * execute only the basic variant of the plan. If any reading fail, it will execute
	 * \p additionalReadOperations as well.
	 * \param plan            plan to be executed
	 * \param availableParts  list of parts that can be used when executing the plan
	 * \param blockCount      number of blocks that should be returned
	 * \param failingParts    list of parts from which reading will not be done
	 */
	static std::vector<Block> executePlan(
			const ReadPlan& plan,
			const std::vector<ChunkType>& availableParts,
			uint32_t blockCount,
			const std::set<ChunkType>& failingParts = std::set<ChunkType>());

	/**
	 * Returns list of blocks from a given part.
	 * This is the expected result of executing a plan which was built using
	 * \code buildPlan(firstBlock, blockCount) \endcode on a planner which plans reading
	 * blocks of \p chunkType.
	 */
	static std::vector<Block> expectedAnswer(
			ChunkType chunkType,
			uint32_t firstBlock,
			uint32_t blockCount);
};

} // namespace unittests
