#pragma once

#include <cstdint>
#include <ostream>
#include <set>

#include "common/read_planner.h"

namespace unittests {

class Block {
public:
	explicit Block() {}
	explicit Block(ChunkType chunkType, uint32_t blocknum);
	void xorWith(const Block& block);
	bool operator==(const Block& block) const;
	friend std::ostream& operator<<(std::ostream& out, const Block& block);

private:
	std::set<uint32_t> xoredBlocks_;
	void toggle(uint32_t blocknum);
};

class PlanTester {
public:
	static std::vector<Block> executePlan(const ReadPlanner::Plan& plan,
			const std::vector<ChunkType>& availableParts, uint32_t blockCount);
	static std::vector<Block> expectedAnswer(ChunkType chunkType,
			uint32_t firstBlock, uint32_t blockCount);
};

} // namespace unittests
