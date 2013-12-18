#include "unittests/plan_tester.h"

#include <algorithm>
#include <boost/format.hpp>

#include "common/massert.h"
#include "unittests/operators.h"

namespace unittests {

Block::Block(ChunkType chunkType, uint32_t blocknum) {
	if (chunkType.isStandardChunkType()) {
		sassert(blocknum < MFSBLOCKSINCHUNK);
		toggle(blocknum);
	} else {
		uint32_t level = chunkType.getXorLevel();
		if (chunkType.isXorParity()) {
			uint32_t blocksInParityPart = (MFSBLOCKSINCHUNK + level - 1) / level;
			sassert(blocknum < blocksInParityPart);
			for (uint32_t i = 0; i < level; ++i) {
				if (blocknum * level + i < MFSBLOCKSINCHUNK) {
					toggle(blocknum * level + i);
				}
			}
		} else {
			uint32_t blocksInPart = (MFSBLOCKSINCHUNK + level - chunkType.getXorPart()) / level;
			massert(blocknum < blocksInPart, boost::str(boost::format(
					"Requested block %1% from %2%") % blocknum % chunkType).c_str());
			toggle(blocknum * level + chunkType.getXorPart() - 1);
		}
	}
}

void Block::xorWith(const Block& block) {
	for (uint32_t blocknum : block.xoredBlocks_) {
		toggle(blocknum);
	}
}

bool Block::operator==(const Block& block) const {
	return xoredBlocks_ == block.xoredBlocks_;
}

void Block::toggle(uint32_t blocknum) {
	if (xoredBlocks_.count(blocknum) == 0) {
		xoredBlocks_.insert(blocknum);
	} else {
		xoredBlocks_.erase(blocknum);
	}
}

std::ostream& operator<<(std::ostream& out, const Block& block) {
	if (block.xoredBlocks_.empty()) {
		out << "<empty>";
	} else {
		bool putCross = false;
		for (uint32_t blocknum : block.xoredBlocks_) {
			if (putCross) {
				out << "*";
			}
			out << blocknum;
			putCross = true;
		}
	}
	return out;
}

std::vector<Block> PlanTester::executePlan(const ReadPlanner::Plan& plan,
		const std::vector<ChunkType>& availableParts, uint32_t blockCount) {
	sassert(plan.requiredBufferSize % MFSBLOCKSIZE == 0);
	std::vector<Block> blocks(plan.requiredBufferSize / MFSBLOCKSIZE);
	sassert(blocks.size() >= blockCount);
	for (const auto& chunkTypeAndOperation : plan.readOperations) {
		ChunkType chunkType = chunkTypeAndOperation.first;
		sassert(std::count(availableParts.begin(), availableParts.end(), chunkType) > 0);
		const ReadPlanner::ReadOperation& operation = chunkTypeAndOperation.second;
		sassert(operation.readDataOffsets.size() * MFSBLOCKSIZE == operation.requestSize);
		sassert(operation.requestOffset % MFSBLOCKSIZE == 0);
		uint32_t firstBlock = operation.requestOffset / MFSBLOCKSIZE;
		for (uint32_t i = 0; i < operation.requestSize / MFSBLOCKSIZE; ++i) {
			sassert(operation.readDataOffsets[i] % MFSBLOCKSIZE == 0);
			uint32_t blockInBuffer = operation.readDataOffsets[i] / MFSBLOCKSIZE;
			blocks[blockInBuffer] = Block(chunkType, firstBlock + i);
		}
	}
	for (const auto& operation : plan.xorOperations) {
		sassert(operation.destinationOffset % MFSBLOCKSIZE == 0);
		uint32_t destBlock = operation.destinationOffset / MFSBLOCKSIZE;
		sassert(destBlock < blocks.size());
		for (uint32_t srcOffset : operation.blocksToXorOffsets) {
			sassert(srcOffset % MFSBLOCKSIZE == 0);
			uint32_t srcBlock = srcOffset / MFSBLOCKSIZE;
			sassert(srcBlock < blocks.size());
			blocks[destBlock].xorWith(blocks[srcBlock]);
		}
	}
	blocks.resize(blockCount);
	return std::move(blocks);
}

std::vector<Block> PlanTester::expectedAnswer(ChunkType chunkType,
		uint32_t firstBlock, uint32_t blockCount) {
	std::vector<Block> blocks;
	for (uint32_t i = 0; i < blockCount; ++i) {
		blocks.push_back(Block(chunkType, firstBlock + i));
	}
	return blocks;
}

} // namespace unittests
