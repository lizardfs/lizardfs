/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "unittests/plan_tester.h"

#include <algorithm>
#include <boost/format.hpp>

#include "common/massert.h"
#include "unittests/operators.h"

namespace unittests {

Block::Block(ChunkType chunkType, uint32_t blocknum) : isInitialized_(true) {
	if (chunkType.isStandardChunkType()) {
		sassert(blocknum < MFSBLOCKSINCHUNK);
		toggle(blocknum);
	} else {
		uint32_t level = chunkType.getXorLevel();
		if (chunkType.isXorParity()) {
			uint32_t blocksInParityPart = chunkType.getNumberOfBlocks(MFSBLOCKSINCHUNK);
			sassert(blocknum < blocksInParityPart);
			for (uint32_t i = 0; i < level; ++i) {
				if (blocknum * level + i < MFSBLOCKSINCHUNK) {
					toggle(blocknum * level + i);
				}
			}
		} else {
			uint32_t blocksInPart = chunkType.getNumberOfBlocks(MFSBLOCKSINCHUNK);
			massert(blocknum < blocksInPart, boost::str(boost::format(
					"Requested block %1% from %2%") % blocknum % chunkType).c_str());
			toggle(blocknum * level + chunkType.getXorPart() - 1);
		}
	}
}

void Block::xorWith(const Block& block) {
	if (!block.isInitialized()) {
		isInitialized_ = false;
	}
	for (uint32_t blocknum : block.xoredBlocks_) {
		toggle(blocknum);
	}
}

bool Block::operator==(const Block& block) const {
	return (xoredBlocks_ == block.xoredBlocks_) && block.isInitialized() && isInitialized();
}

void Block::toggle(uint32_t blocknum) {
	if (!isInitialized_) {
		return;
	}
	if (xoredBlocks_.count(blocknum) == 0) {
		xoredBlocks_.insert(blocknum);
	} else {
		xoredBlocks_.erase(blocknum);
	}
}

std::ostream& operator<<(std::ostream& out, const Block& block) {
	if (!block.isInitialized()) {
		out << "<garbage>";
	} else if (block.xoredBlocks_.empty()) {
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

std::vector<Block> PlanTester::executePlan(
		const ReadPlan& plan,
		const std::vector<ChunkType>& availableParts,
		uint32_t blockCount,
		const std::set<ChunkType>& failingParts) {
	sassert(plan.requiredBufferSize % MFSBLOCKSIZE == 0);
	std::vector<Block> blocks(plan.requiredBufferSize / MFSBLOCKSIZE);
	sassert(blocks.size() >= blockCount);

	// This helper function applies the 'operation' on 'chunkType' to the 'blocks' vector
	auto doReadOperation = [&](ChunkType chunkType, const ReadPlan::ReadOperation& operation) {
		sassert(std::count(availableParts.begin(), availableParts.end(), chunkType) > 0);
		sassert(operation.readDataOffsets.size() * MFSBLOCKSIZE == operation.requestSize);
		sassert(operation.requestOffset % MFSBLOCKSIZE == 0);
		uint32_t firstBlock = operation.requestOffset / MFSBLOCKSIZE;
		for (uint32_t i = 0; i < operation.requestSize / MFSBLOCKSIZE; ++i) {
			sassert(operation.readDataOffsets[i] % MFSBLOCKSIZE == 0);
			uint32_t blockInBuffer = operation.readDataOffsets[i] / MFSBLOCKSIZE;
			blocks[blockInBuffer] = Block(chunkType, firstBlock + i);
		}
	};

	// Perform read operations
	bool additionalReadOperationsExecuted = false;
	std::set<ChunkType> unfinishedOperations;
	for (const auto& chunkTypeAndOperation : plan.basicReadOperations) {
		if (failingParts.count(chunkTypeAndOperation.first) == 0) {
			doReadOperation(chunkTypeAndOperation.first, chunkTypeAndOperation.second);
		} else {
			unfinishedOperations.insert(chunkTypeAndOperation.first);
		}
	}
	if (!unfinishedOperations.empty()) {
		// perform additionalReadOperations only when some basic already operation failed
		for (const auto& chunkTypeAndOperation : plan.additionalReadOperations) {
			if (failingParts.count(chunkTypeAndOperation.first) == 0) {
				doReadOperation(chunkTypeAndOperation.first, chunkTypeAndOperation.second);
			} else {
				unfinishedOperations.insert(chunkTypeAndOperation.first);
			}
		}
		additionalReadOperationsExecuted = true;
	}

	// Choose post-processing operations (if we managed to finish reading)
	std::vector<ReadPlan::PostProcessOperation> postProcessing;
	if (!additionalReadOperationsExecuted) {
		postProcessing = plan.getPostProcessOperationsForBasicPlan();
	} else if (plan.isReadingFinished(unfinishedOperations)) {
		postProcessing = plan.getPostProcessOperationsForExtendedPlan(unfinishedOperations);
	}

	// Do the post-processing
	for (const auto& operation : postProcessing) {
		sassert(operation.destinationOffset % MFSBLOCKSIZE == 0);
		uint32_t destBlock = operation.destinationOffset / MFSBLOCKSIZE;
		sassert(destBlock < blocks.size());
		blocks[destBlock] = blocks[operation.sourceOffset / MFSBLOCKSIZE]; // simulate memcpy
		for (uint32_t srcOffset : operation.blocksToXorOffsets) {
			sassert(srcOffset % MFSBLOCKSIZE == 0);
			uint32_t srcBlock = srcOffset / MFSBLOCKSIZE;
			sassert(srcBlock < blocks.size());
			blocks[destBlock].xorWith(blocks[srcBlock]); // simulate blockXor
		}
	}

	// Remove blocks that are not part of the answer and return them
	blocks.resize(blockCount);
	return blocks;
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
