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
#include "common/xor_chunk_read_planner.h"

#include <algorithm>
#include <iostream>

#include "common/goal.h"
#include "common/single_variant_read_plan.h"
#include "common/slice_traits.h"
#include "common/standard_chunk_read_planner.h"

class XorChunkReadPlanner::PlanBuilder {
public:
	explicit PlanBuilder(ChunkPartType readChunkType) : chunkType_(readChunkType) {
		sassert(slice_traits::isXor(chunkType_));
	}
	virtual ~PlanBuilder() {}
	virtual std::unique_ptr<ReadPlan> build(uint32_t firstBlock, uint32_t blockCount) const = 0;

protected:
	const ChunkPartType chunkType_;
};

class XorChunkReadPlanner::CopyPartPlanBuilder : public XorChunkReadPlanner::PlanBuilder {
public:
	explicit CopyPartPlanBuilder(ChunkPartType readChunkType) : PlanBuilder(readChunkType) {}
	virtual std::unique_ptr<ReadPlan> build(uint32_t firstBlock, uint32_t blockCount) const;
};

class XorChunkReadPlanner::XorPlanBuilder : public XorChunkReadPlanner::PlanBuilder {
public:
	explicit XorPlanBuilder(ChunkPartType readChunkType, const std::vector<ChunkPartType>& availableParts)
			: PlanBuilder(readChunkType),
			  availableParts_(availableParts) {
		standardPlanner_.prepare(availableParts);
		sassert(standardPlanner_.isReadingPossible());
	}
	virtual std::unique_ptr<ReadPlan> build(uint32_t firstBlock, uint32_t blockCount) const;

private:
	const std::vector<ChunkPartType> availableParts_;
	StandardChunkReadPlanner standardPlanner_;

	// Modifies the plan using the given block permutation (offsetMapping)
	void swapBlocks(SingleVariantReadPlan& plan,
			const std::map<uint32_t, uint32_t>& offsetMapping) const;
};

std::unique_ptr<ReadPlan> XorChunkReadPlanner::CopyPartPlanBuilder::build(
		uint32_t firstBlock, uint32_t blockCount) const {
	SingleVariantReadPlan plan;
	plan.requiredBufferSize = blockCount * MFSBLOCKSIZE;
	ReadPlan::ReadOperation& readOperation = plan.basicReadOperations[chunkType_];
	readOperation.requestOffset = firstBlock * MFSBLOCKSIZE;
	readOperation.requestSize = blockCount * MFSBLOCKSIZE;
	for (uint32_t i = 0; i < blockCount; ++i) {
		readOperation.readDataOffsets.push_back(i * MFSBLOCKSIZE);
	}
	return std::unique_ptr<ReadPlan>(new SingleVariantReadPlan(std::move(plan)));
}

void XorChunkReadPlanner::XorPlanBuilder::swapBlocks(SingleVariantReadPlan& plan,
		const std::map<uint32_t, uint32_t>& offsetMapping) const {
	for (auto& chunkTypeAndReadOperation : plan.basicReadOperations) {
		ReadPlan::ReadOperation& operation = chunkTypeAndReadOperation.second;
		for (size_t i = 0; i < operation.readDataOffsets.size(); ++i) {
			operation.readDataOffsets[i] = offsetMapping.at(operation.readDataOffsets[i]);
		}
	}
	for (auto& operation : plan.postProcessOpearations()) {
		operation.destinationOffset = offsetMapping.at(operation.destinationOffset);
		operation.sourceOffset = offsetMapping.at(operation.sourceOffset);
		for (uint32_t& sourceOffset : operation.blocksToXorOffsets) {
			sourceOffset = offsetMapping.at(sourceOffset);
		}
	}
}

std::unique_ptr<ReadPlan> XorChunkReadPlanner::XorPlanBuilder::build(
		uint32_t firstBlock, uint32_t blockCount) const {
	uint32_t level = slice_traits::xors::getXorLevel(chunkType_);

	// We will start with a plan for standard chunk
	uint32_t firstBlockInChunk;
	uint32_t blockCountInChunk;
	if (slice_traits::xors::isXorParity(chunkType_)) {
		firstBlockInChunk = firstBlock * level;
		blockCountInChunk = blockCount * level;
		if (firstBlockInChunk + blockCountInChunk > MFSBLOCKSINCHUNK) {
			blockCountInChunk = MFSBLOCKSINCHUNK - firstBlockInChunk;
		}
	} else {
		int part = slice_traits::xors::getXorPart(chunkType_);
		firstBlockInChunk = firstBlock * level + part - 1;
		blockCountInChunk = 1 + (blockCount - 1) * level;
	}
	SingleVariantReadPlan plan(standardPlanner_.buildPlanFor(firstBlockInChunk, blockCountInChunk));

	// Calculate the parity if we recover a parity part by adding some xor operations
	// to the list of post-process operations from the original plan
	if (slice_traits::xors::isXorParity(chunkType_)) {
		for (uint32_t i = 0; i < blockCount; ++i) {
			ReadPlan::PostProcessOperation operation;
			operation.destinationOffset = operation.sourceOffset = i * level * MFSBLOCKSIZE;
			for (uint32_t j = 1; j < level; ++j) {
				if (i * level + j < blockCountInChunk) {
					operation.blocksToXorOffsets.push_back((i * level + j) * MFSBLOCKSIZE);
				}
			}
			if (!operation.blocksToXorOffsets.empty()) {
				plan.addPostProcessOperation(std::move(operation));
			}
		}
	}

	// Swap blocks in plan to locate the result at the beginning
	std::map<uint32_t, uint32_t> mapping, reverseMapping;
	for (uint32_t i = 0; i < plan.requiredBufferSize / MFSBLOCKSIZE; ++i) {
		mapping[i * MFSBLOCKSIZE] = i * MFSBLOCKSIZE;
	}
	for (uint32_t i = 0; i < blockCount; ++i) {
		std::swap(mapping[i * MFSBLOCKSIZE], mapping[i * level * MFSBLOCKSIZE]);
	}
	for (const auto& entry : mapping) {
		reverseMapping[entry.second] = entry.first;
	}
	swapBlocks(plan, reverseMapping);

	// The plan is ready!
	return std::unique_ptr<ReadPlan>(new SingleVariantReadPlan(std::move(plan)));
}

XorChunkReadPlanner::XorChunkReadPlanner(ChunkPartType readChunkType) : chunkType_(readChunkType) {
	sassert(slice_traits::isXor(chunkType_));
}

XorChunkReadPlanner::~XorChunkReadPlanner() {}

void XorChunkReadPlanner::prepare(const std::vector<ChunkPartType>& availableParts) {
	if (std::count(availableParts.begin(), availableParts.end(), chunkType_) > 0) {
		planBuilder_.reset(new CopyPartPlanBuilder(chunkType_));
		partsToUse_ = {chunkType_};
		return;
	} else if (std::count(availableParts.begin(), availableParts.end(),
			slice_traits::standard::ChunkPartType()) > 0) {
		partsToUse_ = {slice_traits::standard::ChunkPartType()};
		planBuilder_.reset(new XorPlanBuilder(chunkType_, partsToUse_));
		return;
	}

	partsToUse_.clear();
	std::vector<ChunkPartType> availablePartsUniq = availableParts;
	std::sort(availablePartsUniq.begin(), availablePartsUniq.end());
	availablePartsUniq.erase(
			std::unique(availablePartsUniq.begin(), availablePartsUniq.end()),
			availablePartsUniq.end());
	std::vector<int> partsForLevelAvailable(slice_traits::xors::kMaxXorLevel + 1, 0);
	for (const auto& part : availablePartsUniq) {
		if (slice_traits::isXor(part)) {
			partsForLevelAvailable[slice_traits::xors::getXorLevel(part)]++;
		}
	}
	int levelToRead = 0;
	for (int level = slice_traits::xors::kMinXorLevel; level <= slice_traits::xors::kMaxXorLevel; ++level) {
		if (partsForLevelAvailable[level] == level) {
			levelToRead = level;
			break;
		}
	}
	for (int level = slice_traits::xors::kMinXorLevel; level <= slice_traits::xors::kMaxXorLevel; ++level) {
		if (partsForLevelAvailable[level] == (level + 1)) {
			levelToRead = level;
			break;
		}
	}
	if (levelToRead == 0) {
		return;
	}

	bool needParity = partsForLevelAvailable[levelToRead] == levelToRead;
	for (const auto& part : availablePartsUniq) {
		if (slice_traits::isXor(part) && slice_traits::xors::getXorLevel(part) == levelToRead) {
			if (slice_traits::xors::isXorParity(part) && !needParity) {
				continue;
			}
			partsToUse_.push_back(part);
		}
	}
	planBuilder_.reset(new XorPlanBuilder(chunkType_, partsToUse_));
}

std::vector<ChunkPartType> XorChunkReadPlanner::partsToUse() const {
	return partsToUse_;
}

bool XorChunkReadPlanner::isReadingPossible() const {
	return static_cast<bool>(planBuilder_);
}

std::unique_ptr<ReadPlan> XorChunkReadPlanner::buildPlanFor(
		uint32_t firstBlock, uint32_t blockCount) const {
	sassert(firstBlock + blockCount <= MFSBLOCKSINCHUNK);
	sassert(planBuilder_ != nullptr);
	return planBuilder_->build(firstBlock, blockCount);
}
