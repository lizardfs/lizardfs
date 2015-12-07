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
#include "common/standard_chunk_read_planner.h"

#include <algorithm>

#include "common/goal.h"
#include "common/single_variant_read_plan.h"
#include "common/slice_traits.h"
#include "protocol/MFSCommunication.h"

class StandardChunkReadPlanner::StandardPlanBuilder : public StandardChunkReadPlanner::PlanBuilder {
public:
	StandardPlanBuilder(): PlanBuilder(BUILDER_STANDARD) {}
	std::unique_ptr<ReadPlan> buildPlan(uint32_t firstBlock, uint32_t blockCount) const;
};

std::unique_ptr<ReadPlan> StandardChunkReadPlanner::StandardPlanBuilder::buildPlan(
		uint32_t firstBlock, uint32_t blockCount) const {
	ReadPlan::ReadOperation readOp;
	readOp.requestOffset = firstBlock * MFSBLOCKSIZE;
	readOp.requestSize = blockCount * MFSBLOCKSIZE;
	for (uint32_t block = 0; block < blockCount; ++block) {
		readOp.readDataOffsets.push_back(block * MFSBLOCKSIZE);
	}

	std::unique_ptr<ReadPlan> plan(new SingleVariantReadPlan);
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();
	plan->basicReadOperations[chunkType] = readOp;
	plan->requiredBufferSize = readOp.requestSize;
	return plan;
}

class StandardChunkReadPlanner::XorPlanBuilder : public StandardChunkReadPlanner::PlanBuilder {
public:
	XorPlanBuilder(int level, int missingPart)
		: PlanBuilder(BUILDER_XOR),
			level_(level),
			missingPart_(missingPart) {
	}

	void reset(int level, int missingPart) {
		level_ = level;
		missingPart_ = missingPart;
	}

	std::unique_ptr<ReadPlan> buildPlan(uint32_t firstBlock, uint32_t blockCount) const;

	int level() {
		return level_;
	}

	int missingPart() {
		return missingPart_;
	}

private:
	int level_;
	int missingPart_;

	static const uint32_t lastBlockInChunk = MFSBLOCKSINCHUNK - 1;

	// convert size in blocks to bytes
	inline uint32_t toBytes(uint32_t blockCount) const {
		return blockCount * MFSBLOCKSIZE;
	}

	// which part/stripe does this block belong to?
	inline int partOf(uint32_t block) const {
		return block % level_ + 1;
	}
	inline int stripeOf(uint32_t block) const {
		return block / level_;
	}

	// first/last stripe containing any requested block
	inline int firstStripe(uint32_t firstBlock) const {
		return stripeOf(firstBlock);
	}
	inline int lastStripe(uint32_t lastBlock) const {
		return stripeOf(lastBlock);
	}
	inline int stripeCount(uint32_t firstBlock, uint32_t lastBlock) const {
		return lastStripe(lastBlock) - firstStripe(firstBlock) + 1;
	}
	inline int isManyStripes(uint32_t firstBlock, uint32_t lastBlock) const {
		return stripeCount(firstBlock, lastBlock) > 1;
	}

	// what's the last part in this stripe? (last stripe in chunk may be shorter than others)
	inline int lastPartInStripe(int stripe) const {
	       return (stripe < stripeOf(lastBlockInChunk)) ?
				level_ : partOf(lastBlockInChunk);
	}

	// do we need block from the first/last stripe and this part?
	inline bool isFirstStripeRequestedFor(int part,
			uint32_t firstBlock, uint32_t lastBlock) const {
		return (part >= partOf(firstBlock)) &&
			(isManyStripes(firstBlock, lastBlock) || part <= partOf(lastBlock));
	}
	inline bool isLastStripeRequestedFor(int part,
			uint32_t firstBlock, uint32_t lastBlock) const {
		return (part <= partOf(lastBlock)) &&
			(isManyStripes(firstBlock, lastBlock) || part >= partOf(firstBlock));
	}

	// do we need parity recovery in the first/last stripe?
	inline bool firstStripeRecovery(uint32_t firstBlock, uint32_t lastBlock) const {
		return (missingPart_ && isFirstStripeRequestedFor(missingPart_, firstBlock, lastBlock));
	}
	inline bool lastStripeRecovery(uint32_t firstBlock, uint32_t lastBlock) const {
		return (missingPart_ && isLastStripeRequestedFor(missingPart_, firstBlock, lastBlock));
	}

	// how many parts precede/succeede this part
	inline int precedingPartsCount(int part) const {
		return part - 1;
	}
	inline int succeedingPartsCount(int part) const {
		return level_ - part;
	}

	// compute succeeding parts, take into account end-of-chunk
	inline uint32_t actualSucceedingPartsCount(uint32_t block) const {
		return succeedingPartsCount(partOf(block)) -
			succeedingPartsCount(lastPartInStripe(stripeOf(block)));
	}

	// where should this block end up in the buffer?
	// buffer layout: [requested] [lastStripeExtra] [firstStripeExtra]
	inline uint32_t destinationOffset(int stripe, int part,
			uint32_t firstBlock, uint32_t lastBlock) const {
		if (stripe == firstStripe(firstBlock) && part < partOf(firstBlock)) {
			return lastBlock - firstBlock + 1
				+ (lastStripeRecovery(firstBlock, lastBlock) ?
					actualSucceedingPartsCount(lastBlock) : 0)
				+ precedingPartsCount(part);
		}
		return stripe * level_ + precedingPartsCount(part) - firstBlock;
	}

	// which ChunkType should be fetched to satisfy read of this part?
	inline ChunkPartType correspondingChunkType(int part) const {
		return (part == missingPart_) ? slice_traits::xors::ChunkPartType(level_, slice_traits::xors::kXorParityPart) :
		                                slice_traits::xors::ChunkPartType(level_, part);
	}

	// build single ReadOperation for one XOR part
	inline void buildReadOperationForPart(ReadPlan& plan, uint32_t part,
			uint32_t readOffset, uint32_t readSize, uint32_t firstBlock,
			uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		plan.requiredBufferSize += toBytes(readSize);
		ReadPlan::ReadOperation readOp;
		readOp.requestOffset = toBytes(readOffset);
		readOp.requestSize = toBytes(readSize);

		for (uint32_t stripe = readOffset; stripe < readOffset + readSize; stripe++) {
			readOp.readDataOffsets.push_back(toBytes(
					destinationOffset(stripe, part, firstBlock, lastBlock)));
		}
		// add this ReadOperation to the plan
		ChunkPartType partType = correspondingChunkType(part);
		plan.basicReadOperations[partType] = readOp;
	}

	// build ReadOperations for all parts
	inline void buildReadOperations(ReadPlan& plan,
			uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		for (int part = 1; part <= level_; part++) {
			// do we really need first/last block from this part?
			const bool firstBlockNeeded =
				isFirstStripeRequestedFor(part, firstBlock, lastBlock)
				|| firstStripeRecovery(firstBlock, lastBlock);
			const bool lastBlockNeeded =
				isLastStripeRequestedFor(part, firstBlock, lastBlock)
				|| (lastStripeRecovery(firstBlock, lastBlock) &&
					part <= lastPartInStripe(lastStripe(lastBlock)));

			uint32_t readOffset = firstStripe(firstBlock);  // some initial
			int32_t readSize = stripeCount(firstBlock, lastBlock);  // approximation

			if (!firstBlockNeeded) {   // let's
				readOffset++;
				readSize--;        // correct
			}
			if (!lastBlockNeeded) {    // it
				readSize--;
			}                          // now

			// do we need to read from this part at all?
			if (readSize > 0) {
				// converting positive readSize from int32_t to uint32_t
				buildReadOperationForPart(plan, part, readOffset, readSize, firstBlock, blockCount);
			} else {
				ReadPlan::PrefetchOperation op;
				op.requestOffset = toBytes(firstStripe(firstBlock));
				// Above we use firstStripe(firstBlock), when actually it is more likely that the
				// next requested block will be lastBlock+1, therefore firstStripe(lastBlock+1)
				// would be more likely guess. However, prefetching one block more than necessary
				// costs us less than prefetching one block too far when mfs_fuse reorders requests
				// and we will soon be reading firstStripe(firstBlock).
				op.requestSize = MFSBLOCKSIZE;
				plan.prefetchOperations[correspondingChunkType(part)] = op;
			}
		}
	}

	// build single XorBlockOperation to recover one missing block
	inline void buildXorOperationForStripe(SingleVariantReadPlan& plan,
			uint32_t stripe, uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		ReadPlan::PostProcessOperation op;
		op.destinationOffset = op.sourceOffset =
				toBytes(destinationOffset(stripe, missingPart_, firstBlock, lastBlock));
		for (int part = 1; part <= lastPartInStripe(stripe); ++part) {
			if (part != missingPart_) {
				op.blocksToXorOffsets.push_back(toBytes(
					destinationOffset(stripe, part, firstBlock, lastBlock)));
			}
		}
		plan.addPostProcessOperation(std::move(op));
	}

	// build all required XorOperations
	inline void buildXorOperations(SingleVariantReadPlan& plan,
			uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		for (int stripe = firstStripe(firstBlock); stripe <= lastStripe(lastBlock); ++stripe) {
			if ((stripe == firstStripe(firstBlock) &&
					!firstStripeRecovery(firstBlock, lastBlock)) ||
					(stripe == lastStripe(lastBlock) &&
							!lastStripeRecovery(firstBlock, lastBlock))) {
				continue;
			}
			buildXorOperationForStripe(plan, stripe, firstBlock, blockCount);
		}
	}
};

std::unique_ptr<ReadPlan> StandardChunkReadPlanner::XorPlanBuilder::buildPlan(
		uint32_t firstBlock, uint32_t blockCount) const {
	SingleVariantReadPlan plan;
	buildReadOperations(plan, firstBlock, blockCount);
	if (missingPart_ != 0) {
		plan.prefetchOperations.clear();
		buildXorOperations(plan, firstBlock, blockCount);
	}
	return std::unique_ptr<ReadPlan>(new SingleVariantReadPlan(std::move(plan)));
}

void StandardChunkReadPlanner::prepare(const std::vector<ChunkPartType> &availableParts) {
	currentBuilder_ = nullptr;
	planBuilders_[BUILDER_STANDARD].reset(new StandardPlanBuilder);
	planBuilders_[BUILDER_XOR].reset(new XorPlanBuilder(0, 0));

	std::vector<ChunkPartType> partsToUse = availableParts;
	std::sort(partsToUse.begin(), partsToUse.end());
	partsToUse.erase(std::unique(partsToUse.begin(), partsToUse.end()), partsToUse.end());

	std::vector<bool> isParityForLevelAvailable(slice_traits::xors::kMaxXorLevel + 1, false);
	std::vector<int> partsForLevelAvailable(slice_traits::xors::kMaxXorLevel + 1, 0);

	for (const ChunkPartType &chunkType : partsToUse) {
		if (slice_traits::isXor(chunkType)) {
			if (slice_traits::xors::isXorParity(chunkType)) {
				isParityForLevelAvailable[slice_traits::xors::getXorLevel(
				        chunkType)] = true;
			} else {
				++partsForLevelAvailable[slice_traits::xors::getXorLevel(
				        chunkType)];
			}
		} else {
			assert(slice_traits::isStandard(chunkType));
			return setCurrentBuilderToStandard();
		}
	}

	for (int level = slice_traits::xors::kMaxXorLevel;
	     level >= slice_traits::xors::kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level) {
			return setCurrentBuilderToXor(level, 0);
		}
	}

	// 3. If there is a set of xor chunks with one missing and parity available, choose it
	for (int level = slice_traits::xors::kMaxXorLevel;
	     level >= slice_traits::xors::kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level - 1 &&
		    isParityForLevelAvailable[level]) {
			// partsToUse contains our level's parts sorted in ascending order
			// let's find out which one is missing
			int lastPartSeen = 0;
			int missingPart = level;
			for (ChunkPartType type : partsToUse) {
				if (slice_traits::isXor(type) &&
				    !slice_traits::xors::isXorParity(type) &&
				    slice_traits::xors::getXorLevel(type) == level) {
					int part = slice_traits::xors::getXorPart(type);
					if (part > lastPartSeen + 1) {
						missingPart = lastPartSeen + 1;
						break;
					} else {
						lastPartSeen = part;
					}
				}
			}
			return setCurrentBuilderToXor(level, missingPart);
		}
	}
}

std::unique_ptr<ReadPlan> StandardChunkReadPlanner::buildPlanFor(
		uint32_t firstBlock, uint32_t blockCount) const {
	sassert(firstBlock + blockCount <= MFSBLOCKSINCHUNK);
	sassert(currentBuilder_ != nullptr);
	return currentBuilder_->buildPlan(firstBlock, blockCount);
}

bool StandardChunkReadPlanner::isReadingPossible() const {
	return (bool)currentBuilder_;
}

std::vector<ChunkPartType> StandardChunkReadPlanner::partsToUse() const {
	std::vector<ChunkPartType> parts;
	if (currentBuilder_ == nullptr) {
		return parts;
	}

	switch (currentBuilder_->type()) {
		case BUILDER_STANDARD:
			parts.push_back(slice_traits::standard::ChunkPartType());
		break;
		case BUILDER_XOR: {
			auto builder = static_cast<XorPlanBuilder*>(planBuilders_.at(BUILDER_XOR).get());
			if (builder->level() < slice_traits::xors::kMinXorLevel || builder->level() > slice_traits::xors::kMaxXorLevel) {
				break;
			}

			int level = builder->level();
			int missingPart = builder->missingPart();
			if (missingPart != 0) {
				parts.push_back(slice_traits::xors::ChunkPartType(level, slice_traits::xors::kXorParityPart));
			}

			for (int part = 1; part <= level; ++part) {
				if (part != missingPart) {
					parts.push_back(slice_traits::xors::ChunkPartType(level, part));
				}
			}
		}
		break;
		default:
		break;
	}

	return parts;
}

void StandardChunkReadPlanner::setCurrentBuilderToStandard() {
	currentBuilder_ = planBuilders_.at(BUILDER_STANDARD).get();
}

void StandardChunkReadPlanner::setCurrentBuilderToXor(int level, int missingPart) {
	XorPlanBuilder* builder = static_cast<XorPlanBuilder*>(planBuilders_.at(BUILDER_XOR).get());
	builder->reset(level, missingPart);
	currentBuilder_ = builder;
}
