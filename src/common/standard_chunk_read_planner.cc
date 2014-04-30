#include "config.h"
#include "common/standard_chunk_read_planner.h"

#include <algorithm>

#include "common/goal.h"
#include "common/MFSCommunication.h"

namespace {

class IsNotXorLevel {
public:
	IsNotXorLevel(ChunkType::XorLevel level, bool acceptParity = true)
			: level_(level),
			  acceptParity_(acceptParity) {
	}

	bool operator()(ChunkType chunkType) {
		if (!chunkType.isXorChunkType() || chunkType.getXorLevel() != level_) {
			return true;
		}
		if (!acceptParity_ && chunkType.isXorParity()) {
			return true;
		}
		return false;
	}

private:
	ChunkType::XorLevel level_;
	bool acceptParity_;
};

} // anonymous namespace

class StandardChunkReadPlanner::StandardPlanBuilder : public StandardChunkReadPlanner::PlanBuilder {
public:
	StandardPlanBuilder(): PlanBuilder(BUILDER_STANDARD) {}
	virtual Plan buildPlan(uint32_t firstBlock, uint32_t blockCount) const;
};

ReadPlanner::Plan StandardChunkReadPlanner::StandardPlanBuilder::buildPlan(
		uint32_t firstBlock, uint32_t blockCount) const {
	ReadPlanner::ReadOperation readOp;
	readOp.requestOffset = firstBlock * MFSBLOCKSIZE;
	readOp.requestSize = blockCount * MFSBLOCKSIZE;
	for (uint32_t block = 0; block < blockCount; ++block) {
		readOp.readDataOffsets.push_back(block * MFSBLOCKSIZE);
	}

	Plan plan;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	plan.readOperations[chunkType] = readOp;
	plan.requiredBufferSize = readOp.requestSize;
	return plan;
}

class StandardChunkReadPlanner::XorPlanBuilder : public StandardChunkReadPlanner::PlanBuilder {
public:
	XorPlanBuilder(ChunkType::XorLevel level, ChunkType::XorPart missingPart)
		:       PlanBuilder(BUILDER_XOR),
			level_(level),
			missingPart_(missingPart) {
	}

	void reset(ChunkType::XorLevel level, ChunkType::XorPart missingPart) {
		level_ = level;
		missingPart_ = missingPart;
	}

	Plan buildPlan(uint32_t firstBlock, uint32_t blockCount) const;

	ChunkType::XorLevel level() {
		return level_;
	}

	ChunkType::XorPart missingPart() {
		return missingPart_;
	}

private:
	ChunkType::XorLevel level_;
	ChunkType::XorPart missingPart_;

	static const uint32_t lastBlockInChunk = MFSBLOCKSINCHUNK - 1;

	// convert size in blocks to bytes
	inline uint32_t toBytes(uint32_t blockCount) const {
		return blockCount * MFSBLOCKSIZE;
	}

	// which part/stripe does this block belong to?
	inline uint32_t partOf(uint32_t block) const {
		return block % level_ + 1;
	}
	inline uint32_t stripeOf(uint32_t block) const {
		return block / level_;
	}

	// first/last stripe containing any requested block
	inline uint32_t firstStripe(uint32_t firstBlock) const {
		return stripeOf(firstBlock);
	}
	inline uint32_t lastStripe(uint32_t lastBlock) const {
		return stripeOf(lastBlock);
	}
	inline uint32_t stripeCount(uint32_t firstBlock, uint32_t lastBlock) const {
		return lastStripe(lastBlock) - firstStripe(firstBlock) + 1;
	}
	inline bool isManyStripes(uint32_t firstBlock, uint32_t lastBlock) const {
		return stripeCount(firstBlock, lastBlock) > 1;
	}

	// what's the last part in this stripe? (last stripe in chunk may be shorter than others)
	inline uint32_t lastPartInStripe(uint32_t stripe) const {
	       return (stripe < stripeOf(lastBlockInChunk)) ?
				level_ : partOf(lastBlockInChunk);
	}

	// do we need block from the first/last stripe and this part?
	inline bool isFirstStripeRequestedFor(uint32_t part,
			uint32_t firstBlock, uint32_t lastBlock) const {
		return (part >= partOf(firstBlock)) &&
			(isManyStripes(firstBlock, lastBlock) || part <= partOf(lastBlock));
	}
	inline bool isLastStripeRequestedFor(uint32_t part,
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
	inline uint32_t precedingPartsCount(uint32_t part) const {
		return part - 1;
	}
	inline uint32_t succeedingPartsCount(uint32_t part) const {
		return level_ - part;
	}

	// compute succeeding parts, take into account end-of-chunk
	inline uint32_t actualSucceedingPartsCount(uint32_t block) const {
		return succeedingPartsCount(partOf(block)) -
			succeedingPartsCount(lastPartInStripe(stripeOf(block)));
	}

	// where should this block end up in the buffer?
	// buffer layout: [requested] [lastStripeExtra] [firstStripeExtra]
	inline uint32_t destinationOffset(uint32_t stripe, uint32_t part,
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
	inline ChunkType correspondingChunkType(uint32_t part) const {
		return (part == missingPart_) ?
				ChunkType::getXorParityChunkType(level_) :
				ChunkType::getXorChunkType(level_, part);
	}

	// build single ReadOperation for one XOR part
	inline void buildReadOperationForPart(Plan& plan, uint32_t part,
			uint32_t readOffset, uint32_t readSize, uint32_t firstBlock,
			uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		plan.requiredBufferSize += toBytes(readSize);
		ReadOperation readOp;
		readOp.requestOffset = toBytes(readOffset);
		readOp.requestSize = toBytes(readSize);

		for (uint32_t stripe = readOffset; stripe < readOffset + readSize; stripe++) {
			readOp.readDataOffsets.push_back(toBytes(
					destinationOffset(stripe, part, firstBlock, lastBlock)));
		}
		// add this ReadOperation to the plan
		ChunkType partType = correspondingChunkType(part);
		plan.readOperations[partType] = readOp;
	}

	// build ReadOperations for all parts
	inline void buildReadOperations(Plan& plan,
			uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		for (uint32_t part = 1; part <= level_; part++) {
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
			}
		}
	}

	// build single XorBlockOperation to recover one missing block
	inline void buildXorOperationForStripe(Plan& plan, uint32_t stripe,
			uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		XorBlockOperation xorOp;
		xorOp.destinationOffset =
				toBytes(destinationOffset(stripe, missingPart_, firstBlock, lastBlock));
		for (uint32_t part = 1; part <= lastPartInStripe(stripe); ++part) {
			if (part != missingPart_) {
				xorOp.blocksToXorOffsets.push_back(toBytes(
					destinationOffset(stripe, part, firstBlock, lastBlock)));
			}
		}
		plan.xorOperations.push_back(xorOp);
	}

	// build all required XorOperations
	inline void buildXorOperations(Plan& plan,
			uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		for (uint32_t stripe = firstStripe(firstBlock); stripe <= lastStripe(lastBlock); ++stripe) {
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

ReadPlanner::Plan StandardChunkReadPlanner::XorPlanBuilder::buildPlan(
		uint32_t firstBlock, uint32_t blockCount) const {
	Plan plan;
	buildReadOperations(plan, firstBlock, blockCount);
	if (missingPart_ != 0) {
		buildXorOperations(plan, firstBlock, blockCount);
	}
	return plan;
}

float xorPlanScore(const std::map<ChunkType, float>& serverScores, int level) {
	float score = 1;
	for (ChunkType::XorPart part = 1; part <= level; part++) {
		ChunkType type = ChunkType::getXorChunkType(level, part);
		score *= serverScores.at(type);
	}
	return score;
}

float xorPlanScore(const std::map<ChunkType, float>& serverScores, int level, int missing) {
	float score = 1;
	for (ChunkType::XorPart part = 1; part <= level; part++) {
		if (part == missing) {
			continue;
		}
		ChunkType type = ChunkType::getXorChunkType(level, part);
		score *= serverScores.at(type);
	}
	score *= serverScores.at(ChunkType::getXorParityChunkType(level));
	return score;
}

void StandardChunkReadPlanner::prepare(const std::vector<ChunkType>& availableParts,
		const std::map<ChunkType, float>& serverScores) {
	currentBuilder_ = nullptr;
	planBuilders_[BUILDER_STANDARD].reset(new StandardPlanBuilder);
	planBuilders_[BUILDER_XOR].reset(new XorPlanBuilder(0, 0));

	std::vector<ChunkType> partsToUse = availableParts;
	std::sort(partsToUse.begin(), partsToUse.end());
	partsToUse.erase(std::unique(partsToUse.begin(), partsToUse.end()), partsToUse.end());

	float bestScore = -1;

	std::vector<bool> isParityForLevelAvailable(kMaxXorLevel + 1, false);
	std::vector<int> partsForLevelAvailable(kMaxXorLevel + 1, 0);

	for (const ChunkType& chunkType : partsToUse) {
		if (chunkType.isStandardChunkType()) {
			// standard chunk replica available
			float score = serverScores.at(chunkType);
			if (score > bestScore) {
				bestScore = score;
				setCurrentBuilderToStandard();
			}

		} else {
			sassert(chunkType.isXorChunkType());
			if (chunkType.isXorParity()) {
				isParityForLevelAvailable[chunkType.getXorLevel()] = true;
			} else {
				++partsForLevelAvailable[chunkType.getXorLevel()];
			}
		}
	}

	for (int level = kMaxXorLevel; level >= kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level) {
			if (isParityForLevelAvailable[level]) {
				// use scores to decide whether to use parity or not
				ChunkType::XorPart worstPart = 0;
				float worstScore = serverScores.at(ChunkType::getXorParityChunkType(level));

				for (ChunkType::XorPart part = 1; part <= level; part++) {
					ChunkType type = ChunkType::getXorChunkType(level, part);
					float score = serverScores.at(type);
					if (score < worstScore) {
					       worstScore = score;
					       worstPart = part;
					}
				}

				if (worstPart == 0) {
					float score = xorPlanScore(serverScores, level);
					if (score > bestScore) {
						bestScore = score;
						setCurrentBuilderToXor(level, 0);
					}
				} else {
					float score = xorPlanScore(serverScores, level, worstPart);
					if (score > bestScore) {
						bestScore = score;
						setCurrentBuilderToXor(level, worstPart);
					}
				}
			} else {
				// no parity, but all normal parts available
				float score = xorPlanScore(serverScores, level);
				if (score > bestScore) {
					bestScore = score;
					setCurrentBuilderToXor(level, 0);
				}
			}
		}
	}

	// 3. If there is a set of xor chunks with one missing and parity available, choose it
	for (int level = kMaxXorLevel; level >= kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level - 1 && isParityForLevelAvailable[level]) {
			// partsToUse contains our level's parts sorted in ascending order
			// let's find out which one is missing
			ChunkType::XorPart lastPartSeen = 0;
			ChunkType::XorPart missingPart = level;
			for (ChunkType type : partsToUse) {
				if (type.isXorChunkType() && !type.isXorParity() &&
						type.getXorLevel() == level) {
					ChunkType::XorPart part = type.getXorPart();
					if (part > lastPartSeen + 1) {
						missingPart = lastPartSeen + 1;
						break;
					} else {
						lastPartSeen = part;
					}

				}
			}
			float score = xorPlanScore(serverScores, level, missingPart);
			if (score > bestScore) {
				bestScore = score;
				setCurrentBuilderToXor(level, missingPart);
			}
		}
	}
}

ReadPlanner::Plan StandardChunkReadPlanner::buildPlanFor(
		uint32_t firstBlock, uint32_t blockCount) const {
	sassert(firstBlock + blockCount <= MFSBLOCKSINCHUNK);
	sassert(currentBuilder_ != nullptr);
	return currentBuilder_->buildPlan(firstBlock, blockCount);
}

bool StandardChunkReadPlanner::isReadingPossible() const {
	return (bool)currentBuilder_;
}

std::vector<ChunkType> StandardChunkReadPlanner::partsToUse() const {
	std::vector<ChunkType> parts;
	if (currentBuilder_ == nullptr) {
		return parts;
	}

	switch (currentBuilder_->type()) {
		case BUILDER_STANDARD:
			parts.push_back(ChunkType::getStandardChunkType());
		break;
		case BUILDER_XOR: {
			auto builder = static_cast<XorPlanBuilder*>(planBuilders_.at(BUILDER_XOR).get());
			if (builder->level() < kMinXorLevel || builder->level() > kMaxXorLevel) {
				break;
			}

			ChunkType::XorLevel level = builder->level();
			ChunkType::XorPart missingPart = builder->missingPart();
			if (missingPart != 0) {
				parts.push_back(ChunkType::getXorParityChunkType(level));
			}

			for (ChunkType::XorPart part = 1; part <= level; ++part) {
				if (part != missingPart) {
					parts.push_back(ChunkType::getXorChunkType(level, part));
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

void StandardChunkReadPlanner::setCurrentBuilderToXor(
		ChunkType::XorLevel level, ChunkType::XorPart missingPart) {
	XorPlanBuilder* builder = static_cast<XorPlanBuilder*>(planBuilders_.at(BUILDER_XOR).get());
	builder->reset(level, missingPart);
	currentBuilder_ = builder;
}

void StandardChunkReadPlanner::unsetCurrentBuilder() {
	currentBuilder_ = nullptr;
}
