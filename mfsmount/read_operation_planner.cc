#include "read_operation_planner.h"

#include <algorithm>

#include "mfscommon/goal.h"
#include "mfscommon/MFSCommunication.h"


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

class ReadOperationPlanner::StandardPlanBuilder : public ReadOperationPlanner::PlanBuilder {
public:
	StandardPlanBuilder(): ReadOperationPlanner::PlanBuilder(BUILDER_STANDARD) {}
	virtual ReadOperationPlanner::Plan buildPlan(uint32_t firstBlock, uint32_t blockCount) const;
};

ReadOperationPlanner::Plan ReadOperationPlanner::StandardPlanBuilder::buildPlan(
		uint32_t firstBlock, uint32_t blockCount) const {
	ReadOperationPlanner::ReadOperation readOp;
	readOp.requestOffset = firstBlock * MFSBLOCKSIZE;
	readOp.requestSize = blockCount * MFSBLOCKSIZE;
	for (uint32_t block = 0; block < blockCount; ++block) {
		readOp.destinationOffsets.push_back(block * MFSBLOCKSIZE);
	}

	ReadOperationPlanner::Plan plan;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	plan.readOperations[chunkType] = readOp;
	plan.requiredBufferSize = readOp.requestSize;
	return plan;
}

class ReadOperationPlanner::XorPlanBuilder : public ReadOperationPlanner::PlanBuilder {
public:
	XorPlanBuilder(ChunkType::XorLevel level, ChunkType::XorPart missingPart)
		:	ReadOperationPlanner::PlanBuilder(BUILDER_XOR),
			level_(level),
			missingPart_(missingPart) {
	}

	void reset(ChunkType::XorLevel level, ChunkType::XorPart missingPart) {
		level_ = level;
		missingPart_ = missingPart;
	}

	ReadOperationPlanner::Plan buildPlan(uint32_t firstBlock, uint32_t blockCount) const;

	ChunkType::XorLevel level() {
		return level_;
	}

	ChunkType::XorPart missingPart() {
		return missingPart_;
	}

private:
	ChunkType::XorLevel level_;
	ChunkType::XorPart missingPart_;

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

	// where should this block end up in the buffer?
	// buffer layout: [requested] [lastStripeExtra] [firstStripeExtra]
	inline uint32_t destinationOffset(uint32_t stripe, uint32_t part,
			uint32_t firstBlock, uint32_t lastBlock) const {
		if (stripe == firstStripe(firstBlock) && part < partOf(firstBlock)) {
			return lastBlock - firstBlock + 1
				+ (lastStripeRecovery(firstBlock, lastBlock) ?
					succeedingPartsCount(partOf(lastBlock)) : 0)
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
	inline void buildReadOperationForPart(ReadOperationPlanner::Plan& plan, uint32_t part,
			uint32_t readOffset, uint32_t readSize, uint32_t firstBlock,
			uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		plan.requiredBufferSize += toBytes(readSize);
		ReadOperationPlanner::ReadOperation readOp;
		readOp.requestOffset = toBytes(readOffset);
		readOp.requestSize = toBytes(readSize);

		for (uint32_t stripe = readOffset; stripe < readOffset + readSize; stripe++) {
			readOp.destinationOffsets.push_back(toBytes(
					destinationOffset(stripe, part, firstBlock, lastBlock)));
		}
		// add this ReadOperation to the plan
		ChunkType partType = correspondingChunkType(part);
		plan.readOperations[partType] = readOp;
	}

	// build ReadOperations for all parts
	inline void buildReadOperations(ReadOperationPlanner::Plan& plan,
			uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		for (uint32_t part = 1; part <= level_; part++) {
			// do we really need first/last block from this part?
			const bool firstBlockNeeded =
				isFirstStripeRequestedFor(part, firstBlock, lastBlock)
				|| firstStripeRecovery(firstBlock, lastBlock);
			const bool lastBlockNeeded =
				isLastStripeRequestedFor(part, firstBlock, lastBlock)
				|| lastStripeRecovery(firstBlock, lastBlock);

			uint32_t readOffset = firstStripe(firstBlock);  // some initial
			int32_t readSize = stripeCount(firstBlock, lastBlock);  // approximation

			if (!firstBlockNeeded) {   // let's
				readOffset++;
				readSize--;            // correct
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
	inline void buildXorOperationForStripe(ReadOperationPlanner::Plan& plan, uint32_t stripe,
			uint32_t firstBlock, uint32_t blockCount) const {
		uint32_t lastBlock = firstBlock + blockCount - 1;
		ReadOperationPlanner::XorBlockOperation xorOp;
		xorOp.destinationOffset =
				toBytes(destinationOffset(stripe, missingPart_, firstBlock, lastBlock));
		for (uint32_t part = 1; part <= level_; ++part) {
			if (part != missingPart_) {
				xorOp.blocksToXorOffsets.push_back(toBytes(
					destinationOffset(stripe, part, firstBlock, lastBlock)));
			}
		}
		plan.xorOperations.push_back(xorOp);
	}

	// build all required XorOperations
	inline void buildXorOperations(ReadOperationPlanner::Plan& plan,
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

ReadOperationPlanner::Plan ReadOperationPlanner::XorPlanBuilder::buildPlan(
		uint32_t firstBlock, uint32_t blockCount) const {
	ReadOperationPlanner::Plan plan;
	buildReadOperations(plan, firstBlock, blockCount);
	if (missingPart_ != 0) {
		buildXorOperations(plan, firstBlock, blockCount);
	}
	return plan;
}

ReadOperationPlanner::ReadOperationPlanner(const std::vector<ChunkType>& availableParts) {
	planBuilders_[BUILDER_STANDARD].reset(new StandardPlanBuilder);
	planBuilders_[BUILDER_XOR].reset(new XorPlanBuilder(0, 0));

	std::vector<ChunkType> partsToUse = availableParts;
	std::sort(partsToUse.begin(), partsToUse.end());
	partsToUse.erase(std::unique(partsToUse.begin(), partsToUse.end()), partsToUse.end());

	bool isFullReplicaAvailable = false;
	std::vector<bool> isParityForLevelAvailable(kMaxXorLevel + 1, false);
	std::vector<int> partsForLevelAvailable(kMaxXorLevel + 1, 0);
	for (const ChunkType& chunkType : partsToUse) {
		if (chunkType.isStandardChunkType()) {
			isFullReplicaAvailable = true;
		} else {
			sassert(chunkType.isXorChunkType());
			if (chunkType.isXorParity()) {
				isParityForLevelAvailable[chunkType.getXorLevel()] = true;
			} else {
				++partsForLevelAvailable[chunkType.getXorLevel()];
			}
		}
	}

	// 1. If we can read from xor chunks without using a parity choose the highest level
	for (int level = kMaxXorLevel; level >= kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level) {
			auto newEnd = std::remove_if(
					partsToUse.begin(), partsToUse.end(), IsNotXorLevel(level, false));
			partsToUse.erase(newEnd, partsToUse.end());
			setCurrentBuilderToXor(level, 0);
			return;
		}
	}

	// 2. If there is a full replica, choose it
	if (isFullReplicaAvailable) {
		setCurrentBuilderToStandard();
		return;
	}

	// 3. If there is a set of xor chunks with one missing and parity available, choose it
	for (int level = kMaxXorLevel; level >= kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level - 1 && isParityForLevelAvailable[level]) {
			auto newEnd = std::remove_if(
					partsToUse.begin(), partsToUse.end(), IsNotXorLevel(level, true));
			partsToUse.erase(newEnd, partsToUse.end());
			// find missing part, partsToUse_ contains:
			//      0   1 2 ... k-1        k        .. level-1
			//   parity 1 2 ... k-1 <k missing> k+1 ... level
			ChunkType::XorPart missingPart = level;
			for (int i = 1; i < level; i++) {
				if (partsToUse[i].getXorPart() > i) {
					missingPart = i;
					break;
				}
			}
			setCurrentBuilderToXor(level, missingPart);
			return;
		}
	}

	// 4. Chunk is unreadable
	unsetCurrentBuilder();
}

ReadOperationPlanner::Plan ReadOperationPlanner::buildPlanFor(
		uint32_t firstBlock, uint32_t blockCount) const {
	sassert(firstBlock + blockCount <= MFSBLOCKSINCHUNK);
	sassert(currentBuilder_ != nullptr);
	return currentBuilder_->buildPlan(firstBlock, blockCount);
}

bool ReadOperationPlanner::isReadingPossible() const {
	return (bool)currentBuilder_;
}

std::vector<ChunkType> ReadOperationPlanner::partsToUse() const {
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

void ReadOperationPlanner::setCurrentBuilderToStandard() {
	currentBuilder_ = planBuilders_.at(BUILDER_STANDARD).get();
}

void ReadOperationPlanner::setCurrentBuilderToXor(
		ChunkType::XorLevel level, ChunkType::XorPart missingPart) {
	XorPlanBuilder* builder = static_cast<XorPlanBuilder*>(planBuilders_.at(BUILDER_XOR).get());
	builder->reset(level, missingPart);
	currentBuilder_ = builder;
}

void ReadOperationPlanner::unsetCurrentBuilder() {
	currentBuilder_ = nullptr;
}
