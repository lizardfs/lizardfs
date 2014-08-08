#include "common/platform.h"
#include "mount/multi_variant_read_planner.h"

#include <algorithm>
#include <iterator>
#include <limits>

#include "common/massert.h"

namespace {

/**
 * ReadPlanner::Plan for reading from all available xor parts.
 * In such a plan reading is completed when at most one xor part is missing.
 */
class ReadFromAllXorPartsPlan : public ReadPlanner::Plan {
public:
	/// A constructor from other plan.
	/// Discards additional read operations (if any).
	ReadFromAllXorPartsPlan(std::unique_ptr<Plan> plan,
			ChunkType::XorLevel xorLevel, uint32_t firstBlock, uint32_t blockCount)
			: xorLevel_(xorLevel),
			  firstBlock_(firstBlock),
			  blockCount_(blockCount) {
		requiredBufferSize = std::move(plan->requiredBufferSize);
		basicReadOperations = std::move(plan->basicReadOperations);
	}

	bool isReadingFinished(const std::set<ChunkType>& unfinished) const override {
		// Reading is considered finished if at most one part is unfinished
		return unfinished.size() <= 1;
	}

	std::vector<ReadPlanner::PostProcessOperation> getPostProcessOperationsForBasicPlan(
			) const override {
		auto currentLayout = getLayoutAfterReadOperations(
				ReadOperations(basicReadOperations.begin(), basicReadOperations.end()));
		return guessPostProcessOperations(std::move(currentLayout));
	}

	std::vector<ReadPlanner::PostProcessOperation> getPostProcessOperationsForExtendedPlan(
			const std::set<ChunkType>& unfinished) const override {
		sassert(isReadingFinished(unfinished));
		ReadOperations finishedOperations;
		for (auto& partAndOperation : getAllReadOperations()) {
			if (unfinished.count(partAndOperation.first) == 0) {
				finishedOperations.push_back(std::move(partAndOperation));
			}
		}
		auto currentLayout = getLayoutAfterReadOperations(finishedOperations);
		return guessPostProcessOperations(std::move(currentLayout));
	}

private:
	/// A class which represents a block of data read from some part
	struct Block {
		bool valid;          /// false iff the object represents a block where reading didn't finish
		ChunkType chunkType; /// part from which the block was read
		uint32_t stripe;     /// position of the block in the part (eg. stripe=0 => first in part)

		/// Constructs an uninitialized block
		Block() : valid(false), chunkType(ChunkType::getStandardChunkType()), stripe(0) {}

		Block(ChunkType chunkType, uint32_t stripe)
			: valid(true), chunkType(chunkType), stripe(stripe) {
		}

		/// True iff both blocks are valid and are the same
		bool operator==(const Block& other) const {
			if (!valid || !other.valid) {
				return false;
			} else {
				return (chunkType == other.chunkType && stripe == other.stripe);
			}
		}

		bool operator!=(const Block& other) const {
			return !(*this == other);
		}
	};

	/// Description of how blocks are positioned in some buffer.
	typedef std::vector<Block> Layout;

	/// List of read operations for a plan.
	typedef std::vector<std::pair<ChunkType, ReadPlanner::ReadOperation>> ReadOperations;

	/// Returns layout of the buffer after completing read operations from this plan.
	/// \param unfinished    set of parts from which we didn't finish reading
	Layout getLayoutAfterReadOperations(const ReadOperations& operations) const {
		Layout layout(requiredBufferSize / MFSBLOCKSIZE);
		for (const auto& partAndOperation : operations) {
			ChunkType part = partAndOperation.first;
			const auto& operation = partAndOperation.second;
			for (uint32_t i = 0; i < operation.readDataOffsets.size(); ++i) {
				layout[operation.readDataOffsets[i] / MFSBLOCKSIZE] =
						Block(part, operation.requestOffset / MFSBLOCKSIZE + i);
			}
		}
		return layout;
	}

	/// Calculates post-processing operations.
	/// \param actualLayout    layout of the buffer after finishing read operations
	std::vector<ReadPlanner::PostProcessOperation> guessPostProcessOperations(
			Layout actualLayout) const {
		// Generate the layout that is expected to be achieved after completing the plan,
		// ie. just blocks from non-parity parts
		Layout expectedLayout;
		for (uint32_t position = firstBlock_; position < firstBlock_ + blockCount_; ++position) {
			ChunkType::XorPart xorPart = 1 + position % xorLevel_;
			uint32_t stripe = position / xorLevel_;
			expectedLayout.emplace_back(ChunkType::getXorChunkType(xorLevel_, xorPart), stripe);
		}

		// Now we will calculate all the operations needed to transform
		// 'actualLayout' into 'expectedLayout' and store then in the ret variable.
		std::vector<ReadPlanner::PostProcessOperation> ret;

		// In the first pass try to fix all invalid blocks, because fixing them may require using
		// blocks that will be overwritten by the second pass. Eg. if we have the following layout:
		// | 0x1 | 1 | 0 |, we want to generate the following one:  | 0 | 1 |,
		// and block '1' is invalid, we can't start with overwriting '0x1' with '0'.
		for (uint32_t n = 0; n < expectedLayout.size(); ++n) {
			if (!actualLayout[n].valid) {
				ret.push_back(guessOperationForBlock(expectedLayout[n], n, actualLayout));
				actualLayout[n] = expectedLayout[n];
			}
		}

		// In the second pass, fix the rest
		for (uint32_t n = 0; n < expectedLayout.size(); ++n) {
			if (actualLayout[n] != expectedLayout[n]) {
				ret.push_back(guessOperationForBlock(expectedLayout[n], n, actualLayout));
				actualLayout[n] = expectedLayout[n];
			}
		}

		return ret;
	}

	// The function calculates an operation needed to
	// recover a block 'block' at the position 'destinationPosition',
	// when the current layout is as in 'currentLayout'.
	ReadPlanner::PostProcessOperation guessOperationForBlock(
			const Block& block, uint32_t destinationPosition,
			const Layout& currentLayout) const {
		std::set<ChunkType> chunkTypesToXor;
		std::set<uint32_t> positionsToXor;
		// Let's collect positions of all blocks from the same stripe as the block that we need
		for (uint32_t position = 0; position < currentLayout.size(); ++position) {
			if (currentLayout[position] == block) {
				// There is an exact copy available! Just request to memcpy it.
				ReadPlanner::PostProcessOperation op;
				op.destinationOffset = destinationPosition * MFSBLOCKSIZE;
				op.sourceOffset = position * MFSBLOCKSIZE;
				return op;
			} else if (currentLayout[position].valid
					&& currentLayout[position].stripe == block.stripe
					&& chunkTypesToXor.count(currentLayout[position].chunkType) == 0) {
				// The same stripe as our block, so we will have to xor this one with others
				positionsToXor.insert(position);
				chunkTypesToXor.insert(currentLayout[position].chunkType);
			}
		}

		// Now generate a xor operation
		ReadPlanner::PostProcessOperation op;
		op.destinationOffset = destinationPosition * MFSBLOCKSIZE;
		if (positionsToXor.count(destinationPosition) > 0) {
			// no memcpy needed, one of blocks to xor is in the proper place
			op.sourceOffset = destinationPosition * MFSBLOCKSIZE;
		} else {
			// we have to move some block to the destination position to overwrite any garbage
			op.sourceOffset = *positionsToXor.begin() * MFSBLOCKSIZE;
		}
		positionsToXor.erase(op.sourceOffset / MFSBLOCKSIZE); // don't xor with itself
		for (uint32_t positionToXor : positionsToXor) {
			op.blocksToXorOffsets.push_back(positionToXor * MFSBLOCKSIZE);
		}
		return op;
	}

	/// Xor level for which this plan is constructed.
	ChunkType::XorLevel xorLevel_;

	/// Range of blocks for which this plan is constructed.
	uint32_t firstBlock_, blockCount_;
};

// A helper function for the planner.
// Returns a part which should be avoided in the basic version of the plan,
// ie. the one with the lowest score. 'optimalParts' is a set of parts, which
// should not be chosen as worst part in case of equal scores.
ChunkType getWorstPart(
		const std::map<ChunkType, float>& scores,
		const std::set<ChunkType>& optimalParts) {
	float worstScore = std::numeric_limits<float>::max();
	ChunkType worstPart = ChunkType::getXorParityChunkType(9);
	for (const auto& scoreAndPart : scores) {
		float score = scoreAndPart.second;
		ChunkType part = scoreAndPart.first;
		if (score < worstScore || (score == worstScore && optimalParts.count(worstPart) == 1)) {
			worstScore = score;
			worstPart = part;
		}
	}
	return worstPart;
}

// A helper function for the planner.
// Subtracts two read operations.
// Ie. removes from op1 the part that is covered by op2.
// As the result, op1 may have requestSize==0.
void subtractReadOperation(ReadPlanner::ReadOperation& op1, const ReadPlanner::ReadOperation& op2) {
	uint32_t op1End = op1.requestOffset + op1.requestSize;
	uint32_t op2End = op2.requestOffset + op2.requestSize;

	// variant 1, op2. is a superset of op1:
	// op1         |xxxxxxx|
	// op2      |--------------|
	if (op2.requestOffset <= op1.requestOffset && op2End >= op1End) {
		op1.requestSize = 0;
		return;
	}

	// variant 2: op1 needs it's begin to be truncated
	// op1                 |xxxx-----|
	// op2      |--------------|
	if (op2.requestOffset <= op1.requestOffset && op2End > op1.requestOffset) {
		op1.requestSize -= (op2End - op1.requestOffset);
		op1.requestOffset = op2End;
	}

	// variant 2: op1 needs it's end to be truncated
	// op1  |---xxxx|
	// op2      |--------------|
	if (op2.requestOffset < op1End && op2End >= op1End) {
		op1.requestSize -= (op1End - op2.requestOffset);
	}
}

} // anonymous namespace

void MultiVariantReadPlanner::prepare(const std::vector<ChunkType>& availableParts) {
	// If no score for some part is provided, set it to 1.0
	for (const auto& part : availableParts) {
		scores_.insert({part, 1.0});
	}

	// get a list of parts which would be used if no scores were present
	standardPlanner_.prepare(availableParts);
	auto optimalParts = standardPlanner_.partsToUse();

	// choose a part with the worst score trying to avoid choosing one from 'optimalParts'
	ChunkType worstPart = getWorstPart(scores_, {optimalParts.begin(), optimalParts.end()});

	// filter out 'worstPart' from availableParts to get list of parts for the basic plan
	std::vector<ChunkType> bestParts;
	std::copy_if(availableParts.begin(), availableParts.end(), std::back_inserter(bestParts),
			[=](ChunkType type) { return type != worstPart; });
	standardPlanner_.prepare(bestParts);
	if (!standardPlanner_.isReadingPossible()) {
		// If best parts aren't enough to read the data, try to use all available parts
		standardPlanner_.prepare(availableParts);
	}
	partsToUse_.clear();
	if (!standardPlanner_.isReadingPossible()) {
		// Still not enough parts -- nothing can be done, reading isn't possible
		return;
	}
	// Verify that the planner generated a plan which uses a single xor level or a standard part
	uint32_t stripeSize = standardPlanner_.partsToUse().front().getStripeSize();
	for (const auto& part : standardPlanner_.partsToUse()) {
		sassert(part.getStripeSize() == stripeSize);
	}
	// Fill partsToUse_ with all the available chunk types for the xor level being used
	for (const auto& part : availableParts) {
		if (part.getStripeSize() == stripeSize) {
			partsToUse_.insert(part);
		}
	}
}

std::vector<ChunkType> MultiVariantReadPlanner::partsToUse() const {
	return std::vector<ChunkType>(partsToUse_.begin(), partsToUse_.end());
}

bool MultiVariantReadPlanner::isReadingPossible() const {
	return standardPlanner_.isReadingPossible();
}

std::unique_ptr<ReadPlanner::Plan> MultiVariantReadPlanner::buildPlanFor(
		uint32_t firstBlock, uint32_t blockCount) const {
	// Let's start with building a plan using the standard planner
	auto standardPlan = standardPlanner_.buildPlanFor(firstBlock, blockCount);

	// In case of a standard chunk, we will use just the basic version of the plan.
	// We will use it also if there is no redundant part available.
	uint32_t stripeSize = partsToUse_.begin()->getStripeSize();
	if (stripeSize == 1 || partsToUse_.size() == stripeSize) {
		return standardPlan;
	}

	// We are reading xor from all parts, so let's prepare a new plan.
	std::unique_ptr<ReadPlanner::Plan> plan(new ReadFromAllXorPartsPlan(
			std::move(standardPlan), stripeSize, firstBlock, blockCount));

	// For each available part read all the blocks that are needed to recover any block
	// in the range (firstBlock, firstBlock + blockCount)
	sassert(blockCount >= 1);
	uint32_t firstStripe = firstBlock / stripeSize;
	uint32_t stripes = (firstBlock + blockCount - 1) / stripeSize - firstStripe + 1;
	for (const auto& part : partsToUse_) {
		ReadOperation op;
		uint32_t blocksToReadFromPart = stripes;
		if (firstStripe + blocksToReadFromPart > part.getNumberOfBlocks(MFSBLOCKSINCHUNK)) {
			// some parts don't contain blocks from the last stripe, so don't read them
			blocksToReadFromPart = part.getNumberOfBlocks(MFSBLOCKSINCHUNK) - firstStripe;
		}
		op.requestOffset = firstStripe * MFSBLOCKSIZE;
		op.requestSize = blocksToReadFromPart * MFSBLOCKSIZE;
		if (plan->basicReadOperations.count(part) > 0) {
			subtractReadOperation(op, plan->basicReadOperations.at(part));
		}
		if (op.requestSize == 0) {
			continue;
		}
		for (uint32_t i = 0; i < op.requestSize / MFSBLOCKSIZE; ++i) {
			op.readDataOffsets.push_back(plan->requiredBufferSize);
			plan->requiredBufferSize += MFSBLOCKSIZE;
		}
		plan->additionalReadOperations[part] = std::move(op);
	}

	return plan;
}

void MultiVariantReadPlanner::setScores(std::map<ChunkType, float> scores) {
	scores_ = std::move(scores);
}

void MultiVariantReadPlanner::startAvoidingPart(ChunkType partToAvoid) {
	// newSetOfParts := partsToUse_ - { partToAvoid }
	std::vector<ChunkType> newSetOfParts;
	std::copy_if(partsToUse_.begin(), partsToUse_.end(), std::back_inserter(newSetOfParts),
			[=](ChunkType part) { return (part != partToAvoid); });

	// Let's check if after removing 'partToAvoid' reading is still possible
	StandardChunkReadPlanner planner;
	planner.prepare(newSetOfParts);
	if (planner.isReadingPossible()) {
		// It is, so let's reconfigure our planner.
		standardPlanner_.prepare(newSetOfParts);
		sassert(standardPlanner_.isReadingPossible());
	}
}
