#include "master/chunk_copies_calculator.h"

#include "common/goal.h"

ChunkCopiesCalculator::ChunkCopiesCalculator(uint8_t goal): goal_(goal) {}

void ChunkCopiesCalculator::addPart(ChunkType chunkType) {
	availableParts_.push_back(chunkType);
}

std::vector<ChunkType> ChunkCopiesCalculator::getPartsToRecover() {
	std::vector<ChunkType> ret;
	if (isXorGoal(goal_)) {
		uint16_t availablePartsBitmask = 0;
		ChunkType::XorLevel level = goalToXorLevel(goal_);
		for (const auto& part : availableParts_) {
			if (!part.isXorChunkType() || part.getXorLevel() != level) {
				continue;
			}
			if (part.isXorParity()) {
				availablePartsBitmask |= (1 << 0);
			} else {
				availablePartsBitmask |= (1 << part.getXorPart());
			}
		}
		for (ChunkType::XorPart part = 1; part <= level; ++part) {
			if ((availablePartsBitmask & (1 << part)) == 0) {
				ret.push_back(ChunkType::getXorChunkType(level, part));
			}
		}
		if ((availablePartsBitmask & (1 << 0)) == 0) {
			ret.push_back(ChunkType::getXorParityChunkType(level));
		}
	} else {
		uint32_t availableCopies = 0;
		for (const auto& part : availableParts_) {
			if (part.isStandardChunkType()) {
				++availableCopies;
			}
		}
		if (availableCopies < goal_) {
			ret.insert(ret.begin(), goal_ - availableCopies, ChunkType::getStandardChunkType());
		}
	}
	return std::move(ret);
}

bool ChunkCopiesCalculator::isRecoveryPossible() {
	// partsAvailableForLevelBitmask[level][i] <=> part i of level is available (i == 0 -> parity)
	uint16_t partsAvailableForLevelBitmask[kMaxXorLevel + 1] = {0};

	// partsAvailableForLevelCounter[level] == number of different parts available for level
	uint16_t partsAvailableForLevelCounter[kMaxXorLevel + 1] = {0};

	for (auto part: availableParts_) {
		if (part.isStandardChunkType()) {
			return true;
		}
		sassert(part.isXorChunkType());
		ChunkType::XorLevel level = part.getXorLevel();
		uint32_t bitmaskForPart = (1 << 0); // bit 0 -> parity
		if (!part.isXorParity()) {
			bitmaskForPart = (1 << part.getXorPart());
		}
		if ((partsAvailableForLevelBitmask[level] & bitmaskForPart) == 0) {
			// we haven't seen this part
			partsAvailableForLevelBitmask[level] |= bitmaskForPart;
			if (++partsAvailableForLevelCounter[level] == level) {
				return true;
			}
		}
	}
	return false;
}
