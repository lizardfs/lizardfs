#include "master/chunk_copies_calculator.h"

#include <algorithm>
#include <bitset>

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
			ret.insert(ret.end(), goal_ - availableCopies, ChunkType::getStandardChunkType());
		}
	}
	return std::move(ret);
}

std::vector<ChunkType> ChunkCopiesCalculator::getPartsToRemove() {
	std::vector<ChunkType> ret;

	if (isXorGoal(goal_)) {
		ChunkType::XorLevel level = goalToXorLevel(goal_);
		uint16_t partsBitmask = 0;

		for (const ChunkType& part : availableParts_) {
			// Remove standard copies
			if (part.isStandardChunkType()) {
				ret.push_back(ChunkType::getStandardChunkType());
			// Remove other levels' parts
			} else if (part.getXorLevel() != level) {
				ret.push_back(part);
			// Remove excessive parts
			} else if (part.isXorChunkType()) {
				if (part.isXorParity()) {
					if (partsBitmask & (1 << 0)) {
						ret.push_back(ChunkType::getXorParityChunkType(level));
					} else {
						partsBitmask |= (1 << 0);
					}
				} else {
					if (partsBitmask & (1 << part.getXorPart())) {
						ret.push_back(ChunkType::getXorChunkType(level, part.getXorPart()));
					} else {
						partsBitmask |= (1 << part.getXorPart());
					}
				}
			}
		}
	} else {
		uint32_t availableCopies = 0;
		for (const ChunkType& part : availableParts_) {
			// Remove excessive copies
			if (part.isStandardChunkType()) {
				++availableCopies;
			// Remove xor parts
			} else {
				ret.push_back(part);
			}
		}
		if (availableCopies > goal_) {
			ret.insert(ret.begin(), availableCopies - goal_, ChunkType::getStandardChunkType());
		}
	}
	return std::move(ret);
}

bool ChunkCopiesCalculator::isRecoveryPossible() {
	// partsAvailableForLevelBitmask[level][i] <=> part i of level is available (i == 0 -> parity)
	std::bitset<kMaxXorLevel + 1> partsAvailableForLevelBitmask[kMaxXorLevel + 1];

	for (auto part: availableParts_) {
		if (part.isStandardChunkType()) {
			return true;
		}
		ChunkType::XorLevel level = part.getXorLevel();
		uint32_t position = (part.isXorParity() ? 0 : part.getXorPart());
		partsAvailableForLevelBitmask[level][position] = true;
		if (partsAvailableForLevelBitmask[level].count() == level) {
			return true;
		}
	}
	return false;
}

bool ChunkCopiesCalculator::isWritingPossible() {
	// Writing is currently possible if there is a standard chunk or
	// all non-parity parts of some level available
	std::bitset<kMaxXorLevel + 1> partsAvailableForLevel[kMaxXorLevel + 1];
	for (auto part: availableParts_) {
		if (part.isStandardChunkType()) {
			return true;
		} else	if (part.isXorParity()) {
			continue;
		}
		uint32_t level = part.getXorLevel();
		partsAvailableForLevel[level][part.getXorPart()] = true;
		if (partsAvailableForLevel[level].count() == level) {
			return true;
		}
	}
	return false;
}
