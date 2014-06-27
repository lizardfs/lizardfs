#include "common/platform.h"
#include "master/chunk_copies_calculator.h"

#include <algorithm>
#include <bitset>

#include "common/goal.h"

ChunkCopiesCalculator::ChunkCopiesCalculator(uint8_t goal): goal_(goal) {}

void ChunkCopiesCalculator::addPart(ChunkType chunkType) {
	availableParts_.push_back(chunkType);
}

std::vector<ChunkType> ChunkCopiesCalculator::getPartsToRecover() const {
	std::vector<ChunkType> ret;
	getPartsToRecover(&ret);
	return ret;
}

std::vector<ChunkType> ChunkCopiesCalculator::getPartsToRemove() const {
	std::vector<ChunkType> ret;
	getPartsToRemove(&ret);
	return ret;
}

uint32_t ChunkCopiesCalculator::countPartsToRecover() const {
	return getPartsToRecover(nullptr);
}

uint32_t ChunkCopiesCalculator::countPartsToRemove() const {
	return getPartsToRemove(nullptr);
}

bool ChunkCopiesCalculator::isRecoveryPossible() const {
	return getState() != ChunksAvailabilityState::kLost;
}

bool ChunkCopiesCalculator::isWritingPossible() const {
	// don't allow writing to unrecoverable chunks
	return getState() != ChunksAvailabilityState::kLost;
}

ChunksAvailabilityState::State ChunkCopiesCalculator::getState() const {
	// Chunks in goal=0 are always safe
	if (goal_ == 0) {
		return ChunksAvailabilityState::kSafe;
	}

	// partsAvailableForLevelBitmask[level][i] <=> part i of level is available (i == 0 -> parity)
	std::bitset<kMaxXorLevel + 1> partsAvailableForLevelBitmask[kMaxXorLevel + 1];
	uint32_t standardCopies = 0;

	for (auto part: availableParts_) {
		if (part.isStandardChunkType()) {
			if (++standardCopies >= 2) {
				return ChunksAvailabilityState::kSafe;
			}
		} else {
			ChunkType::XorLevel level = part.getXorLevel();
			uint32_t position = (part.isXorParity() ? 0 : part.getXorPart());
			partsAvailableForLevelBitmask[level][position] = true;
			if (partsAvailableForLevelBitmask[level].count() == uint32_t(level + 1)) {
				return ChunksAvailabilityState::kSafe;
			}
		}
	}
	// If we are here, chunk is not safe... Check if it is not lost.
	if (standardCopies == 1) {
		return ChunksAvailabilityState::kEndangered;
	}
	sassert(standardCopies == 0);
	for (uint32_t level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
		if (partsAvailableForLevelBitmask[level].count() == level) {
			return ChunksAvailabilityState::kEndangered;
		}
	}
	// Lost :(
	return ChunksAvailabilityState::kLost;
}

uint32_t ChunkCopiesCalculator::getPartsToRecover(std::vector<ChunkType>* ret) const {
	if (goal_ == 0) {
		// Nothing to recover if chunks is not needed
		return 0;
	}

	uint32_t count = 0;
	if (isXorGoal(goal_)) {
		std::bitset<kMaxXorLevel + 1> availableParts;
		ChunkType::XorLevel level = goalToXorLevel(goal_);
		for (const auto& part : availableParts_) {
			if (!part.isXorChunkType() || part.getXorLevel() != level) {
				continue;
			}
			if (part.isXorParity()) {
				availableParts[0] = true;
			} else {
				availableParts[part.getXorPart()] = true;
			}
		}
		for (ChunkType::XorPart part = 1; part <= level; ++part) {
			if (!availableParts[part]) {
				count++;
				if (ret) {
					ret->push_back(ChunkType::getXorChunkType(level, part));
				}
			}
		}
		if (!availableParts[0]) {
			count++;
			if (ret) {
				ret->push_back(ChunkType::getXorParityChunkType(level));
			}
		}
		return count;
	} else {
		uint32_t availableCopies = 0;
		for (const auto& part : availableParts_) {
			if (part.isStandardChunkType()) {
				++availableCopies;
			}
		}
		if (availableCopies < goal_) {
			count = goal_ - availableCopies;
			if (ret) {
				ret->insert(ret->end(), goal_ - availableCopies, ChunkType::getStandardChunkType());
			}
		}
	}
	return count;
}

uint32_t ChunkCopiesCalculator::getPartsToRemove(std::vector<ChunkType>* ret) const {
	if (goal_ == 0) {
		// Delete everything!
		if (ret) {
			*ret = availableParts_;
		}
		return availableParts_.size();
	}

	uint32_t count = 0;
	if (isXorGoal(goal_)) {
		ChunkType::XorLevel level = goalToXorLevel(goal_);
		std::bitset<kMaxXorLevel + 1> parts;
		for (const ChunkType& part : availableParts_) {
			if (part.isStandardChunkType()) {
				// Remove standard copies
				count++;
				if (ret) {
					ret->push_back(part);
				}
			} else if (part.getXorLevel() != level) {
				// Remove other levels' parts
				count++;
				if (ret) {
					ret->push_back(part);
				}
			} else {
				// Remove excessive parts
				if (part.isXorParity()) {
					if (parts[0]) {
						count++;
						if (ret) {
							ret->push_back(part);
						}
					}
					parts[0] = true;
				} else {
					if (parts[part.getXorPart()]) {
						count++;
						if (ret) {
							ret->push_back(part);
						}
					}
					parts[part.getXorPart()] = true;
				}
			}
		}
	} else {
		uint32_t availableCopies = 0;
		for (const ChunkType& part : availableParts_) {
			if (part.isStandardChunkType()) {
				++availableCopies;
			} else {
				// Remove xor parts
				count++;
				if (ret) {
					ret->push_back(part);
				}
			}
		}
		if (availableCopies > goal_) {
			// Remove excessive copies
			count += availableCopies - goal_;
			if (ret) {
				ret->insert(ret->end(), availableCopies - goal_, ChunkType::getStandardChunkType());
			}
		}
	}
	return count;
}
