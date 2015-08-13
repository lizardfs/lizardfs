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
#include "master/chunk_copies_calculator.h"

#include <algorithm>
#include <bitset>

#include "common/goal.h"

ChunkCopiesCalculator::ChunkCopiesCalculator(const Goal* goal): goal_(goal) {}

void ChunkCopiesCalculator::addPart(ChunkType chunkType, const MediaLabel& label) {
	availableParts_.emplace_back(chunkType, label);
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
	if (goal_->getExpectedCopies() == 0) {
		return ChunksAvailabilityState::kSafe;
	}

	// partsAvailableForLevelBitmask[level][i] <=> part i of level is available (i == 0 -> parity)
	std::bitset<goal::kMaxXorLevel + 1> partsAvailableForLevelBitmask[goal::kMaxXorLevel + 1];
	uint32_t standardCopies = 0;

	for (auto partWithLabel: availableParts_) {
		auto part = partWithLabel.first;
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
	for (uint32_t level = goal::kMinXorLevel; level <= goal::kMaxXorLevel; ++level) {
		if (partsAvailableForLevelBitmask[level].count() == level) {
			return ChunksAvailabilityState::kEndangered;
		}
	}
	// Lost :(
	return ChunksAvailabilityState::kLost;
}

std::pair<uint32_t, uint32_t> ChunkCopiesCalculator::ordinaryPartsToRecoverAndRemove() const {
	uint32_t missingCopiesOfLabels = 0;
	sassert(!goal_->isXor());
	const Goal::Labels& labels = goal_->chunkLabels();
	for (const auto& labelAndCount : labels) {
		const auto& label = labelAndCount.first;
		if (label == MediaLabel::kWildcard) {
			continue;
		}
		uint32_t copiesOfLabel = 0;
		for (const auto& partWithLabel : availableParts_) {
			if (partWithLabel.first.isStandardChunkType() && partWithLabel.second == label) {
				copiesOfLabel++;
			}
		}
		uint32_t expectedCopiesOfLabel = labelAndCount.second;
		if (copiesOfLabel < expectedCopiesOfLabel) {
			missingCopiesOfLabels += expectedCopiesOfLabel - copiesOfLabel;
		}
	}
	uint32_t standardCopies = std::count_if(
			availableParts_.begin(), availableParts_.end(),
			[](const ChunkCopiesCalculator::Part& part) {
					return part.first.isStandardChunkType();
			});
	uint32_t expectedCopies = goal_->getExpectedCopies();
	uint32_t missingCopies = std::max<uint32_t>(
				expectedCopies > standardCopies ? expectedCopies - standardCopies : 0,
				missingCopiesOfLabels);

	uint32_t validAndMissing = standardCopies + missingCopies;
	uint32_t redundantCopies = expectedCopies < validAndMissing ? validAndMissing - expectedCopies : 0;

	return std::make_pair(missingCopies, redundantCopies);
}

uint32_t ChunkCopiesCalculator::getPartsToRecover(std::vector<ChunkType>* ret) const {
	if (goal_->getExpectedCopies() == 0) {
		// Nothing to recover if chunks is not needed
		return 0;
	}

	uint32_t count = 0;
	if (goal_->isXor()) {
		std::bitset<goal::kMaxXorLevel + 1> availableParts;
		ChunkType::XorLevel level = goal_->xorLevel();
		for (const auto& partWithLabel : availableParts_) {
			auto part = partWithLabel.first;
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
		auto toRecover = ordinaryPartsToRecoverAndRemove().first;
		if (ret) {
			ret->assign(toRecover, ChunkType::getStandardChunkType());
		}
		return toRecover;
	}
}

uint32_t ChunkCopiesCalculator::getPartsToRemove(std::vector<ChunkType>* ret) const {
	if (goal_->getExpectedCopies() == 0) {
		// Delete everything!
		if (ret) {
			std::transform(availableParts_.begin(), availableParts_.end(),
					std::back_inserter(*ret),
					[](const ChunkCopiesCalculator::Part& p) {return p.first;});
		}
		return availableParts_.size();
	}

	uint32_t count = 0;
	if (goal_->isXor()) {
		ChunkType::XorLevel level = goal_->xorLevel();
		std::bitset<goal::kMaxXorLevel + 1> parts;
		for (const auto& partWithLabel : availableParts_) {
			const ChunkType& part = partWithLabel.first;
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
		auto toRemove = ordinaryPartsToRecoverAndRemove().second;
		if (ret) {
			ret->assign(toRemove, ChunkType::getStandardChunkType());
			for (const auto& partWithLabel : availableParts_) {
				auto part = partWithLabel.first;
				if (part.isXorChunkType()) {
					ret->push_back(part);
					toRemove++;
				}
			}
		} else {
			for (const auto& partWithLabel : availableParts_) {
				if (partWithLabel.first.isXorChunkType()) {
					toRemove++;
				}
			}
		}
		return toRemove;
	}
	return count;
}
