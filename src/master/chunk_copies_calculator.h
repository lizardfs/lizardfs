#pragma once

#include "common/platform.h"

#include "common/chunk_type.h"
#include "common/chunks_availability_state.h"

class ChunkCopiesCalculator {
public:
	ChunkCopiesCalculator(uint8_t goal);
	void addPart(ChunkType chunkType);

	std::vector<ChunkType> getPartsToRecover() const;
	std::vector<ChunkType> getPartsToRemove() const;
	uint32_t countPartsToRecover() const;
	uint32_t countPartsToRemove() const;
	bool isRecoveryPossible() const;
	bool isWritingPossible() const;
	ChunksAvailabilityState::State getState() const;
	const std::vector<ChunkType>& availableParts() const { return availableParts_; }
	uint8_t goal() const { return goal_; }

private:
	std::vector<ChunkType> availableParts_;
	uint8_t goal_;

	/*
	 * Returns then number of copies to recover.
	 * If ret != nullptr, fills it with all such copies;
	 */
	uint32_t getPartsToRecover(std::vector<ChunkType>* ret) const;

	/*
	 * Returns then number of copies to remove.
	 * If ret != nullptr, fills it with all such copies;
	 */
	uint32_t getPartsToRemove(std::vector<ChunkType>* ret) const;
};
