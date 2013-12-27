#pragma once

#include "common/chunk_type.h"

class ChunkCopiesCalculator {
public:
	ChunkCopiesCalculator(uint8_t goal);
	void addPart(ChunkType chunkType);

	std::vector<ChunkType> getPartsToRecover();
	std::vector<ChunkType> getPartsToRemove();
	bool isRecoveryPossible();
	const std::vector<ChunkType>& availableParts() const { return availableParts_; }

private:
	std::vector<ChunkType> availableParts_;
	uint8_t goal_;
};
