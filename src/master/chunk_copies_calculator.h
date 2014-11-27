#pragma once

#include "common/platform.h"

#include "common/chunk_type.h"
#include "common/chunks_availability_state.h"
#include "common/goal.h"
#include "common/media_label.h"

class ChunkCopiesCalculator {
public:
	typedef std::pair<ChunkType, const MediaLabel*> Part;

	ChunkCopiesCalculator(const Goal* goal);
	void addPart(ChunkType chunkType, const MediaLabel* label);

	std::vector<ChunkType> getPartsToRecover() const;
	std::vector<ChunkType> getPartsToRemove() const;
	uint32_t countPartsToRecover() const;
	uint32_t countPartsToRemove() const;
	bool isRecoveryPossible() const;
	bool isWritingPossible() const;
	ChunksAvailabilityState::State getState() const;

private:
	std::vector<Part> availableParts_;
	const Goal* goal_;

	/*
	 * Assuming that goal_ is not XOR goal calculate how many parts are
	 * missing and redundant
	 */
	std::pair<uint32_t, uint32_t> ordinaryPartsToRecoverAndRemove() const;

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
