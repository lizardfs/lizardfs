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

#pragma once

#include "common/platform.h"

#include "common/chunk_part_type.h"
#include "common/chunks_availability_state.h"
#include "common/goal.h"
#include "common/media_label.h"

class ChunkCopiesCalculator {
public:
	typedef std::pair<ChunkPartType, MediaLabel> Part;

	ChunkCopiesCalculator(const Goal* goal);
	void addPart(ChunkPartType chunkType, const MediaLabel& label);

	std::vector<ChunkPartType> getPartsToRecover() const;
	std::vector<ChunkPartType> getPartsToRemove() const;
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
	uint32_t getPartsToRecover(std::vector<ChunkPartType>* ret) const;

	/*
	 * Returns then number of copies to remove.
	 * If ret != nullptr, fills it with all such copies;
	 */
	uint32_t getPartsToRemove(std::vector<ChunkPartType>* ret) const;
};
