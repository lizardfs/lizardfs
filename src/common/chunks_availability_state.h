/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include "common/goal.h"
#include "common/goal_map.h"
#include "common/serialization.h"

class ChunksAvailabilityState {
public:
	enum State {
		kSafe,
		kEndangered,
		kLost
	};

	uint64_t safeChunks(uint8_t goal) const {
		return safeChunks_[goal];
	}
	uint64_t endangeredChunks(uint8_t goal) const {
		return endangeredChunks_[goal];
	}
	uint64_t lostChunks(uint8_t goal) const {
		return lostChunks_[goal];
	}
	void addChunk(uint8_t goal, State state) {
		getMapForState(state)[goal]++;
	}
	void removeChunk(uint8_t goal, State state) {
		getMapForState(state)[goal]--;
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(safeChunks_, endangeredChunks_, lostChunks_);

private:
	GoalMap<uint64_t>& getMapForState(State state) {
		switch (state) {
		case kSafe:
			return safeChunks_;
		case kEndangered:
			return endangeredChunks_;
		case kLost:
			return lostChunks_;
		default:
			mabort("invalid state");
		}
	}

	// count of safe/endangered/lost chunks for each goal
	GoalMap<uint64_t> safeChunks_;
	GoalMap<uint64_t> endangeredChunks_;
	GoalMap<uint64_t> lostChunks_;
};

class ChunksReplicationState {
public:
	static constexpr uint32_t kMaxPartsCount = 10U;

	uint64_t chunksToReplicate(uint8_t goal, uint32_t missingParts) const {
		sassert(missingParts <= kMaxPartsCount);
		return chunksToReplicate_[goal][missingParts];
	}
	uint64_t chunksToDelete(uint8_t goal, uint32_t redundantParts) const {
		sassert(redundantParts <= kMaxPartsCount);
		return chunksToDelete_[goal][redundantParts];
	}
	void addChunk(uint8_t goal, uint32_t missingParts, uint32_t redundantParts) {
		missingParts = std::min(missingParts, kMaxPartsCount);
		redundantParts = std::min(redundantParts, kMaxPartsCount);
		chunksToReplicate_[goal][missingParts]++;
		chunksToDelete_[goal][redundantParts]++;
	}
	void removeChunk(uint8_t goal, uint32_t missingParts, uint32_t redundantParts) {
		missingParts = std::min(missingParts, kMaxPartsCount);
		redundantParts = std::min(redundantParts, kMaxPartsCount);
		chunksToReplicate_[goal][missingParts]--;
		chunksToDelete_[goal][redundantParts]--;
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(chunksToReplicate_, chunksToDelete_);

private:
	// count of chunks that need replication/deletion for each goal and number of missing parts
	GoalMap<std::array<uint64_t, kMaxPartsCount + 1>> chunksToReplicate_;
	GoalMap<std::array<uint64_t, kMaxPartsCount + 1>> chunksToDelete_;
};
