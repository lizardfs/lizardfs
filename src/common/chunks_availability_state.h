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

#include <array>

#include "common/goal.h"
#include "common/serialization.h"
#include "common/serialization_macros.h"

namespace detail {

template <class T>
class SerializableGoalIdArray : public std::array<T, GoalId::kMax + 1> {
	typedef std::array<T, GoalId::kMax + 1> base;

public:
	uint32_t serializedSize() const {
		return ::serializedSize(goalsAsMap());
	}

	void serialize(uint8_t **destination) const {
		::serialize(destination, goalsAsMap());
	}

	void deserialize(const uint8_t **source, uint32_t &bytes_left_in_buffer) {
		// verify if the map is empty
		for (uint8_t goal = 0; goal <= GoalId::kMax; ++goal) {
			assert(operator[](goal) == T());
		}

		// deserialize the map
		std::map<uint8_t, T> map;
		::deserialize(source, bytes_left_in_buffer, map);
		for (const auto &goal_and_value : map) {
			operator[](goal_and_value.first) = goal_and_value.second;
		}
	}

	using base::operator[];

private:
	std::map<uint8_t, T> goalsAsMap() const {
		std::map<uint8_t, T> result;
		T default_value = T();

		for (uint8_t goal = 0; goal <= GoalId::kMax; ++goal) {
			if (operator[](goal) != default_value) {  // Packet size optimization!
				result[goal] = operator[](goal);
			}
		}

		return result;
	}
};

}  // detail

class ChunksAvailabilityState {
public:
	enum State { kSafe = 0, kEndangered, kLost };

	ChunksAvailabilityState() : data_() {
	}

	uint64_t safeChunks(uint8_t goal) const {
		return data_[kSafe][goal];
	}
	uint64_t endangeredChunks(uint8_t goal) const {
		return data_[kEndangered][goal];
	}
	uint64_t lostChunks(uint8_t goal) const {
		return data_[kLost][goal];
	}
	void addChunk(uint8_t goal, State state) {
		getMapForState(state)[goal]++;
	}
	void removeChunk(uint8_t goal, State state) {
		getMapForState(state)[goal]--;
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(data_);

private:
	detail::SerializableGoalIdArray<uint64_t> &getMapForState(State state) {
		assert(state >= kSafe && state <= kLost);
		return data_[state];
	}

	// count of safe/endangered/lost chunks for each goal
	std::array<detail::SerializableGoalIdArray<uint64_t>, 3> data_;
};

/*! \brief Class for keeping statistics for chunk parts.
 *
 * Basically this is 2x two dimensional matrix for keeping integer values
 * describing number of chunk part copies.
 *
 * Names of the class methods start to make sens after looking
 * at the cgi table "All chunks state matrix".
 */
class ChunksReplicationState {
public:
	constexpr static uint32_t kMaxPartsCount = 11; /*!< Maximum number of parts that we keep
	                                                    in statistics. */

	static_assert(kMaxPartsCount == Goal::Slice::kMaxPartsCount,
	              "Most of the code assumes that those two values are equal. Do not change "
	              "without tests.");

	ChunksReplicationState() : chunksToReplicate_(), chunksToDelete_() {
	}

	uint64_t chunksToReplicate(uint8_t goal, uint32_t missingParts) const {
		assert(missingParts < kMaxPartsCount);
		return chunksToReplicate_[goal][missingParts];
	}
	uint64_t chunksToDelete(uint8_t goal, uint32_t redundantParts) const {
		assert(redundantParts < kMaxPartsCount);
		return chunksToDelete_[goal][redundantParts];
	}
	void addChunk(uint8_t goal, uint32_t missingParts, uint32_t redundantParts) {
		missingParts = std::min(missingParts, kMaxPartsCount - 1);
		redundantParts = std::min(redundantParts, kMaxPartsCount - 1);
		chunksToReplicate_[goal][missingParts]++;
		chunksToDelete_[goal][redundantParts]++;
	}
	void removeChunk(uint8_t goal, uint32_t missingParts, uint32_t redundantParts) {
		missingParts = std::min(missingParts, kMaxPartsCount - 1);
		redundantParts = std::min(redundantParts, kMaxPartsCount - 1);
		chunksToReplicate_[goal][missingParts]--;
		chunksToDelete_[goal][redundantParts]--;
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(chunksToReplicate_, chunksToDelete_);

private:
	// count of chunks that need replication/deletion for each goal and number of missing parts
	detail::SerializableGoalIdArray<std::array<uint64_t, kMaxPartsCount>> chunksToReplicate_;
	detail::SerializableGoalIdArray<std::array<uint64_t, kMaxPartsCount>> chunksToDelete_;
};
