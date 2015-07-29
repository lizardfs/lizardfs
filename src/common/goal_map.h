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

#include <cstdint>

#include "common/chunk_type.h"
#include "common/exception.h"
#include "common/goal.h"
#include "common/serialization_macros.h"
#include "common/slice_traits.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(GoalMapInvalidGoalException, Exception);

template <class T>
class GoalMap {
public:
	GoalMap() : zero_(), goals_() {}

	T& operator[](uint8_t goal) {
		if (GoalId::isValid(goal)) {
			return goals_[goal];
		}
		if (goal == 0) {
			return zero_;
		}
		throw GoalMapInvalidGoalException("Invalid goal: " + std::to_string((uint32_t)goal));
	}

	const T& operator[](uint8_t goal) const {
		return const_cast<GoalMap&>(*this)[goal];
	}

	friend bool operator==(const GoalMap<T>& eins, const GoalMap<T>& zwei) {
		return eins.zero_ == zwei.zero_ && eins.goals_ == zwei.goals_;
	}

	uint32_t serializedSize() const {
		return ::serializedSize(goalsAsMap());
	}
	void serialize(uint8_t** destination) const {
		::serialize(destination, goalsAsMap());
	}
	void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
		// verify if the map is empty
		T defaultValue = T();
		sassert(this->operator[](0) == defaultValue);
		for (auto goal = GoalId::kMin; goal <= GoalId::kMax; ++goal) {
			sassert(this->operator[](goal) == defaultValue);
		}

		// deserialize the map
		std::map<uint8_t, T> map;
		::deserialize(source, bytesLeftInBuffer, map);
		for (auto& goalAndValue : map) {
			uint8_t goal = goalAndValue.first;
			if (GoalId::isValid(goal) || goal == 0) {
				this->operator[](goal) = std::move(goalAndValue.second);
			}
		}
	}

private:
	std::map<uint8_t, T> goalsAsMap() const {
		std::map<uint8_t, T> trueMap;
		T defaultValue = T();

		for (auto goal = GoalId::kMin; goal <= GoalId::kMax; ++goal) {
			if (this->operator[](goal) != defaultValue) { // Packet size optimization!
				trueMap[goal] = this->operator[](goal);
			}
		}
		if (zero_ != defaultValue) {
			trueMap[0] = zero_;
		}
		return trueMap;
	}

	T zero_;
	std::array<T, GoalId::kMax> goals_;
};
