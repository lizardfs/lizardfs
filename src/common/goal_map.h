#pragma once

#include "common/platform.h"

#include <array>
#include <cstdint>
#include <map>

#include "common/exception.h"
#include "common/goal.h"
#include "common/serialization_macros.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(GoalMapInvalidGoalException, Exception);

template <class T>
class GoalMap {
public:
	GoalMap() : zero_(), goals_() {}

	T& operator[](uint8_t goal) {
		if (goal::isGoalValid(goal)) {
			return goals_[goal - goal::kMinGoal];
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
		for (unsigned goal = goal::kMinGoal; goal <= goal::kMaxGoal; ++goal) {
			sassert(this->operator[](goal) == defaultValue);
		}

		// deserialize the map
		std::map<uint8_t, T> map;
		::deserialize(source, bytesLeftInBuffer, map);
		for (auto& goalAndValue : map) {
			uint8_t goal = goalAndValue.first;
			if (goal::isGoalValid(goal) || goal == 0) {
				this->operator[](goal) = std::move(goalAndValue.second);
			}
		}
	}
private:
	std::map<uint8_t, T> goalsAsMap() const {
		std::map<uint8_t, T> trueMap;
		T defaultValue = T();

		for (unsigned i = goal::kMinGoal; i <= goal::kMaxGoal; ++i) {
			if (this->operator[](i) != defaultValue) { // Packet size optimization!
				trueMap[i] = this->operator[](i);
			}
		}
		if (zero_ != defaultValue) {
			trueMap[0] = zero_;
		}
		return trueMap;
	}

	T zero_;
	std::array<T, goal::kNumberOfGoals> goals_;
};
