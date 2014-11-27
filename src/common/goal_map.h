#pragma once

#include "common/platform.h"

#include <cstdint>

#include "common/chunk_type.h"
#include "common/exception.h"
#include "common/goal.h"
#include "common/serialization_macros.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(GoalMapInvalidGoalException, Exception);

template <class T>
class GoalMap {
public:
	GoalMap() : zero_(), ordinary_(), xor_() {}

	T& operator[](uint8_t goal) {
		if (goal::isOrdinaryGoal(goal)) {
			return ordinary_[goal - goal::kMinOrdinaryGoal];
		}
		if (goal::isXorGoal(goal)) {
			return xor_[goal - goal::kMinXorGoal];
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
		return eins.zero_ == zwei.zero_ && eins.ordinary_ == zwei.ordinary_
				&& eins.xor_ == zwei.xor_;
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
		for (auto goal : goal::allGoals()) {
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

		for (auto goal : goal::allGoals()) {
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
	std::array<T, goal::kNumberOfOrdinaryGoals> ordinary_;
	std::array<T, goal::kNumberOfXorGoals> xor_;
};
