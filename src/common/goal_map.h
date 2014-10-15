#pragma once

#include "common/platform.h"

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

	uint32_t serializedSize() const {
		auto map = goalsAsMap();
		return ::serializedSize(zero_, map);
	}
	void serialize(uint8_t** destination) const {
		auto map = goalsAsMap();
		::serialize(destination, zero_, map);
	}
	void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
		T defaultValue = T();
		sassert(zero_ == defaultValue);

		std::map<uint8_t, T> map;
		::deserialize(source, bytesLeftInBuffer, zero_, map);

		for (unsigned i = goal::kMinGoal; i <= goal::kMaxGoal; ++i) {
			sassert(this->operator[](i) == defaultValue);
			this->operator[](i) = map[i];
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
		return trueMap;
	}

	T zero_;
	T goals_[goal::kNumberOfGoals];
};
