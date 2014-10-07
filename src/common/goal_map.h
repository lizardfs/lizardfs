#pragma once

#include "common/platform.h"

#include <cstdint>

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

	LIZARDFS_DEFINE_SERIALIZE_METHODS(zero_, goals_);

private:
	T zero_;
	T goals_[goal::kNumberOfGoals];
};
