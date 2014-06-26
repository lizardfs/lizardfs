#pragma once

#include "config.h"

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
		if (isGoalValid(goal)) {
			return goals_[goal - kMinGoal];
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
	T goals_[kMaxGoal - kMinGoal + 1];
};
