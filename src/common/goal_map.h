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
		if (isOrdinaryGoal(goal)) {
			return ordinary_[goal - kMinOrdinaryGoal];
		}
		if (isXorGoal(goal)) {
			return xor_[goal - kMinXorGoal];
		}
		if (goal == 0) {
			return zero_;
		}
		throw GoalMapInvalidGoalException("Invalid goal: " + std::to_string((uint32_t)goal));
	}

	const T& operator[](uint8_t goal) const {
		return const_cast<GoalMap&>(*this)[goal];
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(zero_, ordinary_, xor_);

private:
	T zero_;
	T ordinary_[kMaxOrdinaryGoal - kMinOrdinaryGoal + 1];
	T xor_[kMaxXorGoal - kMinXorGoal + 1];
};
