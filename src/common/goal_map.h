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

	LIZARDFS_DEFINE_SERIALIZE_METHODS(zero_, ordinary_, xor_);

private:
	T zero_;
	T ordinary_[goal::kMaxOrdinaryGoal - goal::kMinOrdinaryGoal + 1];
	T xor_[goal::kMaxXorGoal - goal::kMinXorGoal + 1];
};
