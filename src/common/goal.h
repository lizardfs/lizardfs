#pragma once

#include "common/platform.h"

#include <cstdint>

constexpr uint8_t kMinGoal = 1;
constexpr uint8_t kMaxGoal = 9;

inline bool isGoalValid(uint8_t goal) {
	return goal >= kMinGoal && goal <= kMaxGoal;
}
