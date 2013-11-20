#include "common/goal.h"

#include <limits>

#include "common/massert.h"

ChunkType::XorLevel goalToXorLevel(uint8_t goal) {
	sassert(isXorGoal(goal));
	return ~goal + kMinXorLevel;
}

bool isGoalValid(uint8_t goal) {
	return isOrdinaryGoal(goal) || isXorGoal(goal);
}

bool isOrdinaryGoal(uint8_t goal) {
	return goal >= kMinOrdinaryGoal && goal <= kMaxOrdinaryGoal;
}

bool isXorGoal(uint8_t goal) {
	return goal >= std::numeric_limits<uint8_t>::max() - kMaxXorLevel + kMinXorLevel;
}

uint8_t xorLevelToGoal(ChunkType::XorLevel xorLevel) {
	sassert(xorLevel >= kMinXorLevel);
	sassert(xorLevel <= kMaxXorLevel);
	return std::numeric_limits<uint8_t>::max() - xorLevel + kMinXorLevel;
}
