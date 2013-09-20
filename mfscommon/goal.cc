#include <limits>

#include "mfscommon/goal.h"
#include "mfscommon/massert.h"

GoalID xorGoalID(uint8_t xorLevel) {
	sassert(xorLevel >= kMinXorLevel);
	sassert(xorLevel <= kMaxXorLevel);
	return std::numeric_limits<GoalID>::max() - xorLevel + kMinXorLevel;
}

GoalID ordinaryGoalID(uint8_t goalLevel) {
	sassert(goalLevel <= 10);
	return goalLevel;
}
