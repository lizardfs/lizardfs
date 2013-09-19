#include <limits>

#include "mfscommon/goal.h"
#include "mfscommon/massert.h"

GoalID xorGoalID(uint8_t xorLevel) {
	sassert(xorLevel >= MinXorLevel);
	sassert(xorLevel <= MaxXorLevel);
	return std::numeric_limits<GoalID>::max() - xorLevel + MinXorLevel;
}

GoalID ordinaryGoalID(uint8_t goalLevel) {
	sassert(goalLevel <= 10);
	return goalLevel;
}
