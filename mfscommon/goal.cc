#include <limits>

#include "mfscommon/goal.h"
#include "mfscommon/massert.h"

GoalID xorGoalID(uint8_t level) {
	eassert(level >= MinXorLevel);
	eassert(level <= MaxXorLevel);

	return std::numeric_limits<GoalID>::max() - level + MinXorLevel;
}

GoalID ordinaryGoalID(uint8_t goalLevel) {
	eassert(goalLevel <= 10);

	return goalLevel;
}

bool isValidXorGoal(uint8_t level) {
	xorGoalID(MinXorLevel);
	GoalID minXorGoal = xorGoalID(MinXorLevel);
	GoalID maxXorGoal = xorGoalID(MaxXorLevel);
	return level >= minXorGoal && level <= maxXorGoal;
}
