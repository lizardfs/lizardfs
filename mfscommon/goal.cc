#include <limits>

#include "mfscommon/goal.h"
#include "mfscommon/massert.h"

using namespace lizardfs;

GoalID xorGoalID(uint8_t partitionsNumber) {
	eassert(partitionsNumber >= MinXorPartitionsNumber);
	eassert(partitionsNumber <= MaxXorPartitionsNumber);

	return std::numeric_limits<GoalID>::max() - partitionsNumber + MinXorPartitionsNumber;
}

GoalID ordinaryGoalID(uint8_t goalLevel) {
	eassert(goalLevel <= 10);

	return goalLevel;
}
