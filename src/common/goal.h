#ifndef LIZARDFS_MFSCOMMON_GOAL_H_
#define LIZARDFS_MFSCOMMON_GOAL_H_

#include <inttypes.h>

typedef uint8_t GoalID;

const uint8_t kMinXorLevel = 2;
const uint8_t kMaxXorLevel = 10;

GoalID xorGoalID(uint8_t xorLevel);
GoalID ordinaryGoalID(uint8_t goalLevel);

#endif // LIZARDFS_MFSCOMMON_GOAL_H_
