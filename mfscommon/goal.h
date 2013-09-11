#ifndef LIZARDFS_MFSCOMMON_GOAL_H
#define LIZARDFS_MFSCOMMON_GOAL_H

#include <inttypes.h>

namespace lizardfs {

typedef uint8_t GoalID;

const uint8_t MinXorPartitionsNumber = 2;
const uint8_t MaxXorPartitionsNumber = 10;

GoalID xorGoalID(uint8_t partitionsNumber);

GoalID ordinaryGoalID(uint8_t goalLevel);

} // namespace lizardfs

#endif // LIZARDFS_MFSCOMMON_GOAL_H
