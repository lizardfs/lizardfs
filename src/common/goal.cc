#include "common/platform.h"
#include "common/goal.h"

#include <limits>

#include "common/massert.h"

namespace goal {

ChunkType::XorLevel toXorLevel(uint8_t goal) {
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

const std::vector<uint8_t>& allGoals() {
	auto f = []() {
		std::vector<uint8_t> ret;
		for (uint8_t goal = goal::kMinOrdinaryGoal; goal <= goal::kMaxOrdinaryGoal; goal++) {
			ret.push_back(goal);
		}
		for (unsigned level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
			ret.push_back(goal::xorLevelToGoal(level));
		}
		return ret;
	};
	static std::vector<uint8_t> ret = f();
	return ret;
}

}

