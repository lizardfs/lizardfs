/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

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

