/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "master/chunk_goal_counters.h"

#include "common/goal.h"

void ChunkGoalCounters::addFile(uint8_t goal) {
	if (!GoalId::isValid(goal)) {
		throw ChunkGoalCounters::InvalidOperation("Trying to add non-existent goal: " + std::to_string(goal));
	}

	/*
	 * For memory saving, max value of a single counter is 255.
	 * If a counter is about to reach 256, a new counter for the same value
	 * is created instead.
	 *
	 * Example:
	 * Counters state: (0, 255), (1, 14), (2, 20)
	 * After adding another 0 it becomes:
	 *                 (0, 255), (0, 1), (1, 14), (2, 20)
	 */
	for (auto &counter : counters_) {
		if (goal == counter.goal
				&& counter.count < std::numeric_limits<uint8_t>::max()) {
				counter.count++;
				return;
		}
	}
	if (!counters_.full()) {
		counters_.push_back({goal, 1});
	} else {
		throw ChunkGoalCounters::InvalidOperation("There is no more space for goals");
	}
}

void ChunkGoalCounters::removeFile(uint8_t goal) {
	for (auto it = counters_.begin(); it != counters_.end(); ++it) {
		if (goal == it->goal) {
			it->count--;
			if (it->count == 0) {
				counters_.erase(it);
			}
			return;
		}
	}
	throw ChunkGoalCounters::InvalidOperation("Trying to remove non-existent goal: " + std::to_string(goal));
}

void ChunkGoalCounters::changeFileGoal(uint8_t prevGoal, uint8_t newGoal) {
	removeFile(prevGoal);
	addFile(newGoal);
}

uint32_t ChunkGoalCounters::fileCount() const {
	uint32_t sum = 0;
	for (auto counter : counters_) {
		sum += counter.count;
	}
	return sum;
}

uint8_t ChunkGoalCounters::highestIdGoal() const {
	return std::accumulate(counters_.begin(), counters_.end(), (uint8_t)0,
	                       [](uint8_t a, const GoalCounter &b) { return std::max(a, b.goal); });
}
