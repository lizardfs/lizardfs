#include "config.h"
#include "master/chunk_goal_counters.h"

#include "common/goal.h"

ChunkGoalCounters::ChunkGoalCounters() : fileCount_(0), goal_(0) {}

void ChunkGoalCounters::addFile(uint8_t goal) {
	if (!isGoalValid(goal)) {
		throw InvalidOperation("Invalid goal " + std::to_string(goal));
	}
	if (fileCount_ == 0) {
		goal_ = goal;
		fileCount_ = 1;
		return;
	}

	if (fileCount_ == 1 && !fileCounters_) {
		fileCounters_.reset(new GoalMap<uint32_t>());
		(*fileCounters_)[goal_]++;
	}
	sassert(fileCounters_);
	(*fileCounters_)[goal]++;
	fileCount_++;
	goal_ = recalculateGoal();
}

void ChunkGoalCounters::removeFile(uint8_t goal) {
	removeFileInternal(goal);
	goal_ = recalculateGoal();
	if (fileCount_ == 1) {
		fileCounters_.reset();
	}
}

void ChunkGoalCounters::changeFileGoal(uint8_t prevGoal, uint8_t newGoal) {
	removeFileInternal(prevGoal);
	addFile(newGoal);
}

void ChunkGoalCounters::removeFileInternal(uint8_t goal) {
	if (!isGoalValid(goal)) {
		throw InvalidOperation("Invalid goal " + std::to_string(goal));
	}
	if (fileCounters_) {
		sassert(fileCount_ > 1);
		if ((*fileCounters_)[goal] == 0) {
			throw InvalidOperation("No file with goal " + std::to_string(goal) + " to remove");
		}
		(*fileCounters_)[goal]--;
	} else {
		sassert(fileCount_ == 1);
		if (goal_ != goal) {
			throw InvalidOperation("No file with goal " + std::to_string(goal) + " to remove");
		}
		// goal_ will be updated during recalculation
	}
	fileCount_--;
}

uint8_t ChunkGoalCounters::recalculateGoal() {
	if (fileCount_ == 0) {
		sassert(!fileCounters_);
		// No files - no goal
		return 0;
	} else {
		sassert(fileCounters_);
		for (uint8_t goal = kMaxGoal; goal >= kMinGoal; --goal) {
			// Effective goal is the highest used one
			if ((*fileCounters_)[goal] != 0) {
				return goal;
			}
		}
	}
	throw InvalidOperation("This should never happen");
}
