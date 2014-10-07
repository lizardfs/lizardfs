#include "common/platform.h"
#include "master/chunk_goal_counters.h"

#include "common/goal.h"

ChunkGoalCounters::ChunkGoalCounters() : fileCount_(0), goal_(0) {}

void ChunkGoalCounters::addFile(uint8_t goal) {
	if (!goal::isGoalValid(goal)) {
		throw InvalidOperation("Invalid goal " + std::to_string(goal));
	}
	if (fileCount_ == 0) {
		goal_ = goal;
		fileCount_ = 1;
		return;
	}
	if (!fileCounters_ && goal != goal_) {
		fileCounters_.reset(new GoalMap<uint32_t>());
		(*fileCounters_)[goal_] = fileCount_;
	}
	fileCount_++;
	if (fileCounters_) {
		(*fileCounters_)[goal]++;
	}
	goal_ = calculateGoal();
}

void ChunkGoalCounters::removeFile(uint8_t goal) {
	removeFileInternal(goal);
	goal_ = calculateGoal();
	tryDeleteFileCounters();
}

void ChunkGoalCounters::changeFileGoal(uint8_t prevGoal, uint8_t newGoal) {
	removeFileInternal(prevGoal);
	addFile(newGoal);
	tryDeleteFileCounters();
}

/*
 * The algorithm here works as follows:
 * * if there is at least one file with goal > 2, than choose the safest of ordinary goals,
 *   ie. the biggest ordinary goal
 * * otherwise, if there are files with xor goals or goal 2 (these are all equally safe),
 *   choose the one which occupies least space (ie. the highest xor level, or goal 2 if no xors)
 * * otherwise choose goal 1
 */
uint8_t ChunkGoalCounters::calculateGoal() {
	if (fileCount_ == 0) {
		sassert(!fileCounters_);
		// No files - no goal
		return 0;
	} else if (fileCounters_) {
		sassert(3 >= goal::kMinOrdinaryGoal);
		for (int goal = goal::kMaxOrdinaryGoal; goal >= 3; --goal) {
			if ((*fileCounters_)[goal] != 0) {
				return goal;
			}
		}
		for (int level = goal::kMaxXorLevel; level >= goal::kMinXorLevel; --level) {
			if ((*fileCounters_)[goal::xorLevelToGoal(level)] != 0) {
				return goal::xorLevelToGoal(level);
			}
		}
		sassert(2 <= goal::kMaxOrdinaryGoal);
		for (int goal = 2; goal >= goal::kMinOrdinaryGoal; --goal) {
			if ((*fileCounters_)[goal] != 0) {
				return goal;
			}
		}
	} else {
		return goal_;
	}
	throw InvalidOperation("This should never happen");
}

void ChunkGoalCounters::tryDeleteFileCounters() {
	if (!fileCounters_) {
		return;
	}

	uint8_t goalsUsed = 0;
	for (uint8_t iGoal = goal::kMaxOrdinaryGoal; iGoal >= goal::kMinOrdinaryGoal; --iGoal) {
		if ((*fileCounters_)[iGoal] > 0) {
			++goalsUsed;
		}
	}
	// if i were uint8_t, the condition "i <= goal::kMaxXorGoal" could be always satisfied
	for (int i = goal::kMinXorGoal; i <= goal::kMaxXorGoal; ++i) {
		if ((*fileCounters_)[i] > 0) {
			++goalsUsed;
		}
	}
	// Found only one goal used? Optimise!
	if (goalsUsed == 1) {
		fileCounters_.reset();
	}
}

void ChunkGoalCounters::removeFileInternal(uint8_t goal) {
	if (!goal::isGoalValid(goal)) {
		throw InvalidOperation("Invalid goal " + std::to_string(goal));
	}
	if (fileCounters_) {
		sassert(fileCount_ > 1);
		if ((*fileCounters_)[goal] == 0) {
			throw InvalidOperation("No file with goal " + std::to_string(goal) + " to remove");
		}
		(*fileCounters_)[goal]--;
	} else {
		if (goal_ != goal) {
			throw InvalidOperation("No file with goal " + std::to_string(goal) + " to remove");
		}
	}
	fileCount_--;
}
