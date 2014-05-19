#pragma once

#include "config.h"

#include "common/exception.h"
#include "common/goal_map.h"

class ChunkGoalCounters {
public:
	LIZARDFS_CREATE_EXCEPTION_CLASS(InvalidOperation, Exception);

	// Default constructor
	ChunkGoalCounters();

	// Add file with a given goal to calculations
	void addFile(uint8_t goal);

	// Remove file with a given goal from calculations
	void removeFile(uint8_t goal);

	// Change goal of one of the added files
	void changeFileGoal(uint8_t prevGoal, uint8_t newGoal);

	// Calculate a superposition of goals of the added files
	uint8_t combinedGoal() const {
		return goal_;
	}

	// Number of added files
	uint32_t fileCount() const {
		return fileCount_;
	}

private:
	std::unique_ptr<GoalMap<uint32_t>> fileCounters_;
	uint32_t fileCount_;
	uint8_t goal_;

	uint8_t recalculateGoal();
	void removeFileInternal(uint8_t goal);
};
