#pragma once

#include "common/platform.h"

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

	// true if this class has some additional memory allocated on the heap
	// The class gives the following three guarantees:
	// * it occupies no additional memory if only files with the same goal were added
	// * it occupies no additional memory if there are no files or there is only one file added
	// * it occupies additional memory if there are files with different goals added
	bool hasAdditionalMemoryAllocated() const {
		return fileCounters_ != nullptr;
	}

private:
	std::unique_ptr<GoalMap<uint32_t>> fileCounters_;
	uint32_t fileCount_;
	uint8_t goal_;

	uint8_t calculateGoal();

	// Free fileCounter_ when it's unneeded
	void tryDeleteFileCounters();

	void removeFileInternal(uint8_t goal);
};
