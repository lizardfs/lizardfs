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

#pragma once

#include "common/platform.h"

#include "common/compact_vector.h"
#include "common/exception.h"

/*! \brief Goal counting class for chunks
 *
 * This class is used to calculate superposition of goals
 * for chunks belonging to multiple inodes (e.g. through snapshotting).
 * Chunks referenced by 1 inode inherit their goal without any computations.
 *
 * Underlying data structure is a compact_vector, which is designed to occupy
 * as little memory as needed.
 *
 * Superposition is counted as a set sum of existing goals.
 * Example:
 *  goal of file1: label1 label2
 *  goal of file2: label2 label3
 *  counted goal of chunk shared between file1 and file2: label1 label2 label3
 *  counted goal of shared chunk after changing file1 goal to _: label2 label3
 */
class ChunkGoalCounters {
public:

	struct GoalCounter {
		uint8_t goal;
		uint8_t count;

		bool operator==(const GoalCounter &other) const {
			return goal == other.goal && count == other.count;
		}
	};

	typedef compact_vector<GoalCounter> Counters;
	typedef Counters::const_iterator const_iterator;

	LIZARDFS_CREATE_EXCEPTION_CLASS(InvalidOperation, Exception);

	ChunkGoalCounters() {}

	// Adds file with a given goal to calculations
	void addFile(uint8_t goal);

	// Removes file with a given goal from calculations
	void removeFile(uint8_t goal);

	// Changes goal of one of the added files
	void changeFileGoal(uint8_t prevGoal, uint8_t newGoal);

	// Returns number of files referring to a chunk
	uint32_t fileCount() const;

	// For backward compatibility - returns highest goalId in counters
	uint8_t highestIdGoal() const;

	const_iterator begin() const {
		return counters_.begin();
	}

	const_iterator end() const {
		return counters_.end();
	}

	Counters::size_type size() const {
		return counters_.size();
	}

private:
	Counters counters_;
};
