/*
   Copyright 2016 Skytechnology sp. z o.o.

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

#include "master/filesystem_node.h"
#include "master/task_manager.h"

class SetTrashtimeTask : public TaskManager::Task {
public:
	enum {
	  kChanged = 0,
	  kNotChanged,
	  kNotPermitted,
	  kStatsSize,
	  kNoAction
	};

	typedef std::array<uint32_t, kStatsSize> StatsArray;

	SetTrashtimeTask(uint32_t inode, uint32_t uid, uint32_t trashtime, uint8_t smode,
		         const std::shared_ptr<StatsArray> &settrashtime_stats) :
		         inode_(inode), uid_(uid), trashtime_(trashtime), smode_(smode),
		         stats_(settrashtime_stats) {
		         }

	SetTrashtimeTask(uint32_t inode, uint32_t uid, uint32_t trashtime, uint8_t smode) :
		         inode_(inode), uid_(uid), trashtime_(trashtime), smode_(smode),
		         stats_() {
		         }

	/*! \brief Execute task specified by this SetTrashtimeTask object.
	 *
	 * This function overrides pure virtual execute function of TaskManager::Task.
	 * It is the only function to be called by Task Manager in order to
	 * execute enqueued task.
	 *
	 * \param ts current time stamp.
	 * \param work_queue a list to which this task adds newly created tasks.
	 * \return status value that indicates whether operation was successful.
	 */
	int execute(uint32_t ts, std::list<std::unique_ptr<Task>> &work_queue);

	uint8_t setTrashtime(FSNode *node, uint32_t ts);

	bool isFinished() const {
		return true;
	}
private:
	uint32_t inode_;
	uint32_t uid_;
	uint32_t trashtime_;
	uint8_t smode_;
	std::shared_ptr<StatsArray> stats_; /*< array for settrashtime operation statistics
			                       [kChanged] - number of inodes with changed trashtime
			                    [kNotChanged] - number of inodes with not changed trashtime
			                  [kNotPermitted] - number of inodes with permission denied */
};
