/*
   Copyright 2016-2017 Skytechnology sp. z o.o.

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

	SetTrashtimeTask(std::vector<uint32_t> inode_list, uint32_t uid, uint32_t trashtime, uint8_t smode,
		         const std::shared_ptr<StatsArray> &settrashtime_stats) :
		         inode_list_(inode_list), uid_(uid), trashtime_(trashtime), smode_(smode),
		         stats_(settrashtime_stats) {
		assert(!inode_list_.empty());
		current_inode_ = inode_list_.begin();
	}

	SetTrashtimeTask(uint32_t uid, uint32_t trashtime, uint8_t smode) :
		         inode_list_(), uid_(uid), trashtime_(trashtime), smode_(smode),
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
	int execute(uint32_t ts, intrusive_list<Task> &work_queue) override;

	bool isFinished() const override;

	static std::string generateDescription(const std::string &target, uint32_t trashtime) {
		return "Setting trashtime (" + std::to_string(trashtime) + "): " + target;
	}

	uint8_t setTrashtime(FSNode *node, uint32_t ts);

private:
	std::vector<uint32_t> inode_list_;
	std::vector<uint32_t>::iterator current_inode_;

	uint32_t uid_;
	uint32_t trashtime_;
	uint8_t smode_;
	std::shared_ptr<StatsArray> stats_; /*< array for settrashtime operation statistics
			                       [kChanged] - number of inodes with changed trashtime
			                    [kNotChanged] - number of inodes with not changed trashtime
			                  [kNotPermitted] - number of inodes with permission denied */
};
