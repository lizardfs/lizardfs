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

#include <memory>

#include "common/special_inode_defs.h"
#include "master/filesystem_node.h"
#include "master/filesystem_operations.h"
#include "master/hstring.h"
#include "master/task_manager.h"

/*! \brief Implementation of Recursive Remove Task that works with Task Manager.
 *
 * RemoveTask class is used for removing files and directories with
 * their entire content. Each task is responsible for removing one node and
 * it's execution depends on the type of node. If node is a not empty directory,
 * it adds it's children to the front of the queue of tasks. Otherwise, if node
 * is empty or not a directory, task removes the node and itself from the queue.
 *
 * Processing of enqueued tasks is done by Task Manager class.
 */
class RemoveTask : public TaskManager::Task {
public:
	typedef std::vector<HString> SubtaskContainer;

	RemoveTask(SubtaskContainer &&subtask, uint32_t parent,
		   const std::shared_ptr<FsContext> &context) :
		   subtask_(std::move(subtask)), parent_(parent),
		   context_(context), repeat_counter_(0) {

		current_subtask_ = subtask_.begin();
	}

	/*! \brief Execute task specified by this RemoveTask object.
	 *
	 * This function overrides pure virtual execute function of TaskManager::Task.
	 * It is the only function to be called by Task Manager in order to
	 * execute enqueued task.
	 *
	 * \param ts current time stamp.
	 * \param work_queue a list to which this task adds newly created tasks.
	 */
	int execute(uint32_t ts, intrusive_list<Task> &work_queue) override;

	bool isFinished() const override;

	static std::string generateDescription(const std::string &target) {
		return "Recursive remove: " + target;
	}

private:
	int retrieveNodes(FSNodeDirectory *&wd, FSNode *&child);

	/*! \brief Execute unlink operation to remove node. */
	void doUnlink(uint32_t ts, FSNodeDirectory *wd, FSNode *child);

private:
	static const uint32_t kMaxRepeatCounter = 3;

	SubtaskContainer subtask_;
	SubtaskContainer::iterator current_subtask_;

	uint32_t parent_;
	std::shared_ptr<FsContext> context_;
	uint32_t repeat_counter_;
};
