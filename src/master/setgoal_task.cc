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

#include "common/platform.h"

#include "master/setgoal_task.h"

#include "master/filesystem_checksum.h"
#include "master/filesystem_node.h"
#include "master/filesystem_operations.h"
#include "master/matotsserv.h"

int SetGoalTask::execute(uint32_t ts, std::list<std::unique_ptr<Task>> &work_queue) {

	FSNode *node = fsnodes_id_to_node(inode_);
	if (!node) {
		return LIZARDFS_ERROR_EINVAL;
	}
	uint8_t result = setGoal(node, ts);

	if (result != kNoAction) {
		if (node->type == FSNode::kDirectory && (smode_ & SMODE_RMASK)) {
			auto current_front = work_queue.begin();
			for (const auto &entry : static_cast<const FSNodeDirectory*>(node)->entries) {
				std::unique_ptr<SetGoalTask> task(new SetGoalTask(entry.second->id,
								  uid_, goal_, smode_, stats_));
				work_queue.insert(current_front, std::move(task));
			}
		}

		if ((smode_ & SMODE_RMASK) == 0 && result == kNotPermitted) {
			return LIZARDFS_ERROR_EPERM;
		}
		(*stats_)[result] += 1;
		fs_changelog(ts, "SETGOAL(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu8
			      "):%" PRIu32, inode_, uid_, goal_, smode_, result);
	}

	return LIZARDFS_STATUS_OK;
}

uint8_t SetGoalTask::setGoal(FSNode *node, uint32_t ts) {

	if (node->type == FSNode::kFile || node->type == FSNode::kDirectory || node->type == FSNode::kTrash ||
	    node->type == FSNode::kReserved) {
		if ((node->mode & (EATTR_NOOWNER << 12)) == 0 && uid_ != 0 && node->uid != uid_) {
			return SetGoalTask::kNotPermitted;
		} else {
			if ((smode_ & SMODE_TMASK) == SMODE_SET && node->goal != goal_) {
				if (node->type != FSNode::kDirectory) {
					fsnodes_changefilegoal(static_cast<FSNodeFile*>(node), goal_);
#ifndef METARESTORE
					if (matotsserv_can_enqueue_node()) {
						fsnodes_enqueue_tape_copies(node);
					}
#endif
				} else {
					node->goal = goal_;
				}
				node->ctime = ts;
				fsnodes_update_checksum(node);
				return SetGoalTask::kChanged;
			} else {
				return SetGoalTask::kNotChanged;
			}
		}
	}
	return SetGoalTask::kNoAction;
}
