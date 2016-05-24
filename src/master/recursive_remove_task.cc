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

#include "common/platform.h"

#include "master/recursive_remove_task.h"

bool RemoveTask::isFinished() const {
	return current_subtask_ == subtask_.end();
}

int RemoveTask::retrieveNodes(FSNodeDirectory *&wd, FSNode *&child) {
	FSNode *wd_tmp = fsnodes_id_to_node(parent_);
	if (!wd_tmp) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (!fsnodes_access(*context_, wd_tmp, MODE_MASK_W)) {
		return LIZARDFS_ERROR_EACCES;
	}
	wd = static_cast<FSNodeDirectory*>(wd_tmp);
	child = fsnodes_lookup(wd, *current_subtask_);
	if (!child) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (!fsnodes_sticky_access(wd, child, context_->uid())) {
		return LIZARDFS_ERROR_EPERM;
	}
	return LIZARDFS_STATUS_OK;
}

void RemoveTask::doUnlink(uint32_t ts, FSNodeDirectory *wd, FSNode *child) {
	fs_changelog(ts, "UNLINK(%" PRIu32 ",%s):%" PRIu32, parent_,
		    fsnodes_escape_name(*current_subtask_).c_str(), child->id);
	fsnodes_unlink(ts, wd, *current_subtask_, child);
}

int RemoveTask::execute(uint32_t ts, intrusive_list<Task> &work_queue) {
	FSNodeDirectory *wd = nullptr;
	FSNode *child = nullptr;
	int status = retrieveNodes(wd, child);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (child->type == FSNode::kDirectory &&
	    !static_cast<FSNodeDirectory*>(child)->entries.empty()) {

		SubtaskContainer subtasks;
		subtasks.reserve(
			  static_cast<const FSNodeDirectory*>(child)->entries.size());
		for (const auto &entry :
				static_cast<FSNodeDirectory*>(child)->entries) {
			subtasks.push_back(static_cast<HString>(entry.first));
		}
		auto task = new RemoveTask(std::move(subtasks),
					    child->id, context_);
		work_queue.push_front(*task);
		if (++repeat_counter_ >= kMaxRepeatCounter) {
			// something keeps adding files to a folder which is being deleted
			return LIZARDFS_ERROR_ENOTEMPTY;
		}
	} else {
		doUnlink(ts, wd, child);
		++gFsStatsArray[child->type == FSNode::kDirectory ?
		                FsStats::Rmdir : FsStats::Unlink];
		++current_subtask_;
		repeat_counter_ = 0;
	}
	return LIZARDFS_STATUS_OK;
}
