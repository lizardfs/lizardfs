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

#include <cassert>
#include <functional>
#include <list>
#include <string>

#include "master/task_manager.h"
#include "master/filesystem_node.h"
#include "master/hstring.h"

/*! \brief Implementation of Snapshot Task to work with Task Manager.
 *
 * This class uses new approach to executing snapshots.
 * Each snapshot request is split into clone tasks.
 * Clone task is responsible for snapshoting only one inode.
 * In the case of cloning directory inode new clone tasks can be enqueued
 * with child inodes.
 *
 * Processing of enqueued tasks is done by Task Manager class.
 */
class SnapshotTask : public TaskManager::Task {
public:
	typedef std::vector<std::pair<uint32_t, HString>> SubtaskContainer;

	SnapshotTask(SubtaskContainer &&subtask, uint32_t orig_inode, uint32_t dst_parent_inode,
		     uint32_t dst_inode, uint8_t can_overwrite, uint8_t ignore_missing_src,
		     bool emit_changelog, bool enqueue_work) :
		     subtask_(std::move(subtask)), orig_inode_(orig_inode),
		     dst_parent_inode_(dst_parent_inode),dst_inode_(dst_inode),
		     can_overwrite_(can_overwrite), ignore_missing_src_(ignore_missing_src),
		     emit_changelog_(emit_changelog), enqueue_work_(enqueue_work), local_tasks_() {
		assert(subtask_.size() == 1 || (subtask_.size() > 1 && dst_inode == 0));
		current_subtask_ = subtask_.begin();
	}

	 /*! \brief Clone one fsnode.
	 *
	 * This function clones exactly one fsnode specified by this SnapshotTask object.
	 * If the field enque_work is true then the function can generate new clone
	 * tasks.
	 *
	 * \param ts current time stamp.
	 */
	int cloneNode(uint32_t ts);

	/*! \brief Execute task specified by this SnapshotTask object.
	 *
	 * This function overrides pure virtual execute function of TaskManager::Task.
	 * It is the only function to be called by Task Manager in order to
	 * execute enqueued task.
	 *
	 * \param ts current time stamp.
	 * \param work_queue a list to which this task adds newly created tasks.
	 */
	int execute(uint32_t ts, intrusive_list<Task> &work_queue) override;

	bool isFinished() const override {
		return current_subtask_ == subtask_.end();
	};

	static std::string generateDescription(const std::string &src, const std::string &dst) {
		return "Creating snapshot: " + src + " -> " + dst;
	}

protected:
	/*! \brief Test if node can be cloned. */
	int cloneNodeTest(FSNode *src_node, FSNode *dst_node, FSNodeDirectory *dst_parent);
	FSNode *cloneToExistingNode(uint32_t ts, FSNode *src_node, FSNodeDirectory *dst_parent,
				    FSNode *dst_node);
	FSNode *cloneToNewNode(uint32_t ts, FSNode *src_node, FSNodeDirectory *dst_parent);
	FSNodeFile *cloneToExistingFileNode(uint32_t ts, FSNodeFile *src_node,
	                                    FSNodeDirectory *dst_parent, FSNodeFile *dst_node);
	void cloneChunkData(const FSNodeFile *src_node, FSNodeFile *dst_node,
	                    FSNodeDirectory *dst_parent);
	void cloneDirectoryData(const FSNodeDirectory *src_node, FSNodeDirectory *dst_node);
	void cloneDirectoryData(FSNodeDirectory *src_node, FSNodeDirectory *dst_node);
	void cloneSymlinkData(FSNodeSymlink *src_node, FSNodeSymlink *dst_node,
	                      FSNodeDirectory *dst_parent);

	/*! \brief Emit metadata changelog.
	 *
	 * The function (for master) emits metadata CLONE information. For shadow it updates
	 * metadata version.
	 */
	void emitChangelog(uint32_t ts, uint32_t dst_inode);

private:
	SubtaskContainer subtask_; /*!< List of pairs (inode to be cloned, clone file name). */
	SubtaskContainer::iterator current_subtask_; /*!< Current subtask to execute. */

	uint32_t orig_inode_;       /*!< First inode of snapshot request. */
	uint32_t dst_parent_inode_; /*!< Inode of clone parent. */
	uint32_t dst_inode_;        /*!< Inode number of clone. If 0 means that
	                                 inode number should be requested. */
	uint8_t can_overwrite_;     /*!< Can cloning operation overwrite existing node. */
	uint8_t ignore_missing_src_;/*!< Continue execution of snapshot task despite encountering
	                                 missing files in source folder*/
	bool emit_changelog_;       /*!< If true change log message should be generated. */
	bool enqueue_work_;         /*!< If true then new clone request should be created
	                                 for source inode's children. */
	intrusive_list<Task> local_tasks_; /*< List of snapshot tasks created by this
	                                                   task for source inode's children. */
};
