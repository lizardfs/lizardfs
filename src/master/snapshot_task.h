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
	SnapshotTask(uint32_t orig_inode, uint32_t src_inode, uint32_t dst_parent_inode,
		     uint32_t dst_inode, const HString &dst_name, uint8_t can_overwrite,
		     bool emit_changelog, bool enqueue_work) :
		     orig_inode_(orig_inode), src_inode_(src_inode),
		     dst_parent_inode_(dst_parent_inode),dst_inode_(dst_inode),
		     dst_name_(dst_name), can_overwrite_(can_overwrite),
		     emit_changelog_(emit_changelog), enqueue_work_(enqueue_work),
		     local_tasks_() {
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
	int execute(uint32_t ts, std::list<std::unique_ptr<Task>> &work_queue);

	bool isFinished() const {
		return true;
	};

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
	uint32_t orig_inode_;          /*!< First inode of snapshot request. */
	uint32_t src_inode_;           /*!< Inode to be cloned. */
	uint32_t dst_parent_inode_;    /*!< Inode of clone parent. */
	uint32_t dst_inode_;           /*!< Inode number of clone. If 0 means that
					    inode number should be requested. */
	HString dst_name_;             /*!< Clone file name. */
	uint8_t can_overwrite_;        /*!< Can cloning operation overwrite existing node. */
	bool emit_changelog_;          /*!< If true change log message should be generated. */
	bool enqueue_work_;            /*!< If true then new clone request should be created
					    for source inode's children. */
	std::list<std::unique_ptr<Task>> local_tasks_; /*< List of snapshot tasks created by this
							   task for source inode's children. */
};
