/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include "master/filesystem_node.h"

/*! \brief Implementation of snapshot engine.
 *
 * This class uses new approach to executing snapshots.
 * Each snapshot request is split into clone tasks.
 * Clone task is responsible for snapshoting only one inode.
 * In the case of cloning directory inode new clone tasks can be enqueued
 * with child inodes.
 *
 * Processing of enqueued tasks is done in batches (default 1000 inodes).
 * The batchProcess method should be executed by server frequently
 * so the all enqueued tasks can be done as soon as possible.
 */
class SnapshotManager {
public:
	struct SnapshotRequest {
		std::function<void(int)> finish_callback; /*!< Callback function called when the
		                                               snapshot request is done. */
		int enqueued_count; /*!< Number of unfinished clone jobs. */
	};

	typedef std::list<SnapshotRequest>::iterator RequestHandle;

	struct CloneData {
		uint32_t orig_inode;       /*!< First inode of snapshot request. */
		uint32_t src_inode;        /*!< Inode to be cloned. */
		uint32_t dst_parent_inode; /*!< Inode of clone parent. */
		uint32_t dst_inode;        /*!< Inode number of clone. If 0 means that
		                                inode number should be requested. */
		std::string dst_name;      /*!< Clone file name. */
		uint8_t can_overwrite;     /*!< Can cloning operation overwrite existing node. */
		bool emit_changelog;       /*!< If true change log message should be generated. */
		bool enqueue_work;         /*!< If true than new clone request should be created
		                                for source inode's children. */
		RequestHandle request;     /*!< Handle to snapshot request that this clone request
		                                belong to. */
	};

public:
	/*! \brief Constructor.
	 *
	 * \param default_batch_size Number of clone tasks processed in one batch.
	 */
	SnapshotManager(int default_batch_size = 1000);

	/*! \brief Make snapshot request.
	 *
	 * \param ts current time stamp.
	 * \param src_node pointer to fsnode that should be snapshotted.
	 * \param parent_node pointer to directory fsnode that is destination for snapshot.
	 * \param name source node clone name.
	 * \param can_overwrite if true then snapshot can overwrite existing files.
	 * \param callback function to be called on snapshot finish.
	 */
	int makeSnapshot(uint32_t ts, fsnode *src_node, fsnode *parent_node,
	                 const std::string &name, bool can_overwrite,
	                 const std::function<void(int)> &callback);

	/*! \brief Clone one fsnode.
	 *
	 * This function clones exactly one fsnode specified in CloneData structure.
	 * If the field enque_work is true then the function can generate new clone
	 * tasks.
	 *
	 * \param ts current time stamp.
	 * \param info information about clone information.
	 */
	int cloneNode(uint32_t ts, const CloneData &info);

	/*! \brief Process batch of clone tasks.
	 *
	 * This function clones exactly one fsnode specified in CloneData structure.
	 * If the field enqueue_work is true then the function can enqueue new clone
	 * tasks.
	 *
	 * The number of tasks to process is obtained from variable batch_size_.
	 *
	 * \param ts current time stamp.
	 * \param info information about clone information.
	 */
	void batchProcess(uint32_t ts);

	/*! \brief Check if there are clone tasks available for execution.
	 *
	 * \return true if there are enqueued clone tasks for execution.
	 */
	bool workAvailable() const;

protected:
	int cloneNodeTest(fsnode *src_node, fsnode *dst_parent, fsedge *dst_edge,
	                  const CloneData &info);
	fsnode *cloneToExistingNode(uint32_t ts, fsnode *src_node, fsnode *parent_node,
	                            fsedge *dst_edge, const CloneData &info);
	fsnode *cloneToNewNode(uint32_t ts, fsnode *src_node, fsnode *parent_node,
	                       const CloneData &info);
	fsnode *cloneToExistingFileNode(uint32_t ts, fsnode *src_node, fsnode *parent_node,
	                                fsnode *dst_node, fsedge *dst_edge, const CloneData &info);
	void cloneChunkData(fsnode *src_node, fsnode *dst_node, fsnode *parent_node);
	void cloneDirectoryData(fsnode *src_node, fsnode *dst_node, const CloneData &info);
	void cloneSymlinkData(fsnode *src_node, fsnode *dst_node, fsnode *parent_node);

	void emitChangelog(uint32_t ts, uint32_t dst_inode, const CloneData &info);

	void checkoutNode(std::list<CloneData>::iterator, int status);

protected:
	std::list<CloneData> work_queue_; /*!< Queue with clone task to execute. */
	std::list<SnapshotRequest> request_queue_; /*!< Queue with unfinished snapshot requests. */
	int batch_size_;
};
