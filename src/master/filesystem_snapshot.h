/*
   Copyright 2015-2017 Skytechnology sp. z o.o.

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

#include "master/filesystem.h"
#include "master/fs_context.h"

void fs_read_snapshot_config_file();

/*! \brief Register snapshot task.
 *
 * \param context server context.
 * \param inode_src number of inode to clone.
 * \param parent_dst number of inode of the directory where source inode should be cloned to.
 * \param name_dst clone name.
 * \param can_overwrite if true then cloning process can overwrite existing nodes.
 * \param callback function that should be executed on finish of snapshot task.
 * \param job_id desired id for this snapshot request.
 */
uint8_t fs_snapshot(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
		const HString &name_dst, uint8_t can_overwrite, uint8_t ignore_missing_src, uint32_t initial_batch_size,
		const std::function<void(int)> &callback, uint32_t job_id);

/*! \brief Clone one inode.
 *
 * \param context server context.
 * \param inode_src number of inode to clone.
 * \param parent_dst number of inode of the directory where source inode should be cloned to.
 * \param inode_dst inode number that should be used for clone's inode.
 * \param name_dst clone name.
 * \param can_overwrite if true then cloning process can overwrite existing nodes.
 */
uint8_t fs_clone_node(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
		uint32_t inode_dst, const HString &name_dst,
		uint8_t can_overwrite);
