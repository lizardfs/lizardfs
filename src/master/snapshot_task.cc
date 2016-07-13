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

#include "master/snapshot_task.h"

#include "master/filesystem_checksum.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_quota.h"

int SnapshotTask::cloneNodeTest(FSNode *src_node, FSNode *dst_node, FSNodeDirectory *dst_parent) {
	if (fsnodes_quota_exceeded_ug(src_node, {{QuotaResource::kInodes, 1}}) ||
	    fsnodes_quota_exceeded_dir(dst_parent, {{QuotaResource::kInodes, 1}})) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if (src_node->type == FSNode::kFile &&
	    (fsnodes_quota_exceeded_ug(src_node, {{QuotaResource::kSize, 1}}) ||
	     fsnodes_quota_exceeded_dir(dst_parent, {{QuotaResource::kSize, 1}}))) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if (dst_node) {
		if (orig_inode_ != 0 && dst_node->id == orig_inode_) {
			return LIZARDFS_ERROR_EINVAL;
		}
		if (dst_node->type != src_node->type) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (src_node->type == FSNode::kTrash || src_node->type == FSNode::kReserved) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (src_node->type != FSNode::kDirectory && !can_overwrite_) {
			return LIZARDFS_ERROR_EEXIST;
		}
	}
	return LIZARDFS_STATUS_OK;
}

FSNode *SnapshotTask::cloneToExistingNode(uint32_t ts, FSNode *src_node,
					  FSNodeDirectory *dst_parent, FSNode *dst_node) {
	if (dst_node->type != src_node->type) {
		return NULL;
	}

	switch (src_node->type) {
	case FSNode::kDirectory:
		cloneDirectoryData(static_cast<const FSNodeDirectory *>(src_node),
		                   static_cast<FSNodeDirectory *>(dst_node));
		break;
	case FSNode::kFile:
		dst_node = cloneToExistingFileNode(ts, static_cast<FSNodeFile *>(src_node),
						   dst_parent, static_cast<FSNodeFile *>(dst_node));
		break;
	case FSNode::kSymlink:
		cloneSymlinkData(static_cast<FSNodeSymlink *>(src_node),
		                 static_cast<FSNodeSymlink *>(dst_node), dst_parent);
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
		static_cast<FSNodeDevice *>(dst_node)->rdev =
			static_cast<FSNodeDevice *>(src_node)->rdev;
	}

	dst_node->mode = src_node->mode;
	dst_node->uid = src_node->uid;
	dst_node->gid = src_node->gid;
	dst_node->atime = src_node->atime;
	dst_node->mtime = src_node->mtime;
	dst_node->ctime = ts;

	return dst_node;
}

FSNode *SnapshotTask::cloneToNewNode(uint32_t ts, FSNode *src_node, FSNodeDirectory *dst_parent) {
	if (!(src_node->type == FSNode::kFile || src_node->type == FSNode::kDirectory ||
	      src_node->type == FSNode::kSymlink || src_node->type == FSNode::kBlockDev ||
	      src_node->type == FSNode::kCharDev || src_node->type == FSNode::kSocket ||
	      src_node->type == FSNode::kFifo)) {
		return NULL;
	}

	FSNode *dst_node = fsnodes_create_node(ts, dst_parent, dst_name_, src_node->type,
	                                       src_node->mode, 0, src_node->uid, src_node->gid, 0,
	                                       AclInheritance::kDontInheritAcl, dst_inode_);

	dst_node->goal = src_node->goal;
	dst_node->trashtime = src_node->trashtime;
	dst_node->mode = src_node->mode;
	dst_node->atime = src_node->atime;
	dst_node->mtime = src_node->mtime;

	switch (src_node->type) {
	case FSNode::kDirectory:
		cloneDirectoryData(static_cast<const FSNodeDirectory *>(src_node),
		                   static_cast<FSNodeDirectory *>(dst_node));
		break;
	case FSNode::kFile:
		cloneChunkData(static_cast<FSNodeFile *>(src_node),
			       static_cast<FSNodeFile *>(dst_node), dst_parent);
		break;
	case FSNode::kSymlink:
		cloneSymlinkData(static_cast<FSNodeSymlink *>(src_node),
		                 static_cast<FSNodeSymlink *>(dst_node), dst_parent);
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
		static_cast<FSNodeDevice *>(dst_node)->rdev =
			static_cast<FSNodeDevice *>(src_node)->rdev;
	}

	return dst_node;
}

FSNodeFile *SnapshotTask::cloneToExistingFileNode(uint32_t ts, FSNodeFile *src_node,
				    FSNodeDirectory *dst_parent, FSNodeFile *dst_node) {
	bool same = dst_node->length == src_node->length && dst_node->chunks == src_node->chunks;

	if (same) {
		return dst_node;
	}

	fsnodes_unlink(ts, dst_parent, dst_name_, dst_node);
	dst_node = static_cast<FSNodeFile *>(fsnodes_create_node(
	    ts, dst_parent, dst_name_, FSNode::kFile, src_node->mode, 0, src_node->uid,
	    src_node->gid, 0, AclInheritance::kDontInheritAcl, dst_inode_));

	cloneChunkData(src_node, dst_node, dst_parent);

	return dst_node;
}

void SnapshotTask::cloneChunkData(const FSNodeFile *src_node, FSNodeFile *dst_node,
		    FSNodeDirectory *dst_parent) {
	statsrecord psr, nsr;

	fsnodes_get_stats(dst_node, &psr);

	dst_node->goal = src_node->goal;
	dst_node->trashtime = src_node->trashtime;
	dst_node->chunks = src_node->chunks;
	dst_node->length = src_node->length;
	for (uint32_t i = 0; i < src_node->chunks.size(); ++i) {
		auto chunkid = src_node->chunks[i];
		if (chunkid > 0) {
			if (chunk_add_file(chunkid, dst_node->goal) != LIZARDFS_STATUS_OK) {
				syslog(LOG_ERR, "structure error - chunk %016" PRIX64
				                " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",
				       chunkid, src_node->id, i);
			}
		}
	}

	fsnodes_get_stats(dst_node, &nsr);
	fsnodes_add_sub_stats(dst_parent, &nsr, &psr);
	fsnodes_quota_update(dst_node, {{QuotaResource::kSize, nsr.size - psr.size}});
}

void SnapshotTask::cloneDirectoryData(const FSNodeDirectory *src_node, FSNodeDirectory *dst_node) {
	if (!enqueue_work_) {
		return;
	}
	for(const auto &entry : src_node->entries) {
		std::unique_ptr<SnapshotTask> task(new SnapshotTask(orig_inode_, entry.second->id,
						   dst_node->id, 0, (HString)entry.first,
						   can_overwrite_, emit_changelog_, enqueue_work_));
		local_tasks_.push_back(std::move(task));
	}
}

void SnapshotTask::cloneSymlinkData(FSNodeSymlink *src_node, FSNodeSymlink *dst_node,
		      FSNodeDirectory *dst_parent) {
	statsrecord psr, nsr;

	fsnodes_get_stats(dst_node, &psr);

	dst_node->path = src_node->path;
	dst_node->path_length = src_node->path_length;

	fsnodes_get_stats(dst_node, &nsr);
	fsnodes_add_sub_stats(dst_parent, &nsr, &psr);
}

void SnapshotTask::emitChangelog(uint32_t ts, uint32_t dst_inode) {
	if (!emit_changelog_) {
		gMetadata->metaversion++;
		return;
	}

	fs_changelog(ts, "CLONE(%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%s,%" PRIu8 ")", src_inode_,
	             dst_parent_inode_, dst_inode,
	             fsnodes_escape_name(dst_name_).c_str(),
	             can_overwrite_);
}

int SnapshotTask::cloneNode(uint32_t ts) {
	FSNode *src_node = fsnodes_id_to_node(src_inode_);
	FSNodeDirectory *dst_parent = fsnodes_id_to_node<FSNodeDirectory>(dst_parent_inode_);

	if (!src_node || !dst_parent || dst_parent->type != FSNode::kDirectory) {
		return LIZARDFS_ERROR_EINVAL;
	}

	FSNode *dst_node = fsnodes_lookup(dst_parent, dst_name_);

	int status = cloneNodeTest(src_node, dst_node, dst_parent);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}

	if (dst_node) {
		dst_node = cloneToExistingNode(ts, src_node, dst_parent, dst_node);
	} else {
		dst_node = cloneToNewNode(ts, src_node, dst_parent);
	}

	if (dst_node) {
		fsnodes_update_checksum(dst_node);
		fsnodes_update_checksum(dst_parent);
		emitChangelog(ts, dst_node->id);
		if (dst_inode_ != 0 && dst_inode_ != dst_node->id) {
			return LIZARDFS_ERROR_MISMATCH;
		}
		return LIZARDFS_STATUS_OK;
	}

	return LIZARDFS_ERROR_EPERM;
}

int SnapshotTask::execute(uint32_t ts, std::list<std::unique_ptr<Task>> &work_queue) {
	int status = cloneNode(ts);
	if (status == LIZARDFS_STATUS_OK) {
		work_queue.splice(work_queue.end(), local_tasks_);
	}
	return status;
}
