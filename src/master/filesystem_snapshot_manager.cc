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

#include "common/platform.h"
#include "master/filesystem_snapshot_manager.h"

#include <cinttypes>

#include "master/chunks.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_node.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_quota.h"

SnapshotManager::SnapshotManager(int default_batch_size) : batch_size_(default_batch_size) {
}

/*! \brief Emit metadata changelog.
 *
 * The function (for master) emits metadata CLONE information. For shadow it updates
 * metadata version.
 */
void SnapshotManager::emitChangelog(uint32_t ts, uint32_t dst_inode, const CloneData &info) {
	if (!info.emit_changelog) {
		gMetadata->metaversion++;
		return;
	}

	fs_changelog(ts, "CLONE(%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%s,%" PRIu8 ")", info.src_inode,
	             info.dst_parent_inode, dst_inode,
	             fsnodes_escape_name(info.dst_name).c_str(),
	             info.can_overwrite);
}

/*! \brief The function finalizes processing of clone node request. */
void SnapshotManager::checkoutNode(std::list<CloneData>::iterator idata, int status) {
	RequestHandle request = idata->request;

	request->enqueued_count--;

	if (request->enqueued_count <= 0 || status) {
		if (request->finish_callback) {
			request->finish_callback(status);
		}

		if (status && request->enqueued_count > 1) {
			// if the clone has failed (status != 0) then
			// we need to remove all issued clone commands from the
			// snapshot request that this clone belongs to.
			std::list<CloneData>::iterator iclone = work_queue_.begin();
			while (iclone != work_queue_.end()) {
				if (iclone->request == request) {
					iclone = work_queue_.erase(iclone);
				} else {
					++iclone;
				}
			}
		} else {
			work_queue_.erase(idata);
		}

		request_queue_.erase(request);

		return;
	}

	work_queue_.erase(idata);
}

int SnapshotManager::makeSnapshot(uint32_t ts, FSNode *src_node, FSNodeDirectory *parent_node,
				const HString &name, bool can_overwrite,
				const std::function<void(int)> &callback) {
	std::list<CloneData> tmp;

	// swap work_queue_ with tmp to prioritize tasks from this snapshot request.
	std::swap(tmp, work_queue_);

	RequestHandle request =
	        request_queue_.insert(request_queue_.end(), {std::function<void(int)>(), 1});
	int done = 0;
	int status = LIZARDFS_STATUS_OK;

	request->finish_callback = [&done, &status](int code) {
		status = code;
		done = 1;
	};

	work_queue_.push_back({src_node->id, src_node->id, parent_node->id, 0, name, can_overwrite,
	                       true, true, request});
	batchProcess(ts);

	if (done) {
		assert(work_queue_.empty());
		work_queue_ = std::move(tmp); // revert to old task queue
		return status;
	}

	// attach new unfinished clone tasks at the and of work queue
	work_queue_.splice(work_queue_.begin(), tmp, tmp.begin(), tmp.end());

	request->finish_callback = callback;

	return LIZARDFS_ERROR_WAITING;
}

void SnapshotManager::batchProcess(uint32_t ts) {
	for (int i = 0; i < batch_size_; i++) {
		if (work_queue_.empty()) {
			break;
		}
		int status = cloneNode(ts, work_queue_.front());
		checkoutNode(work_queue_.begin(), status);
	}
}

bool SnapshotManager::workAvailable() const {
	return !work_queue_.empty();
}

/*! \brief Test if node can be cloned. */
int SnapshotManager::cloneNodeTest(FSNode *src_node, FSNode *dst_node, FSNodeDirectory* dst_parent,
		const CloneData &info) {
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
		if (info.orig_inode != 0 && dst_node->id == info.orig_inode) {
			return LIZARDFS_ERROR_EINVAL;
		}
		if (dst_node->type != src_node->type) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (src_node->type == FSNode::kTrash || src_node->type == FSNode::kReserved) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (src_node->type != FSNode::kDirectory && !info.can_overwrite) {
			return LIZARDFS_ERROR_EEXIST;
		}
	}
	return LIZARDFS_STATUS_OK;
}

int SnapshotManager::cloneNode(uint32_t ts, const CloneData &info) {
	FSNode *src_node = fsnodes_id_to_node(info.src_inode);
	FSNodeDirectory *dst_parent = fsnodes_id_to_node<FSNodeDirectory>(info.dst_parent_inode);

	if (!src_node || !dst_parent || dst_parent->type != FSNode::kDirectory) {
		return LIZARDFS_ERROR_EINVAL;
	}

	FSNode *dst_node = fsnodes_lookup(dst_parent, info.dst_name);

	int status = cloneNodeTest(src_node, dst_node, dst_parent, info);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}

	if (dst_node) {
		dst_node = cloneToExistingNode(ts, src_node, dst_parent, dst_node, info);
	} else {
		dst_node = cloneToNewNode(ts, src_node, dst_parent, info);
	}

	if (dst_node) {
		fsnodes_update_checksum(dst_node);
		fsnodes_update_checksum(dst_parent);
		emitChangelog(ts, dst_node->id, info);
		if (info.dst_inode != 0 && info.dst_inode != dst_node->id) {
			return LIZARDFS_ERROR_MISMATCH;
		}
		return LIZARDFS_STATUS_OK;
	}

	return LIZARDFS_ERROR_EPERM;
}

FSNode *SnapshotManager::cloneToExistingNode(uint32_t ts, FSNode *src_node, FSNodeDirectory *dst_parent,
					FSNode *dst_node, const CloneData &info) {
	if (dst_node->type != src_node->type) {
		return NULL;
	}

	switch (src_node->type) {
	case FSNode::kDirectory:
		cloneDirectoryData(static_cast<const FSNodeDirectory *>(src_node),
		                   static_cast<FSNodeDirectory *>(dst_node), info);
		break;
	case FSNode::kFile:
		dst_node = cloneToExistingFileNode(ts, static_cast<FSNodeFile *>(src_node), dst_parent,
		                                   static_cast<FSNodeFile *>(dst_node), info);
		break;
	case FSNode::kSymlink:
		cloneSymlinkData(static_cast<FSNodeSymlink *>(src_node),
		                 static_cast<FSNodeSymlink *>(dst_node), dst_parent);
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
		static_cast<FSNodeDevice *>(dst_node)->rdev = static_cast<FSNodeDevice *>(src_node)->rdev;
	}

	dst_node->mode = src_node->mode;
	dst_node->uid = src_node->uid;
	dst_node->gid = src_node->gid;
	dst_node->atime = src_node->atime;
	dst_node->mtime = src_node->mtime;
	dst_node->ctime = ts;

	return dst_node;
}

FSNode *SnapshotManager::cloneToNewNode(uint32_t ts, FSNode *src_node, FSNodeDirectory *dst_parent,
					const CloneData &info) {
	if (!(src_node->type == FSNode::kFile || src_node->type == FSNode::kDirectory ||
	      src_node->type == FSNode::kSymlink || src_node->type == FSNode::kBlockDev ||
	      src_node->type == FSNode::kCharDev || src_node->type == FSNode::kSocket ||
	      src_node->type == FSNode::kFifo)) {
		return NULL;
	}

	FSNode *dst_node = fsnodes_create_node(ts, dst_parent, info.dst_name, src_node->type,
	                                       src_node->mode, 0, src_node->uid, src_node->gid, 0,
	                                       AclInheritance::kDontInheritAcl, info.dst_inode);

	dst_node->goal = src_node->goal;
	dst_node->trashtime = src_node->trashtime;
	dst_node->mode = src_node->mode;
	dst_node->atime = src_node->atime;
	dst_node->mtime = src_node->mtime;

	switch (src_node->type) {
	case FSNode::kDirectory:
		cloneDirectoryData(static_cast<const FSNodeDirectory *>(src_node),
		                   static_cast<FSNodeDirectory *>(dst_node), info);
		break;
	case FSNode::kFile:
		cloneChunkData(static_cast<FSNodeFile *>(src_node), static_cast<FSNodeFile *>(dst_node),
		               dst_parent);
		break;
	case FSNode::kSymlink:
		cloneSymlinkData(static_cast<FSNodeSymlink *>(src_node),
		                 static_cast<FSNodeSymlink *>(dst_node), dst_parent);
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
		static_cast<FSNodeDevice *>(dst_node)->rdev = static_cast<FSNodeDevice *>(src_node)->rdev;
	}

	return dst_node;
}

FSNodeFile *SnapshotManager::cloneToExistingFileNode(uint32_t ts, FSNodeFile *src_node,
		FSNodeDirectory *dst_parent, FSNodeFile *dst_node, const CloneData &info) {
	bool same = dst_node->length == src_node->length && dst_node->chunks == src_node->chunks;

	if (same) {
		return dst_node;
	}

	fsnodes_unlink(ts, dst_parent, info.dst_name, dst_node);
	dst_node = static_cast<FSNodeFile *>(fsnodes_create_node(
	    ts, dst_parent, info.dst_name, FSNode::kFile, src_node->mode, 0, src_node->uid,
	    src_node->gid, 0, AclInheritance::kDontInheritAcl, info.dst_inode));

	cloneChunkData(src_node, dst_node, dst_parent);

	return dst_node;
}

void SnapshotManager::cloneChunkData(const FSNodeFile *src_node, FSNodeFile *dst_node,
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
				syslog(LOG_ERR, "structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32
				                " ; index: %" PRIu32 ")",
				       chunkid, src_node->id, i);
			}
		}
	}

	fsnodes_get_stats(dst_node, &nsr);
	fsnodes_add_sub_stats(dst_parent, &nsr, &psr);
	fsnodes_quota_update(dst_node, {{QuotaResource::kSize, nsr.size - psr.size}});
}

void SnapshotManager::cloneDirectoryData(const FSNodeDirectory *src_node, FSNodeDirectory *dst_node,
					const CloneData &info) {
	if (!info.enqueue_work) {
		return;
	}
	for(const auto &entry : src_node->entries) {
		work_queue_.push_back({info.orig_inode, entry.second->id, dst_node->id, 0,
		                       (HString)entry.first, info.can_overwrite,
		                       info.emit_changelog, info.enqueue_work, info.request});
		info.request->enqueued_count++;
	}
}

void SnapshotManager::cloneSymlinkData(FSNodeSymlink *src_node, FSNodeSymlink *dst_node,
		FSNodeDirectory *dst_parent) {
	statsrecord psr, nsr;

	fsnodes_get_stats(dst_node, &psr);

	dst_node->path = src_node->path;
	dst_node->path_length = src_node->path_length;

	fsnodes_get_stats(dst_node, &nsr);
	fsnodes_add_sub_stats(dst_parent, &nsr, &psr);
}
