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
	             fsnodes_escape_name(info.dst_name.size(),
	                                 reinterpret_cast<const uint8_t *>(info.dst_name.c_str())),
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

int SnapshotManager::makeSnapshot(uint32_t ts, fsnode *src_node, fsnode *parent_node,
				const std::string &name, bool can_overwrite,
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
int SnapshotManager::cloneNodeTest(fsnode *src_node, fsnode *dst_parent, fsedge *dst_edge, const CloneData &info) {
	if (fsnodes_quota_exceeded_ug(src_node, {{QuotaResource::kInodes, 1}}) ||
	    fsnodes_quota_exceeded_dir(dst_parent, {{QuotaResource::kInodes, 1}})) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if (src_node->type == TYPE_FILE &&
	    (fsnodes_quota_exceeded_ug(src_node, {{QuotaResource::kSize, 1}}) ||
	     fsnodes_quota_exceeded_dir(dst_parent, {{QuotaResource::kSize, 1}}))) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if (dst_edge) {
		fsnode *dst_node = dst_edge->child;
		if (info.orig_inode != 0 && dst_node->id == info.orig_inode) {
			return LIZARDFS_ERROR_EINVAL;
		}
		if (dst_node->type != src_node->type) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (src_node->type == TYPE_TRASH || src_node->type == TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (src_node->type != TYPE_DIRECTORY && !info.can_overwrite) {
			return LIZARDFS_ERROR_EEXIST;
		}
	}
	return LIZARDFS_STATUS_OK;
}

int SnapshotManager::cloneNode(uint32_t ts, const CloneData &info) {
	fsnode *src_node = fsnodes_id_to_node(info.src_inode);
	fsnode *dst_parent = fsnodes_id_to_node(info.dst_parent_inode);

	if (!src_node || !dst_parent) {
		return LIZARDFS_ERROR_EINVAL;
	}

	fsedge *dst_edge = fsnodes_lookup(dst_parent, info.dst_name.size(),
	                                  reinterpret_cast<const uint8_t *>(info.dst_name.c_str()));

	int status = cloneNodeTest(src_node, dst_parent, dst_edge, info);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}

	fsnode *dst_node;
	if (dst_edge) {
		dst_node = cloneToExistingNode(ts, src_node, dst_parent, dst_edge, info);
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

fsnode *SnapshotManager::cloneToExistingNode(uint32_t ts, fsnode *src_node, fsnode *dst_parent,
					fsedge *dst_edge, const CloneData &info) {
	fsnode *dst_node = dst_edge->child;

	if (dst_node->type != src_node->type) {
		return NULL;
	}

	switch (src_node->type) {
	case TYPE_DIRECTORY:
		cloneDirectoryData(src_node, dst_node, info);
		break;
	case TYPE_FILE:
		dst_node = cloneToExistingFileNode(ts, src_node, dst_parent, dst_node, dst_edge,
		                                      info);
		break;
	case TYPE_SYMLINK:
		cloneSymlinkData(src_node, dst_node, dst_parent);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		dst_node->data.devdata.rdev = src_node->data.devdata.rdev;
	}

	dst_node->mode = src_node->mode;
	dst_node->uid = src_node->uid;
	dst_node->gid = src_node->gid;
	dst_node->atime = src_node->atime;
	dst_node->mtime = src_node->mtime;
	dst_node->ctime = ts;

	return dst_node;
}

fsnode *SnapshotManager::cloneToNewNode(uint32_t ts, fsnode *src_node, fsnode *dst_parent,
					const CloneData &info) {
	if (!(src_node->type == TYPE_FILE || src_node->type == TYPE_DIRECTORY ||
	      src_node->type == TYPE_SYMLINK || src_node->type == TYPE_BLOCKDEV ||
	      src_node->type == TYPE_CHARDEV || src_node->type == TYPE_SOCKET ||
	      src_node->type == TYPE_FIFO)) {
		return NULL;
	}

	fsnode *dst_node = fsnodes_create_node(
	        ts, dst_parent, info.dst_name.size(), reinterpret_cast<const uint8_t *>(info.dst_name.c_str()),
	        src_node->type, src_node->mode, 0, src_node->uid, src_node->gid, 0,
	        AclInheritance::kDontInheritAcl, info.dst_inode);

	dst_node->goal = src_node->goal;
	dst_node->trashtime = src_node->trashtime;
	dst_node->mode = src_node->mode;
	dst_node->atime = src_node->atime;
	dst_node->mtime = src_node->mtime;

	switch (src_node->type) {
	case TYPE_DIRECTORY:
		cloneDirectoryData(src_node, dst_node, info);
		break;
	case TYPE_FILE:
		cloneChunkData(src_node, dst_node, dst_parent);
		break;
	case TYPE_SYMLINK:
		cloneSymlinkData(src_node, dst_node, dst_parent);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		dst_node->data.devdata.rdev = src_node->data.devdata.rdev;
	}

	return dst_node;
}

fsnode *SnapshotManager::cloneToExistingFileNode(uint32_t ts, fsnode *src_node, fsnode *dst_parent,
				fsnode *dst_node, fsedge *dst_edge,const CloneData &info) {
	bool same = false;

	if (dst_node->data.fdata.length == src_node->data.fdata.length &&
	    dst_node->data.fdata.chunks == src_node->data.fdata.chunks) {
		same = true;
		for (uint32_t i = 0; i < src_node->data.fdata.chunks && same; i++) {
			if (src_node->data.fdata.chunktab[i] != dst_node->data.fdata.chunktab[i]) {
				same = false;
			}
		}
	}

	if (same) {
		return dst_node;
	}

	fsnodes_unlink(ts, dst_edge);
	dst_node = fsnodes_create_node(ts, dst_parent, info.dst_name.size(),
	                               reinterpret_cast<const uint8_t *>(info.dst_name.c_str()), TYPE_FILE,
	                               src_node->mode, 0, src_node->uid, src_node->gid, 0,
	                               AclInheritance::kDontInheritAcl, info.dst_inode);

	cloneChunkData(src_node, dst_node, dst_parent);

	return dst_node;
}

void SnapshotManager::cloneChunkData(fsnode *src_node, fsnode *dst_node, fsnode *dst_parent) {
	statsrecord psr, nsr;

	fsnodes_get_stats(dst_node, &psr);

	dst_node->goal = src_node->goal;
	dst_node->trashtime = src_node->trashtime;
	if (src_node->data.fdata.chunks > 0) {
		dst_node->data.fdata.chunktab =
		        static_cast<uint64_t *>(malloc(sizeof(uint64_t) * (src_node->data.fdata.chunks)));
		passert(dst_node->data.fdata.chunktab);
		dst_node->data.fdata.chunks = src_node->data.fdata.chunks;
		for (uint32_t i = 0; i < src_node->data.fdata.chunks; i++) {
			uint64_t chunkid = src_node->data.fdata.chunktab[i];
			dst_node->data.fdata.chunktab[i] = chunkid;
			if (chunkid > 0) {
				if (chunk_add_file(chunkid, dst_node->goal) != LIZARDFS_STATUS_OK) {
					syslog(LOG_ERR, "structure error - chunk %016" PRIX64
					                " not found (inode: %" PRIu32
					                " ; index: %" PRIu32 ")",
					       chunkid, src_node->id, i);
				}
			}
		}
	} else {
		dst_node->data.fdata.chunktab = NULL;
		dst_node->data.fdata.chunks = 0;
	}
	dst_node->data.fdata.length = src_node->data.fdata.length;

	fsnodes_get_stats(dst_node, &nsr);
	fsnodes_add_sub_stats(dst_parent, &nsr, &psr);
	fsnodes_quota_update(dst_node, {{QuotaResource::kSize, nsr.size - psr.size}});
}

void SnapshotManager::cloneDirectoryData(fsnode *src_node, fsnode *dst_node,
					const CloneData &info) {
	if (!info.enqueue_work) {
		return;
	}
	for (fsedge *e = src_node->data.ddata.children; e; e = e->nextchild) {
		work_queue_.push_back({info.orig_inode, e->child->id, dst_node->id, 0,
		                       std::string(reinterpret_cast<const char *>(e->name),e->nleng), info.can_overwrite,
		                       info.emit_changelog, info.enqueue_work, info.request});
		info.request->enqueued_count++;
	}
}

void SnapshotManager::cloneSymlinkData(fsnode *src_node, fsnode *dst_node, fsnode *dst_parent) {
	statsrecord psr, nsr;

	fsnodes_get_stats(dst_node, &psr);

	if (dst_node->data.sdata.path) {
		free(dst_node->data.sdata.path);
	}

	if (src_node->data.sdata.pleng > 0) {
		dst_node->data.sdata.path = static_cast<uint8_t *>(malloc(src_node->data.sdata.pleng));
		passert(dst_node->data.sdata.path);
		memcpy(dst_node->data.sdata.path, src_node->data.sdata.path,
		       src_node->data.sdata.pleng);
		dst_node->data.sdata.pleng = src_node->data.sdata.pleng;
	} else {
		dst_node->data.sdata.path = NULL;
		dst_node->data.sdata.pleng = 0;
	}

	fsnodes_get_stats(dst_node, &nsr);
	fsnodes_add_sub_stats(dst_parent, &nsr, &psr);
}
