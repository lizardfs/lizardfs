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

#include "common/main.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_quota.h"
#include "master/snapshot_task.h"
#include "master/task_manager.h"

static const int kInitialSnapshotTaskBatch = 1000;

uint8_t fs_snapshot(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
					const HString &name_dst, uint8_t can_overwrite,
			const std::function<void(int)> &callback) {
	ChecksumUpdater cu(context.ts());
	FSNode *src_node = nullptr;
	FSNode *dst_parent_node = nullptr;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent_dst, &dst_parent_node);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_R, inode_src,
	                                        &src_node);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (src_node->type == FSNode::kDirectory) {
		if (src_node == dst_parent_node ||
		    fsnodes_isancestor(static_cast<FSNodeDirectory *>(src_node), dst_parent_node)) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}

	assert(context.isPersonalityMaster());

	std::unique_ptr<SnapshotTask> task(new SnapshotTask(src_node->id, src_node->id,
	                                   static_cast<FSNodeDirectory *>(dst_parent_node)->id,
	                                   0, name_dst, can_overwrite, true, true));

	return gMetadata->task_manager.submitTask(context.ts(), kInitialSnapshotTaskBatch,
						  std::move(task), callback);
}

uint8_t fs_clone_node(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
			uint32_t inode_dst, const HString &name_dst, uint8_t can_overwrite) {

	SnapshotTask task(0, inode_src, parent_dst, inode_dst, name_dst, can_overwrite,
			  false, false);

	return task.cloneNode(context.ts());
}

uint8_t fsnodes_deprecated_snapshot_test(FSNode *origsrcnode, FSNode *srcnode,
		FSNodeDirectory *parentnode, const HString &name,
		uint8_t can_overwrite) {
	FSNode *dstnode;
	uint8_t status;
	if (fsnodes_quota_exceeded_ug(srcnode, {{QuotaResource::kInodes, 1}}) ||
	    fsnodes_quota_exceeded_dir(parentnode, {{QuotaResource::kInodes, 1}})) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if (srcnode->type == FSNode::kFile &&
	    (fsnodes_quota_exceeded_ug(srcnode, {{QuotaResource::kSize, 1}}) ||
	     fsnodes_quota_exceeded_dir(parentnode, {{QuotaResource::kSize, 1}}))) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if ((dstnode = fsnodes_lookup(parentnode, name))) {
		if (dstnode == origsrcnode) {
			return LIZARDFS_ERROR_EINVAL;
		}
		if (dstnode->type != srcnode->type) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (srcnode->type == FSNode::kTrash || srcnode->type == FSNode::kReserved) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (srcnode->type == FSNode::kDirectory) {
			for (const auto &entry : static_cast<const FSNodeDirectory *>(srcnode)->entries) {
				status = fsnodes_deprecated_snapshot_test(origsrcnode, entry.second,
				                                          static_cast<FSNodeDirectory *>(dstnode),
				                                          (HString)entry.first, can_overwrite);
				if (status != LIZARDFS_STATUS_OK) {
					return status;
				}
			}
		} else if (can_overwrite == 0) {
			return LIZARDFS_ERROR_EEXIST;
		}
	}
	return LIZARDFS_STATUS_OK;
}

/// creates cow copy
void fsnodes_deprecated_snapshot(uint32_t ts, FSNode *srcnode, FSNodeDirectory *parentnode,
		const HString &name) {
	FSNode *dstnode = nullptr;
	if ((dstnode = fsnodes_lookup(parentnode, name))) {
		// srcnode->type == dsntnode->type - guaranteed thx to check in snapshot_test
		// link already exists
		if (srcnode->type == FSNode::kDirectory) {
			for (const auto &entry : static_cast<const FSNodeDirectory *>(srcnode)->entries) {
				fsnodes_deprecated_snapshot(ts, entry.second,
				                            static_cast<FSNodeDirectory *>(dstnode),
				                            (HString)entry.first);
			}
		} else if (srcnode->type == FSNode::kFile) {
			FSNodeFile *src_file = static_cast<FSNodeFile *>(srcnode);
			FSNodeFile *dst_file = static_cast<FSNodeFile *>(dstnode);

			uint8_t same =
			    dst_file->length == src_file->length && dst_file->chunks == src_file->chunks;

			if (same == 0) {
				statsrecord psr, nsr;
				fsnodes_unlink(ts, static_cast<FSNodeDirectory *>(parentnode), name, dst_file);
				dstnode = fsnodes_create_node(ts, static_cast<FSNodeDirectory *>(parentnode), name,
				                              FSNode::kFile, srcnode->mode, 0, srcnode->uid,
				                              srcnode->gid, 0, AclInheritance::kDontInheritAcl);
				fsnodes_get_stats(dstnode, &psr);
				dstnode->goal = srcnode->goal;
				dstnode->trashtime = srcnode->trashtime;

				dst_file = static_cast<FSNodeFile *>(dstnode);
				dst_file->chunks = src_file->chunks;
				dst_file->length = src_file->length;

				for (uint32_t i = 0; i < src_file->chunks.size(); i++) {
					auto chunkid = src_file->chunks[i];
					if (chunkid > 0) {
						if (chunk_add_file(chunkid, dstnode->goal) != LIZARDFS_STATUS_OK) {
							syslog(LOG_ERR,
							       "structure error - chunk "
							       "%016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32
							       ")",
							       chunkid, srcnode->id, i);
						}
					}
				}

				fsnodes_get_stats(dstnode, &nsr);
				fsnodes_add_sub_stats(static_cast<FSNodeDirectory *>(parentnode), &nsr, &psr);
				fsnodes_quota_update(dstnode, {{QuotaResource::kSize, nsr.size - psr.size}});
			}
		} else if (srcnode->type == FSNode::kSymlink) {
			FSNodeSymlink *dst_symlink = static_cast<FSNodeSymlink *>(dstnode);
			FSNodeSymlink *src_symlink = static_cast<FSNodeSymlink *>(srcnode);

			if (dst_symlink->path_length != src_symlink->path_length) {
				statsrecord sr;
				memset(&sr, 0, sizeof(statsrecord));
				sr.length = dst_symlink->path_length - src_symlink->path_length;
				fsnodes_add_stats(static_cast<FSNodeDirectory *>(parentnode), &sr);
			}
			dst_symlink->path_length = src_symlink->path_length;
			dst_symlink->path = dst_symlink->path;
		} else if (srcnode->type == FSNode::kBlockDev || srcnode->type == FSNode::kCharDev) {
			static_cast<FSNodeDevice *>(dstnode)->rdev = static_cast<FSNodeDevice *>(srcnode)->rdev;
		}
		dstnode->mode = srcnode->mode;
		dstnode->uid = srcnode->uid;
		dstnode->gid = srcnode->gid;
		dstnode->atime = srcnode->atime;
		dstnode->mtime = srcnode->mtime;
		dstnode->ctime = ts;
	} else {
		if (srcnode->type == FSNode::kFile || srcnode->type == FSNode::kDirectory ||
		    srcnode->type == FSNode::kSymlink || srcnode->type == FSNode::kBlockDev ||
		    srcnode->type == FSNode::kCharDev || srcnode->type == FSNode::kSocket ||
		    srcnode->type == FSNode::kFifo) {
			statsrecord psr, nsr;
			dstnode = fsnodes_create_node(ts, static_cast<FSNodeDirectory *>(parentnode), name,
			                              srcnode->type, srcnode->mode, 0, srcnode->uid,
			                              srcnode->gid, 0, AclInheritance::kDontInheritAcl);
			fsnodes_get_stats(dstnode, &psr);
			dstnode->goal = srcnode->goal;
			dstnode->trashtime = srcnode->trashtime;
			dstnode->mode = srcnode->mode;
			dstnode->atime = srcnode->atime;
			dstnode->mtime = srcnode->mtime;
			if (srcnode->type == FSNode::kDirectory) {
				for (const auto &entry : static_cast<const FSNodeDirectory *>(srcnode)->entries) {
					fsnodes_deprecated_snapshot(ts, entry.second,
					                            static_cast<FSNodeDirectory *>(dstnode),
					                            (HString)entry.first);
				}
			} else if (srcnode->type == FSNode::kFile) {
				FSNodeFile *src_file = static_cast<FSNodeFile *>(srcnode);
				FSNodeFile *dst_file = static_cast<FSNodeFile *>(dstnode);

				dst_file->chunks = src_file->chunks;
				dst_file->length = src_file->length;
				for (uint32_t i = 0; i < src_file->chunks.size(); i++) {
					auto chunkid = src_file->chunks[i];
					if (chunkid > 0) {
						if (chunk_add_file(chunkid, dstnode->goal) != LIZARDFS_STATUS_OK) {
							syslog(LOG_ERR,
							       "structure error - chunk "
							       "%016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32
							       ")",
							       chunkid, srcnode->id, i);
						}
					}
				}

				fsnodes_get_stats(dstnode, &nsr);
				fsnodes_add_sub_stats(static_cast<FSNodeDirectory *>(parentnode), &nsr, &psr);
				fsnodes_quota_update(dstnode, {{QuotaResource::kSize, nsr.size - psr.size}});
			} else if (srcnode->type == FSNode::kSymlink) {
				FSNodeSymlink *dst_symlink = static_cast<FSNodeSymlink *>(dstnode);
				FSNodeSymlink *src_symlink = static_cast<FSNodeSymlink *>(srcnode);

				if (src_symlink->path_length > 0) {
					dst_symlink->path_length = src_symlink->path_length;
					dst_symlink->path = src_symlink->path;
				}
				fsnodes_get_stats(dstnode, &nsr);
				fsnodes_add_sub_stats(static_cast<FSNodeDirectory *>(parentnode), &nsr, &psr);
			} else if (srcnode->type == FSNode::kBlockDev || srcnode->type == FSNode::kCharDev) {
				static_cast<FSNodeDevice *>(dstnode)->rdev =
				    static_cast<FSNodeDevice *>(srcnode)->rdev;
			}
		}
	}
	fsnodes_update_checksum(dstnode);
}

uint8_t fs_deprecated_snapshot(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
			const HString &name_dst, uint8_t can_overwrite) {
	ChecksumUpdater cu(context.ts());
	FSNode *sp = NULL;
	FSNode *dwd = NULL;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent_dst, &dwd);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_R, inode_src,
	                                        &sp);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (sp->type == FSNode::kDirectory) {
		if (sp == dwd || fsnodes_isancestor(static_cast<FSNodeDirectory*>(sp), dwd)) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}
	status = fsnodes_deprecated_snapshot_test(sp, sp, static_cast<FSNodeDirectory *>(dwd), name_dst,
	                                          can_overwrite);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	fsnodes_deprecated_snapshot(context.ts(), sp, static_cast<FSNodeDirectory *>(dwd), name_dst);
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "SNAPSHOT(%" PRIu32 ",%" PRIu32 ",%s,%" PRIu8 ")", sp->id,
		             dwd->id, fsnodes_escape_name(name_dst).c_str(), can_overwrite);
	} else {
		gMetadata->metaversion++;
	}
	return LIZARDFS_STATUS_OK;
}
