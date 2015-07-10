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

uint8_t fs_snapshot(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
			uint16_t nleng_dst, const uint8_t *name_dst, uint8_t can_overwrite,
			const std::function<void(int)> &callback) {
	ChecksumUpdater cu(context.ts());
	fsnode *src_node = NULL;
	fsnode *dst_parent_node = NULL;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent_dst, &dst_parent_node);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_R,
	                                        inode_src, &src_node);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (src_node->type == TYPE_DIRECTORY) {
		if (src_node == dst_parent_node || fsnodes_isancestor(src_node, dst_parent_node)) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}

	assert(context.isPersonalityMaster());

	return gMetadata->snapshot_manager.makeSnapshot(context.ts(), src_node, dst_parent_node,
	                                         std::string((const char *)name_dst, nleng_dst),
	                                         can_overwrite, callback);
}

uint8_t fs_clone_node(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
			uint32_t inode_dst, uint16_t nleng_dst, const uint8_t *name_dst,
			uint8_t can_overwrite) {
	SnapshotManager::CloneData data;

	data.orig_inode = 0;
	data.src_inode = inode_src;
	data.dst_parent_inode = parent_dst;
	data.dst_inode = inode_dst;
	data.dst_name = std::string(reinterpret_cast<const char *>(name_dst), nleng_dst);
	data.can_overwrite = can_overwrite;
	data.emit_changelog = false;
	data.enqueue_work = false;

	return gMetadata->snapshot_manager.cloneNode(context.ts(), data);
}

#ifndef METARESTORE

void fs_background_snapshot_work() {
	if (gMetadata->snapshot_manager.workAvailable()) {
		uint32_t ts = main_time();
		ChecksumUpdater cu(ts);
		gMetadata->snapshot_manager.batchProcess(ts);
		if (gMetadata->snapshot_manager.workAvailable()) {
			main_make_next_poll_nonblocking();
		}
	}
}

#endif

uint8_t fsnodes_deprecated_snapshot_test(fsnode *origsrcnode, fsnode *srcnode, fsnode *parentnode,
			uint32_t nleng, const uint8_t *name, uint8_t can_overwrite) {
	fsedge *e;
	fsnode *dstnode;
	uint8_t status;
	if (fsnodes_inode_quota_exceeded(srcnode->uid, srcnode->gid)) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if (srcnode->type == TYPE_FILE && fsnodes_size_quota_exceeded(srcnode->uid, srcnode->gid)) {
		return LIZARDFS_ERROR_QUOTA;
	}
	if ((e = fsnodes_lookup(parentnode, nleng, name))) {
		dstnode = e->child;
		if (dstnode == origsrcnode) {
			return LIZARDFS_ERROR_EINVAL;
		}
		if (dstnode->type != srcnode->type) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (srcnode->type == TYPE_TRASH || srcnode->type == TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (srcnode->type == TYPE_DIRECTORY) {
			for (e = srcnode->data.ddata.children; e; e = e->nextchild) {
				status = fsnodes_deprecated_snapshot_test(origsrcnode, e->child, dstnode,
				                               e->nleng, e->name, can_overwrite);
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
void fsnodes_deprecated_snapshot(uint32_t ts, fsnode *srcnode, fsnode *parentnode, uint32_t nleng,
				const uint8_t *name) {
	fsedge *e;
	fsnode *dstnode = nullptr;
	uint32_t i;
	uint64_t chunkid;
	if ((e = fsnodes_lookup(parentnode, nleng, name))) {
		// link already exists
		dstnode = e->child;
		if (srcnode->type == TYPE_DIRECTORY) {
			for (e = srcnode->data.ddata.children; e; e = e->nextchild) {
				fsnodes_deprecated_snapshot(ts, e->child, dstnode, e->nleng,
				                            e->name);
			}
		} else if (srcnode->type == TYPE_FILE) {
			uint8_t same = 0;
			if (dstnode->data.fdata.length == srcnode->data.fdata.length &&
			    dstnode->data.fdata.chunks == srcnode->data.fdata.chunks) {
				same = 1;
				for (i = 0; i < srcnode->data.fdata.chunks && same; i++) {
					if (srcnode->data.fdata.chunktab[i] !=
					    dstnode->data.fdata.chunktab[i]) {
						same = 0;
					}
				}
			}
			if (same == 0) {
				statsrecord psr, nsr;
				fsnodes_unlink(ts, e);
				dstnode = fsnodes_create_node(ts, parentnode, nleng, name,
				                              TYPE_FILE, srcnode->mode, 0,
				                              srcnode->uid, srcnode->gid, 0,
				                              AclInheritance::kDontInheritAcl);
				fsnodes_get_stats(dstnode, &psr);
				dstnode->goal = srcnode->goal;
				dstnode->trashtime = srcnode->trashtime;
				if (srcnode->data.fdata.chunks > 0) {
					dstnode->data.fdata.chunktab = (uint64_t *)malloc(
					        sizeof(uint64_t) * (srcnode->data.fdata.chunks));
					passert(dstnode->data.fdata.chunktab);
					dstnode->data.fdata.chunks = srcnode->data.fdata.chunks;
					for (i = 0; i < srcnode->data.fdata.chunks; i++) {
						chunkid = srcnode->data.fdata.chunktab[i];
						dstnode->data.fdata.chunktab[i] = chunkid;
						if (chunkid > 0) {
							if (chunk_add_file(chunkid,
							                   dstnode->goal) !=
							    LIZARDFS_STATUS_OK) {
								syslog(LOG_ERR,
								       "structure error - chunk "
								       "%016" PRIX64
								       " not found (inode: %" PRIu32
								       " ; index: %" PRIu32 ")",
								       chunkid, srcnode->id, i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
				fsnodes_get_stats(dstnode, &nsr);
				fsnodes_add_sub_stats(parentnode, &nsr, &psr);
				fsnodes_quota_update_size(dstnode, nsr.size - psr.size);
			}
		} else if (srcnode->type == TYPE_SYMLINK) {
			if (dstnode->data.sdata.pleng != srcnode->data.sdata.pleng) {
				statsrecord sr;
				memset(&sr, 0, sizeof(statsrecord));
				sr.length = dstnode->data.sdata.pleng - srcnode->data.sdata.pleng;
				fsnodes_add_stats(parentnode, &sr);
			}
			if (dstnode->data.sdata.path) {
				free(dstnode->data.sdata.path);
			}
			if (srcnode->data.sdata.pleng > 0) {
				dstnode->data.sdata.path =
				        (uint8_t *)malloc(srcnode->data.sdata.pleng);
				passert(dstnode->data.sdata.path);
				memcpy(dstnode->data.sdata.path, srcnode->data.sdata.path,
				       srcnode->data.sdata.pleng);
				dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
			} else {
				dstnode->data.sdata.path = NULL;
				dstnode->data.sdata.pleng = 0;
			}
		} else if (srcnode->type == TYPE_BLOCKDEV || srcnode->type == TYPE_CHARDEV) {
			dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
		}
		dstnode->mode = srcnode->mode;
		dstnode->uid = srcnode->uid;
		dstnode->gid = srcnode->gid;
		dstnode->atime = srcnode->atime;
		dstnode->mtime = srcnode->mtime;
		dstnode->ctime = ts;
	} else {
		if (srcnode->type == TYPE_FILE || srcnode->type == TYPE_DIRECTORY ||
		    srcnode->type == TYPE_SYMLINK || srcnode->type == TYPE_BLOCKDEV ||
		    srcnode->type == TYPE_CHARDEV || srcnode->type == TYPE_SOCKET ||
		    srcnode->type == TYPE_FIFO) {
			statsrecord psr, nsr;
			dstnode = fsnodes_create_node(ts, parentnode, nleng, name, srcnode->type,
			                              srcnode->mode, 0, srcnode->uid, srcnode->gid,
			                              0, AclInheritance::kDontInheritAcl);
			fsnodes_get_stats(dstnode, &psr);
			dstnode->goal = srcnode->goal;
			dstnode->trashtime = srcnode->trashtime;
			dstnode->mode = srcnode->mode;
			dstnode->atime = srcnode->atime;
			dstnode->mtime = srcnode->mtime;
			if (srcnode->type == TYPE_DIRECTORY) {
				for (e = srcnode->data.ddata.children; e; e = e->nextchild) {
					fsnodes_deprecated_snapshot(ts, e->child, dstnode, e->nleng,
					                            e->name);
				}
			} else if (srcnode->type == TYPE_FILE) {
				if (srcnode->data.fdata.chunks > 0) {
					dstnode->data.fdata.chunktab = (uint64_t *)malloc(
					        sizeof(uint64_t) * (srcnode->data.fdata.chunks));
					passert(dstnode->data.fdata.chunktab);
					dstnode->data.fdata.chunks = srcnode->data.fdata.chunks;
					for (i = 0; i < srcnode->data.fdata.chunks; i++) {
						chunkid = srcnode->data.fdata.chunktab[i];
						dstnode->data.fdata.chunktab[i] = chunkid;
						if (chunkid > 0) {
							if (chunk_add_file(chunkid,
							                   dstnode->goal) !=
							    LIZARDFS_STATUS_OK) {
								syslog(LOG_ERR,
								       "structure error - chunk "
								       "%016" PRIX64
								       " not found (inode: %" PRIu32
								       " ; index: %" PRIu32 ")",
								       chunkid, srcnode->id, i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
				fsnodes_get_stats(dstnode, &nsr);
				fsnodes_add_sub_stats(parentnode, &nsr, &psr);
				fsnodes_quota_update_size(dstnode, nsr.size - psr.size);
			} else if (srcnode->type == TYPE_SYMLINK) {
				if (srcnode->data.sdata.pleng > 0) {
					dstnode->data.sdata.path =
					        (uint8_t *)malloc(srcnode->data.sdata.pleng);
					passert(dstnode->data.sdata.path);
					memcpy(dstnode->data.sdata.path, srcnode->data.sdata.path,
					       srcnode->data.sdata.pleng);
					dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
				}
				fsnodes_get_stats(dstnode, &nsr);
				fsnodes_add_sub_stats(parentnode, &nsr, &psr);
			} else if (srcnode->type == TYPE_BLOCKDEV ||
			           srcnode->type == TYPE_CHARDEV) {
				dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
			}
		}
	}
	fsnodes_update_checksum(dstnode);
}

uint8_t fs_deprecated_snapshot(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
			uint16_t nleng_dst, const uint8_t *name_dst, uint8_t can_overwrite) {
	ChecksumUpdater cu(context.ts());
	fsnode *sp = NULL;
	fsnode *dwd = NULL;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent_dst, &dwd);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_R,
	                                        inode_src, &sp);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (sp->type == TYPE_DIRECTORY) {
		if (sp == dwd || fsnodes_isancestor(sp, dwd)) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}
	status = fsnodes_deprecated_snapshot_test(sp, sp, dwd, nleng_dst, name_dst, can_overwrite);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	fsnodes_deprecated_snapshot(context.ts(), sp, dwd, nleng_dst, name_dst);
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "SNAPSHOT(%" PRIu32 ",%" PRIu32 ",%s,%" PRIu8 ")",
		             sp->id, dwd->id, fsnodes_escape_name(nleng_dst, name_dst),
		             can_overwrite);
	} else {
		gMetadata->metaversion++;
	}
	return LIZARDFS_STATUS_OK;
}
