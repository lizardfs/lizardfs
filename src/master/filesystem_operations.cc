/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015
   Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "master/filesystem_operations.h"

#include <cstdarg>
#include <cstdint>

#include "common/attributes.h"
#include "common/main.h"
#include "master/changelog.h"
#include "master/filesystem.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_node.h"
#include "master/fs_context.h"
#include "master/matocsserv.h"
#include "master/matoclserv.h"
#include "master/matomlserv.h"

static uint32_t stats_statfs = 0;
static uint32_t stats_getattr = 0;
static uint32_t stats_setattr = 0;
static uint32_t stats_lookup = 0;
static uint32_t stats_mkdir = 0;
static uint32_t stats_rmdir = 0;
static uint32_t stats_symlink = 0;
static uint32_t stats_readlink = 0;
static uint32_t stats_mknod = 0;
static uint32_t stats_unlink = 0;
static uint32_t stats_rename = 0;
static uint32_t stats_link = 0;
static uint32_t stats_readdir = 0;
static uint32_t stats_open = 0;
static uint32_t stats_read = 0;
static uint32_t stats_write = 0;

template <class T>
bool decodeChar(const char *keys, const std::vector<T> values, char key, T &value) {
	const uint32_t count = strlen(keys);
	sassert(values.size() == count);
	for (uint32_t i = 0; i < count; i++) {
		if (key == keys[i]) {
			value = values[i];
			return true;
		}
	}
	return false;
}

void fs_stats(uint32_t stats[16]) {
	stats[0] = stats_statfs;
	stats[1] = stats_getattr;
	stats[2] = stats_setattr;
	stats[3] = stats_lookup;
	stats[4] = stats_mkdir;
	stats[5] = stats_rmdir;
	stats[6] = stats_symlink;
	stats[7] = stats_readlink;
	stats[8] = stats_mknod;
	stats[9] = stats_unlink;
	stats[10] = stats_rename;
	stats[11] = stats_link;
	stats[12] = stats_readdir;
	stats[13] = stats_open;
	stats[14] = stats_read;
	stats[15] = stats_write;
	stats_statfs = 0;
	stats_getattr = 0;
	stats_setattr = 0;
	stats_lookup = 0;
	stats_mkdir = 0;
	stats_rmdir = 0;
	stats_symlink = 0;
	stats_readlink = 0;
	stats_mknod = 0;
	stats_unlink = 0;
	stats_rename = 0;
	stats_link = 0;
	stats_readdir = 0;
	stats_open = 0;
	stats_read = 0;
	stats_write = 0;
}

void fs_changelog(uint32_t ts, const char *format, ...) {
#ifdef METARESTORE
	(void)ts;
	(void)format;
#else
	const uint32_t kMaxTimestampSize = 20;
	const uint32_t kMaxEntrySize = kMaxLogLineSize - kMaxTimestampSize;
	static char entry[kMaxLogLineSize];

	// First, put "<timestamp>|" in the buffer
	int tsLength = snprintf(entry, kMaxTimestampSize, "%" PRIu32 "|", ts);

	// Then append the entry to the buffer
	va_list ap;
	uint32_t entryLength;
	va_start(ap, format);
	entryLength = vsnprintf(entry + tsLength, kMaxEntrySize, format, ap);
	va_end(ap);

	if (entryLength >= kMaxEntrySize) {
		entry[tsLength + kMaxEntrySize - 1] = '\0';
		entryLength = kMaxEntrySize;
	} else {
		entryLength++;
	}

	uint64_t version = gMetadata->metaversion++;
	changelog(version, entry);
	matomlserv_broadcast_logstring(version, (uint8_t *)entry, tsLength + entryLength);
#endif
}

#ifndef METARESTORE
uint8_t fs_readreserved_size(uint32_t rootinode, uint8_t sesflags, uint32_t *dbuffsize) {
	if (rootinode != 0) {
		return LIZARDFS_ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(gMetadata->reserved);
	return LIZARDFS_STATUS_OK;
}

void fs_readreserved_data(uint32_t rootinode, uint8_t sesflags, uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(gMetadata->reserved, dbuff);
}

uint8_t fs_readtrash_size(uint32_t rootinode, uint8_t sesflags, uint32_t *dbuffsize) {
	if (rootinode != 0) {
		return LIZARDFS_ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(gMetadata->trash);
	return LIZARDFS_STATUS_OK;
}

void fs_readtrash_data(uint32_t rootinode, uint8_t sesflags, uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(gMetadata->trash, dbuff);
}

/* common procedure for trash and reserved files */
uint8_t fs_getdetachedattr(uint32_t rootinode, uint8_t sesflags, uint32_t inode, Attributes &attr,
				uint8_t dtype) {
	fsnode *p;
	memset(attr, 0, 35);
	if (rootinode != 0) {
		return LIZARDFS_ERROR_EPERM;
	}
	(void)sesflags;
	if (!DTYPE_ISVALID(dtype)) {
		return LIZARDFS_ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (dtype == DTYPE_TRASH && p->type == TYPE_RESERVED) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (dtype == DTYPE_RESERVED && p->type == TYPE_TRASH) {
		return LIZARDFS_ERROR_ENOENT;
	}
	fsnodes_fill_attr(p, NULL, p->uid, p->gid, p->uid, p->gid, sesflags, attr);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_gettrashpath(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t *pleng,
			uint8_t **path) {
	fsnode *p;
	*pleng = 0;
	*path = NULL;
	if (rootinode != 0) {
		return LIZARDFS_ERROR_EPERM;
	}
	(void)sesflags;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_TRASH) {
		return LIZARDFS_ERROR_ENOENT;
	}
	*pleng = p->parents->nleng;
	*path = p->parents->name;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_settrashpath(const FsContext &context, uint32_t inode, uint32_t pleng,
			const uint8_t *path) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kOnlyMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	} else if (p->type != TYPE_TRASH) {
		return LIZARDFS_ERROR_ENOENT;
	} else if (pleng == 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	for (uint32_t i = 0; i < pleng; i++) {
		if (path[i] == 0) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}

	uint8_t *newpath = (uint8_t *)malloc(pleng);
	passert(newpath);
	free(p->parents->name);
	memcpy(newpath, path, pleng);
	p->parents->name = newpath;
	p->parents->nleng = pleng;
	fsedges_update_checksum(p->parents);
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "SETPATH(%" PRIu32 ",%s)", p->id,
		             fsnodes_escape_name(pleng, newpath));
	} else {
		gMetadata->metaversion++;
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_undel(const FsContext &context, uint32_t inode) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kOnlyMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	} else if (p->type != TYPE_TRASH) {
		return LIZARDFS_ERROR_ENOENT;
	}

	status = fsnodes_undel(context.ts(), p);
	if (context.isPersonalityMaster()) {
		if (status == LIZARDFS_STATUS_OK) {
			fs_changelog(context.ts(), "UNDEL(%" PRIu32 ")", p->id);
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

uint8_t fs_purge(const FsContext &context, uint32_t inode) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kOnlyMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	} else if (p->type != TYPE_TRASH) {
		return LIZARDFS_ERROR_ENOENT;
	}
	uint32_t purged_inode =
	        p->id;  // This should be equal to inode, because p is not a directory
	fsnodes_purge(context.ts(), p);

	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "PURGE(%" PRIu32 ")", purged_inode);
	} else {
		gMetadata->metaversion++;
	}
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
void fs_info(uint64_t *totalspace, uint64_t *availspace, uint64_t *trspace, uint32_t *trnodes,
		uint64_t *respace, uint32_t *renodes, uint32_t *inodes, uint32_t *dnodes,
		uint32_t *fnodes) {
	matocsserv_getspace(totalspace, availspace);
	*trspace = gMetadata->trashspace;
	*trnodes = gMetadata->trashnodes;
	*respace = gMetadata->reservedspace;
	*renodes = gMetadata->reservednodes;
	*inodes = gMetadata->nodes;
	*dnodes = gMetadata->dirnodes;
	*fnodes = gMetadata->filenodes;
}

uint8_t fs_getrootinode(uint32_t *rootinode, const uint8_t *path) {
	uint32_t nleng;
	const uint8_t *name;
	fsnode *p;
	fsedge *e;

	name = path;
	p = gMetadata->root;
	for (;;) {
		while (*name == '/') {
			name++;
		}
		if (*name == '\0') {
			*rootinode = p->id;
			return LIZARDFS_STATUS_OK;
		}
		nleng = 0;
		while (name[nleng] && name[nleng] != '/') {
			nleng++;
		}
		if (fsnodes_namecheck(nleng, name) < 0) {
			return LIZARDFS_ERROR_EINVAL;
		}
		e = fsnodes_lookup(p, nleng, name);
		if (!e) {
			return LIZARDFS_ERROR_ENOENT;
		}
		p = e->child;
		if (p->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOTDIR;
		}
		name += nleng;
	}
}

void fs_statfs(uint32_t rootinode, uint8_t sesflags, uint64_t *totalspace, uint64_t *availspace,
		uint64_t *trspace, uint64_t *respace, uint32_t *inodes) {
	fsnode *rn;
	statsrecord sr;
	(void)sesflags;
	if (rootinode == MFS_ROOT_ID) {
		*trspace = gMetadata->trashspace;
		*respace = gMetadata->reservedspace;
		rn = gMetadata->root;
	} else {
		*trspace = 0;
		*respace = 0;
		rn = fsnodes_id_to_node(rootinode);
	}
	if (!rn || rn->type != TYPE_DIRECTORY) {
		*totalspace = 0;
		*availspace = 0;
		*inodes = 0;
	} else {
		matocsserv_getspace(totalspace, availspace);
		fsnodes_get_stats(rn, &sr);
		*inodes = sr.inodes;
		if (sr.realsize + *availspace < *totalspace) {
			*totalspace = sr.realsize + *availspace;
		}
	}
	stats_statfs++;
}
#endif /* #ifndef METARESTORE */

uint8_t fs_apply_checksum(const std::string &version, uint64_t checksum) {
	std::string versionString = lizardfsVersionToString(LIZARDFS_VERSHEX);
	uint64_t computedChecksum = fs_checksum(ChecksumMode::kGetCurrent);
	gMetadata->metaversion++;
	if (!gDisableChecksumVerification && (version == versionString)) {
		if (checksum != computedChecksum) {
			return LIZARDFS_ERROR_BADMETADATACHECKSUM;
		}
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_apply_access(uint32_t ts, uint32_t inode) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	p->atime = ts;
	fsnodes_update_checksum(p);
	gMetadata->metaversion++;
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_access(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid, uint32_t gid,
			int modemask) {
	fsnode *p, *rn;
	if ((sesflags & SESFLAG_READONLY) && (modemask & MODE_MASK_W)) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	return fsnodes_access(p, uid, gid, modemask, sesflags) ? LIZARDFS_STATUS_OK : LIZARDFS_ERROR_EACCES;
}

uint8_t fs_lookup(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint32_t uid, uint32_t gid, uint32_t auid, uint32_t agid,
		uint32_t *inode, Attributes &attr) {
	fsnode *wd, *rn;
	fsedge *e;

	*inode = 0;
	memset(attr, 0, 35);
	if (rootinode == MFS_ROOT_ID) {
		rn = gMetadata->root;
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (parent == MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, wd)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (wd->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd, uid, gid, MODE_MASK_X, sesflags)) {
		return LIZARDFS_ERROR_EACCES;
	}
	if (name[0] == '.') {
		if (nleng == 1) {  // self
			if (parent == rootinode) {
				*inode = MFS_ROOT_ID;
			} else {
				*inode = wd->id;
			}
			fsnodes_fill_attr(wd, wd, uid, gid, auid, agid, sesflags, attr);
			stats_lookup++;
			return LIZARDFS_STATUS_OK;
		}
		if (nleng == 2 && name[1] == '.') {  // parent
			if (parent == rootinode) {
				*inode = MFS_ROOT_ID;
				fsnodes_fill_attr(wd, wd, uid, gid, auid, agid, sesflags, attr);
			} else {
				if (wd->parents) {
					if (wd->parents->parent->id == rootinode) {
						*inode = MFS_ROOT_ID;
					} else {
						*inode = wd->parents->parent->id;
					}
					fsnodes_fill_attr(wd->parents->parent, wd, uid, gid, auid,
					                  agid, sesflags, attr);
				} else {
					*inode = MFS_ROOT_ID;  // rn->id;
					fsnodes_fill_attr(rn, wd, uid, gid, auid, agid, sesflags,
					                  attr);
				}
			}
			stats_lookup++;
			return LIZARDFS_STATUS_OK;
		}
	}
	if (fsnodes_namecheck(nleng, name) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd, nleng, name);
	if (!e) {
		return LIZARDFS_ERROR_ENOENT;
	}
	*inode = e->child->id;
	fsnodes_fill_attr(e->child, wd, uid, gid, auid, agid, sesflags, attr);
	stats_lookup++;
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_getattr(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid, uint32_t gid,
			uint32_t auid, uint32_t agid, Attributes &attr) {
	fsnode *p, *rn;

	(void)sesflags;
	memset(attr, 0, 35);
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	fsnodes_fill_attr(p, NULL, uid, gid, auid, agid, sesflags, attr);
	stats_getattr++;
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_try_setlength(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t opened,
			uint32_t uid, uint32_t gid, uint32_t auid, uint32_t agid, uint64_t length,
			bool denyTruncatingParity, uint32_t lockId, Attributes &attr,
			uint64_t *chunkid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p, *rn;
	memset(attr, 0, 35);
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (opened == 0) {
		if (!fsnodes_access(p, uid, gid, MODE_MASK_W, sesflags)) {
			return LIZARDFS_ERROR_EACCES;
		}
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	if (length & MFSCHUNKMASK) {
		uint32_t indx = (length >> MFSCHUNKBITS);
		if (indx < p->data.fdata.chunks) {
			uint64_t ochunkid = p->data.fdata.chunktab[indx];
			if (ochunkid > 0) {
				uint8_t status;
				uint64_t nchunkid;
				// We deny truncating parity only if truncating down
				denyTruncatingParity =
				        denyTruncatingParity && (length < p->data.fdata.length);
				status = chunk_multi_truncate(
				        ochunkid, lockId, (length & MFSCHUNKMASK), p->goal,
				        denyTruncatingParity,
				        fsnodes_size_quota_exceeded(p->uid, p->gid), &nchunkid);
				if (status != LIZARDFS_STATUS_OK) {
					return status;
				}
				p->data.fdata.chunktab[indx] = nchunkid;
				*chunkid = nchunkid;
				fs_changelog(ts,
				             "TRUNC(%" PRIu32 ",%" PRIu32 ",%" PRIu32 "):%" PRIu64,
				             inode, indx, lockId, nchunkid);
				fsnodes_update_checksum(p);
				return LIZARDFS_ERROR_DELAYED;
			}
		}
	}
	fsnodes_fill_attr(p, NULL, uid, gid, auid, agid, sesflags, attr);
	stats_setattr++;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_apply_trunc(uint32_t ts, uint32_t inode, uint32_t indx, uint64_t chunkid,
			uint32_t lockid) {
	uint64_t ochunkid, nchunkid;
	uint8_t status;
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (indx > MAX_INDEX) {
		return LIZARDFS_ERROR_INDEXTOOBIG;
	}
	if (indx >= p->data.fdata.chunks) {
		return LIZARDFS_ERROR_EINVAL;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	if (ochunkid == 0) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	status = chunk_apply_modification(ts, ochunkid, lockid, p->goal, true, &nchunkid);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (chunkid != nchunkid) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	gMetadata->metaversion++;
	fsnodes_update_checksum(p);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_set_nextchunkid(const FsContext &context, uint64_t nextChunkId) {
	ChecksumUpdater cu(context.ts());
	uint8_t status = chunk_set_next_chunkid(nextChunkId);
	if (context.isPersonalityMaster()) {
		if (status == LIZARDFS_STATUS_OK) {
			fs_changelog(context.ts(), "NEXTCHUNKID(%" PRIu64 ")", nextChunkId);
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

#ifndef METARESTORE
uint8_t fs_end_setlength(uint64_t chunkid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fs_changelog(ts, "UNLOCK(%" PRIu64 ")", chunkid);
	return chunk_unlock(chunkid);
}
#endif

uint8_t fs_apply_unlock(uint64_t chunkid) {
	gMetadata->metaversion++;
	return chunk_unlock(chunkid);
}

#ifndef METARESTORE
uint8_t fs_do_setlength(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid,
			uint32_t gid, uint32_t auid, uint32_t agid, uint64_t length,
			Attributes &attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = NULL;

	memset(attr, 0, 35);
	if (rootinode == MFS_ROOT_ID || rootinode == 0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		fsnode *rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	fsnodes_setlength(p, length);
	fs_changelog(ts, "LENGTH(%" PRIu32 ",%" PRIu64 ")", inode, p->data.fdata.length);
	p->ctime = p->mtime = ts;
	fsnodes_update_checksum(p);
	fsnodes_fill_attr(p, NULL, uid, gid, auid, agid, sesflags, attr);
	stats_setattr++;
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_setattr(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid, uint32_t gid,
		uint32_t auid, uint32_t agid, uint8_t setmask, uint16_t attrmode,
		uint32_t attruid, uint32_t attrgid, uint32_t attratime, uint32_t attrmtime,
		SugidClearMode sugidclearmode, Attributes &attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = NULL;

	memset(attr, 0, 35);
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		fsnode *rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (uid != 0 && (sesflags & SESFLAG_MAPALL) && (setmask & (SET_UID_FLAG | SET_GID_FLAG))) {
		return LIZARDFS_ERROR_EPERM;
	}
	if ((p->mode & (EATTR_NOOWNER << 12)) == 0 && uid != 0 && uid != p->uid) {
		if (setmask & (SET_MODE_FLAG | SET_UID_FLAG | SET_GID_FLAG)) {
			return LIZARDFS_ERROR_EPERM;
		}
		if ((setmask & SET_ATIME_FLAG) && !(setmask & SET_ATIME_NOW_FLAG)) {
			return LIZARDFS_ERROR_EPERM;
		}
		if ((setmask & SET_MTIME_FLAG) && !(setmask & SET_MTIME_NOW_FLAG)) {
			return LIZARDFS_ERROR_EPERM;
		}
		if ((setmask & (SET_ATIME_NOW_FLAG | SET_MTIME_NOW_FLAG)) &&
		    !fsnodes_access(p, uid, gid, MODE_MASK_W, sesflags)) {
			return LIZARDFS_ERROR_EACCES;
		}
	}
	if (uid != 0 && uid != attruid && (setmask & SET_UID_FLAG)) {
		return LIZARDFS_ERROR_EPERM;
	}
	if ((sesflags & SESFLAG_IGNOREGID) == 0) {
		if (uid != 0 && gid != attrgid && (setmask & SET_GID_FLAG)) {
			return LIZARDFS_ERROR_EPERM;
		}
	}
	// first ignore sugid clears done by kernel
	if ((setmask & (SET_UID_FLAG | SET_GID_FLAG)) &&
	    (setmask & SET_MODE_FLAG)) {  // chown+chmod = chown with sugid clears
		attrmode |= (p->mode & 06000);
	}
	// then do it yourself
	if ((p->mode & 06000) &&
	    (setmask & (SET_UID_FLAG |
	                SET_GID_FLAG))) {  // this is "chown" operation and suid or sgid bit is set
		switch (sugidclearmode) {
		case SugidClearMode::kAlways:
			p->mode &= 0171777;  // safest approach - always delete both suid and sgid
			attrmode &= 01777;
			break;
		case SugidClearMode::kOsx:
			if (uid != 0) {  // OSX+Solaris - every change done by unprivileged user
				         // should clear suid and sgid
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		case SugidClearMode::kBsd:
			if (uid != 0 && (setmask & SET_GID_FLAG) &&
			    p->gid != attrgid) {  // *BSD - like in kOsx but only when something is
				                  // actually changed
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		case SugidClearMode::kExt:
			if (p->type != TYPE_DIRECTORY) {
				if (p->mode & 010) {  // when group exec is set - clear both bits
					p->mode &= 0171777;
					attrmode &= 01777;
				} else {  // when group exec is not set - clear suid only
					p->mode &= 0173777;
					attrmode &= 03777;
				}
			}
			break;
		case SugidClearMode::kXfs:
			if (p->type != TYPE_DIRECTORY) {  // similar to EXT3, but unprivileged users
				                          // also clear suid/sgid bits on
				                          // directories
				if (p->mode & 010) {
					p->mode &= 0171777;
					attrmode &= 01777;
				} else {
					p->mode &= 0173777;
					attrmode &= 03777;
				}
			} else if (uid != 0) {
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		case SugidClearMode::kNever:
			break;
		}
	}
	if (setmask & SET_MODE_FLAG) {
		p->mode = (attrmode & 07777) | (p->mode & 0xF000);
	}
	if (setmask & (SET_UID_FLAG | SET_GID_FLAG)) {
		fsnodes_change_uid_gid(p, ((setmask & SET_UID_FLAG) ? attruid : p->uid),
		                       ((setmask & SET_GID_FLAG) ? attrgid : p->gid));
	}
	if (setmask & SET_ATIME_NOW_FLAG) {
		p->atime = ts;
	} else if (setmask & SET_ATIME_FLAG) {
		p->atime = attratime;
	}
	if (setmask & SET_MTIME_NOW_FLAG) {
		p->mtime = ts;
	} else if (setmask & SET_MTIME_FLAG) {
		p->mtime = attrmtime;
	}
	fs_changelog(ts, "ATTR(%" PRIu32 ",%d,%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ")",
	             inode, p->mode & 07777, p->uid, p->gid, p->atime, p->mtime);
	p->ctime = ts;
	fsnodes_fill_attr(p, NULL, uid, gid, auid, agid, sesflags, attr);
	fsnodes_update_checksum(p);
	stats_setattr++;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_apply_attr(uint32_t ts, uint32_t inode, uint32_t mode, uint32_t uid, uint32_t gid,
			uint32_t atime, uint32_t mtime) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (mode > 07777) {
		return LIZARDFS_ERROR_EINVAL;
	}
	p->mode = mode | (p->mode & 0xF000);
	if (p->uid != uid || p->gid != gid) {
		fsnodes_change_uid_gid(p, uid, gid);
	}
	p->atime = atime;
	p->mtime = mtime;
	p->ctime = ts;
	fsnodes_update_checksum(p);
	gMetadata->metaversion++;
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_apply_length(uint32_t ts, uint32_t inode, uint64_t length) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EINVAL;
	}
	fsnodes_setlength(p, length);
	p->mtime = ts;
	p->ctime = ts;
	fsnodes_update_checksum(p);
	gMetadata->metaversion++;
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE

/// Update atime of the given node and generate a changelog entry.
/// Doesn't do anything if NO_ATIME=1 is set in the config file.
static inline void fs_update_atime(fsnode *p, uint32_t ts) {
	if (!gAtimeDisabled && p->atime != ts) {
		p->atime = ts;
		fsnodes_update_checksum(p);
		fs_changelog(ts, "ACCESS(%" PRIu32 ")", p->id);
	}
}

uint8_t fs_readlink(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t *pleng,
			uint8_t **path) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = NULL;

	(void)sesflags;
	*pleng = 0;
	*path = NULL;
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		fsnode *rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (p->type != TYPE_SYMLINK) {
		return LIZARDFS_ERROR_EINVAL;
	}
	*pleng = p->data.sdata.pleng;
	*path = p->data.sdata.path;
	fs_update_atime(p, ts);
	stats_readlink++;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_symlink(const FsContext &context, uint32_t parent, uint16_t nleng, const uint8_t *name,
		uint32_t pleng, const uint8_t *path, uint32_t *inode, Attributes *attr) {
	ChecksumUpdater cu(context.ts());
	fsnode *wd;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent, &wd);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (pleng == 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	for (uint32_t i = 0; i < pleng; i++) {
		if (path[i] == 0) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}
	if (fsnodes_namecheck(nleng, name) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd, nleng, name)) {
		return LIZARDFS_ERROR_EEXIST;
	}
	if ((context.isPersonalityMaster()) &&
	    fsnodes_inode_quota_exceeded(context.uid(), context.gid())) {
		return LIZARDFS_ERROR_QUOTA;
	}
	uint8_t *newpath = (uint8_t *)malloc(pleng);
	passert(newpath);
	fsnode *p = fsnodes_create_node(context.ts(), wd, nleng, name, TYPE_SYMLINK, 0777, 0,
	                                context.uid(), context.gid(), 0,
	                                AclInheritance::kDontInheritAcl);
	memcpy(newpath, path, pleng);
	p->data.sdata.path = newpath;
	p->data.sdata.pleng = pleng;
	fsnodes_update_checksum(p);
	statsrecord sr;
	memset(&sr, 0, sizeof(statsrecord));
	sr.length = pleng;
	fsnodes_add_stats(wd, &sr);
	if (attr != NULL) {
		fsnodes_fill_attr(context, p, wd, *attr);
	}
	if (context.isPersonalityMaster()) {
		*inode = p->id;
		fs_changelog(context.ts(),
		             "SYMLINK(%" PRIu32 ",%s,%s,%" PRIu32 ",%" PRIu32 "):%" PRIu32, wd->id,
		             fsnodes_escape_name(nleng, name), fsnodes_escape_name(pleng, newpath),
		             context.uid(), context.gid(), p->id);
	} else {
		if (*inode != p->id) {
			return LIZARDFS_ERROR_MISMATCH;
		}
		gMetadata->metaversion++;
	}
#ifndef METARESTORE
	stats_symlink++;
#endif /* #ifndef METARESTORE */
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_mknod(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid,
		uint32_t gid, uint32_t auid, uint32_t agid, uint32_t rdev, uint32_t *inode,
		Attributes &attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *wd, *p, *rn;
	*inode = 0;
	memset(attr, 0, 35);
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (type != TYPE_FILE && type != TYPE_SOCKET && type != TYPE_FIFO &&
	    type != TYPE_BLOCKDEV && type != TYPE_CHARDEV) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (rootinode == MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (parent == MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, wd)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (wd->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd, uid, gid, MODE_MASK_W, sesflags)) {
		return LIZARDFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng, name) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd, nleng, name)) {
		return LIZARDFS_ERROR_EEXIST;
	}
	if (fsnodes_inode_quota_exceeded(uid, gid)) {
		return LIZARDFS_ERROR_QUOTA;
	}
	p = fsnodes_create_node(ts, wd, nleng, name, type, mode, umask, uid, gid, 0,
	                        AclInheritance::kInheritAcl);
	if (type == TYPE_BLOCKDEV || type == TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
	}
	*inode = p->id;
	fsnodes_fill_attr(p, wd, uid, gid, auid, agid, sesflags, attr);
	fs_changelog(ts,
	             "CREATE(%" PRIu32 ",%s,%c,%d,%" PRIu32 ",%" PRIu32 ",%" PRIu32 "):%" PRIu32,
	             parent, fsnodes_escape_name(nleng, name), type, p->mode & 07777, uid, gid,
	             rdev, p->id);
	stats_mknod++;
	fsnodes_update_checksum(p);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_mkdir(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid,
		uint32_t auid, uint32_t agid, uint8_t copysgid, uint32_t *inode,
		Attributes &attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *wd, *p, *rn;
	*inode = 0;
	memset(attr, 0, 35);
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (parent == MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, wd)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (wd->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd, uid, gid, MODE_MASK_W, sesflags)) {
		return LIZARDFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng, name) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd, nleng, name)) {
		return LIZARDFS_ERROR_EEXIST;
	}
	if (fsnodes_inode_quota_exceeded(uid, gid)) {
		return LIZARDFS_ERROR_QUOTA;
	}
	p = fsnodes_create_node(ts, wd, nleng, name, TYPE_DIRECTORY, mode, umask, uid, gid,
	                        copysgid, AclInheritance::kInheritAcl);
	*inode = p->id;
	fsnodes_fill_attr(p, wd, uid, gid, auid, agid, sesflags, attr);
	fs_changelog(ts,
	             "CREATE(%" PRIu32 ",%s,%c,%d,%" PRIu32 ",%" PRIu32 ",%" PRIu32 "):%" PRIu32,
	             parent, fsnodes_escape_name(nleng, name), TYPE_DIRECTORY, p->mode & 07777, uid,
	             gid, 0, p->id);
	stats_mkdir++;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_apply_create(uint32_t ts, uint32_t parent, uint32_t nleng, const uint8_t *name,
		uint8_t type, uint32_t mode, uint32_t uid, uint32_t gid, uint32_t rdev,
		uint32_t inode) {
	fsnode *wd, *p;
	if (type != TYPE_FILE && type != TYPE_SOCKET && type != TYPE_FIFO &&
	    type != TYPE_BLOCKDEV && type != TYPE_CHARDEV && type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_EINVAL;
	}
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (wd->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (fsnodes_nameisused(wd, nleng, name)) {
		return LIZARDFS_ERROR_EEXIST;
	}
	p = fsnodes_create_node(ts, wd, nleng, name, type, mode, 0, uid, gid, 0,
	                        AclInheritance::kInheritAcl);
	if (type == TYPE_BLOCKDEV || type == TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
		fsnodes_update_checksum(p);
	}
	if (inode != p->id) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	gMetadata->metaversion++;
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_unlink(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint32_t uid, uint32_t gid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *wd, *rn;
	fsedge *e;
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (parent == MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, wd)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (wd->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd, uid, gid, MODE_MASK_W, sesflags)) {
		return LIZARDFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng, name) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd, nleng, name);
	if (!e) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (!fsnodes_sticky_access(wd, e->child, uid)) {
		return LIZARDFS_ERROR_EPERM;
	}
	if (e->child->type == TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_EPERM;
	}
	fs_changelog(ts, "UNLINK(%" PRIu32 ",%s):%" PRIu32, parent,
	             fsnodes_escape_name(nleng, name), e->child->id);
	fsnodes_unlink(ts, e);
	stats_unlink++;
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_rmdir(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint32_t uid, uint32_t gid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *wd, *rn;
	fsedge *e;
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (parent == MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, wd)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (wd->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd, uid, gid, MODE_MASK_W, sesflags)) {
		return LIZARDFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng, name) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd, nleng, name);
	if (!e) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (!fsnodes_sticky_access(wd, e->child, uid)) {
		return LIZARDFS_ERROR_EPERM;
	}
	if (e->child->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (e->child->data.ddata.children != NULL) {
		return LIZARDFS_ERROR_ENOTEMPTY;
	}
	fs_changelog(ts, "UNLINK(%" PRIu32 ",%s):%" PRIu32, parent,
	             fsnodes_escape_name(nleng, name), e->child->id);
	fsnodes_unlink(ts, e);
	stats_rmdir++;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_apply_unlink(uint32_t ts, uint32_t parent, uint32_t nleng, const uint8_t *name,
		uint32_t inode) {
	fsnode *wd;
	fsedge *e;
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (wd->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	e = fsnodes_lookup(wd, nleng, name);
	if (!e) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (e->child->id != inode) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	if (e->child->type == TYPE_DIRECTORY && e->child->data.ddata.children != NULL) {
		return LIZARDFS_ERROR_ENOTEMPTY;
	}
	fsnodes_unlink(ts, e);
	gMetadata->metaversion++;
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_rename(const FsContext &context, uint32_t parent_src, uint16_t nleng_src,
		const uint8_t *name_src, uint32_t parent_dst, uint16_t nleng_dst,
		const uint8_t *name_dst, uint32_t *inode, Attributes *attr) {
	ChecksumUpdater cu(context.ts());
	fsnode *swd;
	fsnode *dwd;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent_dst, &dwd);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent_src, &swd);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (fsnodes_namecheck(nleng_src, name_src) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	fsedge *se = fsnodes_lookup(swd, nleng_src, name_src);
	if (!se) {
		return LIZARDFS_ERROR_ENOENT;
	}
	fsnode *node = se->child;
	if (context.canCheckPermissions() && !fsnodes_sticky_access(swd, node, context.uid())) {
		return LIZARDFS_ERROR_EPERM;
	}
	if ((context.personality() != metadataserver::Personality::kMaster) &&
	    (node->id != *inode)) {
		return LIZARDFS_ERROR_MISMATCH;
	} else {
		*inode = node->id;
	}
	if (se->child->type == TYPE_DIRECTORY) {
		if (fsnodes_isancestor(se->child, dwd)) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}
	if (fsnodes_namecheck(nleng_dst, name_dst) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	fsedge *de = fsnodes_lookup(dwd, nleng_dst, name_dst);
	if (de) {
		if (de->child->type == TYPE_DIRECTORY && de->child->data.ddata.children != NULL) {
			return LIZARDFS_ERROR_ENOTEMPTY;
		}
		if (context.canCheckPermissions() &&
		    !fsnodes_sticky_access(dwd, de->child, context.uid())) {
			return LIZARDFS_ERROR_EPERM;
		}
		fsnodes_unlink(context.ts(), de);
	}
	fsnodes_remove_edge(context.ts(), se);
	fsnodes_link(context.ts(), dwd, node, nleng_dst, name_dst);
	if (attr) {
		fsnodes_fill_attr(context, node, dwd, *attr);
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "MOVE(%" PRIu32 ",%s,%" PRIu32 ",%s):%" PRIu32, swd->id,
		             fsnodes_escape_name(nleng_src, name_src), dwd->id,
		             fsnodes_escape_name(nleng_dst, name_dst), node->id);
	} else {
		gMetadata->metaversion++;
	}
#ifndef METARESTORE
	stats_rename++;
#endif
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_link(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
		uint16_t nleng_dst, const uint8_t *name_dst, uint32_t *inode, Attributes *attr) {
	ChecksumUpdater cu(context.ts());
	fsnode *sp;
	fsnode *dwd;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kDirectory, MODE_MASK_W,
	                                        parent_dst, &dwd);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kNotDirectory,
	                                        MODE_MASK_EMPTY, inode_src, &sp);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (sp->type == TYPE_TRASH || sp->type == TYPE_RESERVED) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (fsnodes_namecheck(nleng_dst, name_dst) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (fsnodes_nameisused(dwd, nleng_dst, name_dst)) {
		return LIZARDFS_ERROR_EEXIST;
	}
	fsnodes_link(context.ts(), dwd, sp, nleng_dst, name_dst);
	if (inode) {
		*inode = inode_src;
	}
	if (attr) {
		fsnodes_fill_attr(context, sp, dwd, *attr);
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "LINK(%" PRIu32 ",%" PRIu32 ",%s)", sp->id, dwd->id,
		             fsnodes_escape_name(nleng_dst, name_dst));
	} else {
		gMetadata->metaversion++;
	}
#ifndef METARESTORE
	stats_link++;
#endif
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_snapshot(const FsContext &context, uint32_t inode_src, uint32_t parent_dst,
		uint16_t nleng_dst, const uint8_t *name_dst, uint8_t canoverwrite) {
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
	status = fsnodes_snapshot_test(sp, sp, dwd, nleng_dst, name_dst, canoverwrite);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	fsnodes_snapshot(context.ts(), sp, dwd, nleng_dst, name_dst);
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "SNAPSHOT(%" PRIu32 ",%" PRIu32 ",%s,%" PRIu8 ")",
		             sp->id, dwd->id, fsnodes_escape_name(nleng_dst, name_dst),
		             canoverwrite);
	} else {
		gMetadata->metaversion++;
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_append(const FsContext &context, uint32_t inode, uint32_t inode_src) {
	ChecksumUpdater cu(context.ts());
	fsnode *p, *sp;
	if (inode == inode_src) {
		return LIZARDFS_ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kFile, MODE_MASK_W,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kFile, MODE_MASK_R,
	                                        inode_src, &sp);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (context.isPersonalityMaster() && fsnodes_size_quota_exceeded(p->uid, p->gid)) {
		return LIZARDFS_ERROR_QUOTA;
	}
	status = fsnodes_appendchunks(context.ts(), p, sp);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "APPEND(%" PRIu32 ",%" PRIu32 ")", p->id, sp->id);
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

#ifndef METARESTORE
uint8_t fs_readdir_size(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid,
		uint32_t gid, uint8_t flags, void **dnode, uint32_t *dbuffsize) {
	fsnode *p, *rn;
	*dnode = NULL;
	*dbuffsize = 0;
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (p->type != TYPE_DIRECTORY) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if (!fsnodes_access(p, uid, gid, MODE_MASK_R, sesflags)) {
		return LIZARDFS_ERROR_EACCES;
	}
	*dnode = p;
	*dbuffsize = fsnodes_getdirsize(p, flags & GETDIR_FLAG_WITHATTR);
	return LIZARDFS_STATUS_OK;
}

void fs_readdir_data(uint32_t rootinode, uint8_t sesflags, uint32_t uid, uint32_t gid,
		uint32_t auid, uint32_t agid, uint8_t flags, void *dnode, uint8_t *dbuff) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = (fsnode *)dnode;
	fs_update_atime(p, ts);
	fsnodes_getdirdata(rootinode, uid, gid, auid, agid, sesflags, p, dbuff,
	                   flags & GETDIR_FLAG_WITHATTR);
	stats_readdir++;
}

uint8_t fs_checkfile(uint32_t rootinode, uint8_t sesflags, uint32_t inode,
		uint32_t chunkcount[CHUNK_MATRIX_SIZE]) {
	fsnode *p, *rn;
	(void)sesflags;
	if (rootinode == MFS_ROOT_ID || rootinode == 0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	fsnodes_checkfile(p, chunkcount);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_opencheck(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid,
		uint32_t gid, uint32_t auid, uint32_t agid, uint8_t flags, Attributes &attr) {
	fsnode *p, *rn;
	if ((sesflags & SESFLAG_READONLY) && (flags & WANT_WRITE)) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
#ifndef METARESTORE
	if (fsnodes_has_tape_goal(p) && (flags & WANT_WRITE)) {
		lzfs_pretty_syslog(LOG_INFO, "Access denied: node %d has tape goal", inode);
		return LIZARDFS_ERROR_EPERM;
	}
#endif
	if ((flags & AFTER_CREATE) == 0) {
		uint8_t modemask = 0;
		if (flags & WANT_READ) {
			modemask |= MODE_MASK_R;
		}
		if (flags & WANT_WRITE) {
			modemask |= MODE_MASK_W;
		}
		if (!fsnodes_access(p, uid, gid, modemask, sesflags)) {
			return LIZARDFS_ERROR_EACCES;
		}
	}
	fsnodes_fill_attr(p, NULL, uid, gid, auid, agid, sesflags, attr);
	stats_open++;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_acquire(const FsContext &context, uint32_t inode, uint32_t sessionid) {
	ChecksumUpdater cu(context.ts());
#ifndef METARESTORE
	if (context.isPersonalityShadow()) {
		matoclserv_add_open_file(sessionid, inode);
	}
#endif /* #ifndef METARESTORE */
	fsnode *p;
	sessionidrec *cr;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	for (cr = p->data.fdata.sessionids; cr; cr = cr->next) {
		if (cr->sessionid == sessionid) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}
	cr = sessionidrec_malloc();
	cr->sessionid = sessionid;
	cr->next = p->data.fdata.sessionids;
	p->data.fdata.sessionids = cr;
	fsnodes_update_checksum(p);
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "ACQUIRE(%" PRIu32 ",%" PRIu32 ")", inode, sessionid);
	} else {
		gMetadata->metaversion++;
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_release(const FsContext &context, uint32_t inode, uint32_t sessionid) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	sessionidrec *cr, **crp;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	crp = &(p->data.fdata.sessionids);
	while ((cr = *crp)) {
		if (cr->sessionid == sessionid) {
			*crp = cr->next;
			sessionidrec_free(cr);
#ifndef METARESTORE
			if (context.isPersonalityShadow()) {
				matoclserv_remove_open_file(sessionid, inode);
			}
#endif /* #ifndef METARESTORE */
			if (context.isPersonalityMaster()) {
				fs_changelog(context.ts(), "RELEASE(%" PRIu32 ",%" PRIu32 ")",
				             inode, sessionid);
			} else {
				gMetadata->metaversion++;
			}
			fsnodes_update_checksum(p);
			return LIZARDFS_STATUS_OK;
		} else {
			crp = &(cr->next);
		}
	}
#ifndef METARESTORE
	syslog(LOG_WARNING, "release: session not found");
#endif
	return LIZARDFS_ERROR_EINVAL;
}

#ifndef METARESTORE
uint32_t fs_newsessionid(void) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fs_changelog(ts, "SESSION():%" PRIu32, gMetadata->nextsessionid);
	return gMetadata->nextsessionid++;
}
#endif
uint8_t fs_apply_session(uint32_t sessionid) {
	if (sessionid != gMetadata->nextsessionid) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	gMetadata->metaversion++;
	gMetadata->nextsessionid++;
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_auto_repair_if_needed(fsnode *p, uint32_t chunkIndex) {
	uint64_t chunkId =
	        (chunkIndex < p->data.fdata.chunks ? p->data.fdata.chunktab[chunkIndex] : 0);
	if (chunkId != 0 && chunk_has_only_invalid_copies(chunkId)) {
		uint32_t notchanged, erased, repaired;
		fs_repair(MFS_ROOT_ID, 0, p->id, 0, 0, &notchanged, &erased, &repaired);
		syslog(LOG_NOTICE,
		       "auto repair inode %" PRIu32 ", chunk %016" PRIX64
		       ": "
		       "not changed: %" PRIu32 ", erased: %" PRIu32 ", repaired: %" PRIu32,
		       p->id, chunkId, notchanged, erased, repaired);
		DEBUG_LOG("master.fs.file_auto_repaired") << p->id << " " << repaired;
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_readchunk(uint32_t inode, uint32_t indx, uint64_t *chunkid, uint64_t *length) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p;

	*chunkid = 0;
	*length = 0;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	if (indx > MAX_INDEX) {
		return LIZARDFS_ERROR_INDEXTOOBIG;
	}
#ifndef METARESTORE
	if (gMagicAutoFileRepair) {
		fs_auto_repair_if_needed(p, indx);
	}
#endif
	if (indx < p->data.fdata.chunks) {
		*chunkid = p->data.fdata.chunktab[indx];
	}
	*length = p->data.fdata.length;
	fs_update_atime(p, ts);
	stats_read++;
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_writechunk(const FsContext &context, uint32_t inode, uint32_t indx, bool usedummylockid,
		/* inout */ uint32_t *lockid, uint64_t *chunkid, uint8_t *opflag,
		uint64_t *length) {
	ChecksumUpdater cu(context.ts());
	uint32_t i;
	uint64_t ochunkid, nchunkid;
	fsnode *p;

	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kFile, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (indx > MAX_INDEX) {
		return LIZARDFS_ERROR_INDEXTOOBIG;
	}
#ifndef METARESTORE
	if (gMagicAutoFileRepair && context.isPersonalityMaster()) {
		fs_auto_repair_if_needed(p, indx);
	}
#endif

	const bool quota_exceeded = fsnodes_size_quota_exceeded(p->uid, p->gid);
	statsrecord psr;
	fsnodes_get_stats(p, &psr);

	/* resize chunks structure */
	if (indx >= p->data.fdata.chunks) {
		if (context.isPersonalityMaster() && quota_exceeded) {
			return LIZARDFS_ERROR_QUOTA;
		}
		uint32_t newsize;
		if (indx < 8) {
			newsize = indx + 1;
		} else if (indx < 64) {
			newsize = (indx & 0xFFFFFFF8) + 8;
		} else {
			newsize = (indx & 0xFFFFFFC0) + 64;
		}
		if (p->data.fdata.chunktab == NULL) {
			p->data.fdata.chunktab = (uint64_t *)malloc(sizeof(uint64_t) * newsize);
		} else {
			p->data.fdata.chunktab = (uint64_t *)realloc(p->data.fdata.chunktab,
			                                             sizeof(uint64_t) * newsize);
		}
		passert(p->data.fdata.chunktab);
		for (i = p->data.fdata.chunks; i < newsize; i++) {
			p->data.fdata.chunktab[i] = 0;
		}
		p->data.fdata.chunks = newsize;
	}

	ochunkid = p->data.fdata.chunktab[indx];
	if (context.isPersonalityMaster()) {
#ifndef METARESTORE
		status = chunk_multi_modify(ochunkid, lockid, p->goal, usedummylockid,
		                            quota_exceeded, opflag, &nchunkid);
#else
		(void)usedummylockid;
		// This will NEVER happen (metarestore doesn't call this in master context)
		mabort("bad code path: fs_writechunk");
#endif
	} else {
		bool increaseVersion = (*opflag != 0);
		status = chunk_apply_modification(context.ts(), ochunkid, *lockid, p->goal,
		                                  increaseVersion, &nchunkid);
	}
	if (status != LIZARDFS_STATUS_OK) {
		fsnodes_update_checksum(p);
		return status;
	}
	if (context.isPersonalityShadow() && nchunkid != *chunkid) {
		fsnodes_update_checksum(p);
		return LIZARDFS_ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	*chunkid = nchunkid;
	statsrecord nsr;
	fsnodes_get_stats(p, &nsr);
	for (fsedge *e = p->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	fsnodes_quota_update_size(p, nsr.size - psr.size);
	if (length) {
		*length = p->data.fdata.length;
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(),
		             "WRITE(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu32 "):%" PRIu64,
		             inode, indx, *opflag, *lockid, nchunkid);
	} else {
		gMetadata->metaversion++;
	}
	if (p->mtime != context.ts() || p->ctime != context.ts()) {
		p->mtime = p->ctime = context.ts();
	}
	fsnodes_update_checksum(p);
#ifndef METARESTORE
	stats_write++;
#endif
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_writeend(uint32_t inode, uint64_t length, uint64_t chunkid, uint32_t lockid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	uint8_t status = chunk_can_unlock(chunkid, lockid);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (length > 0) {
		fsnode *p;
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
		if (length > p->data.fdata.length) {
			fsnodes_setlength(p, length);
			p->mtime = p->ctime = ts;
			fsnodes_update_checksum(p);
			fs_changelog(ts, "LENGTH(%" PRIu32 ",%" PRIu64 ")", inode, length);
		}
	}
	fs_changelog(ts, "UNLOCK(%" PRIu64 ")", chunkid);
	return chunk_unlock(chunkid);
}

void fs_incversion(uint64_t chunkid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fs_changelog(ts, "INCVERSION(%" PRIu64 ")", chunkid);
}
#endif

uint8_t fs_apply_incversion(uint64_t chunkid) {
	gMetadata->metaversion++;
	return chunk_increase_version(chunkid);
}

#ifndef METARESTORE
uint8_t fs_repair(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid, uint32_t gid,
		uint32_t *notchanged, uint32_t *erased, uint32_t *repaired) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	uint32_t nversion, indx;
	statsrecord psr, nsr;
	fsedge *e;
	fsnode *p, *rn;

	*notchanged = 0;
	*erased = 0;
	*repaired = 0;
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID || rootinode == 0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	if (!fsnodes_access(p, uid, gid, MODE_MASK_W, sesflags)) {
		return LIZARDFS_ERROR_EACCES;
	}
	fsnodes_get_stats(p, &psr);
	for (indx = 0; indx < p->data.fdata.chunks; indx++) {
		if (chunk_repair(p->goal, p->data.fdata.chunktab[indx], &nversion)) {
			fs_changelog(ts, "REPAIR(%" PRIu32 ",%" PRIu32 "):%" PRIu32, inode, indx,
			             nversion);
			p->mtime = p->ctime = ts;
			if (nversion > 0) {
				(*repaired)++;
			} else {
				p->data.fdata.chunktab[indx] = 0;
				(*erased)++;
			}
		} else {
			(*notchanged)++;
		}
	}
	fsnodes_get_stats(p, &nsr);
	for (e = p->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	fsnodes_quota_update_size(p, nsr.size - psr.size);
	fsnodes_update_checksum(p);
	return LIZARDFS_STATUS_OK;
}
#endif /* #ifndef METARESTORE */

uint8_t fs_apply_repair(uint32_t ts, uint32_t inode, uint32_t indx, uint32_t nversion) {
	fsnode *p;
	uint8_t status;
	statsrecord psr, nsr;

	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (p->type != TYPE_FILE && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	if (indx > MAX_INDEX) {
		return LIZARDFS_ERROR_INDEXTOOBIG;
	}
	if (indx >= p->data.fdata.chunks) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	if (p->data.fdata.chunktab[indx] == 0) {
		return LIZARDFS_ERROR_NOCHUNK;
	}
	fsnodes_get_stats(p, &psr);
	if (nversion == 0) {
		status = chunk_delete_file(p->data.fdata.chunktab[indx], p->goal);
		p->data.fdata.chunktab[indx] = 0;
	} else {
		status = chunk_set_version(p->data.fdata.chunktab[indx], nversion);
	}
	fsnodes_get_stats(p, &nsr);
	for (fsedge *e = p->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	fsnodes_quota_update_size(p, nsr.size - psr.size);
	gMetadata->metaversion++;
	p->mtime = p->ctime = ts;
	fsnodes_update_checksum(p);
	return status;
}

#ifndef METARESTORE
uint8_t fs_getgoal(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t gmode,
		GoalMap<uint32_t> &fgtab, GoalMap<uint32_t> &dgtab) {
	fsnode *p, *rn;
	(void)sesflags;
	if (!GMODE_ISVALID(gmode)) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (rootinode == MFS_ROOT_ID || rootinode == 0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (p->type != TYPE_DIRECTORY && p->type != TYPE_FILE && p->type != TYPE_TRASH &&
	    p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	fsnodes_getgoal_recursive(p, gmode, fgtab, dgtab);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_gettrashtime_prepare(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t gmode,
	TrashtimeMap &fileTrashtimes, TrashtimeMap &dirTrashtimes) {
	fsnode *p, *rn;
	(void)sesflags;

	if (!GMODE_ISVALID(gmode)) {
		return LIZARDFS_ERROR_EINVAL;
	}

	if (rootinode == MFS_ROOT_ID || rootinode == 0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}

	if (p->type != TYPE_DIRECTORY && p->type != TYPE_FILE && p->type != TYPE_TRASH
		&& p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
	}
	fsnodes_gettrashtime_recursive(p, gmode, fileTrashtimes, dirTrashtimes);

	return LIZARDFS_STATUS_OK;
}

void fs_gettrashtime_store(TrashtimeMap &fileTrashtimes,TrashtimeMap &dirTrashtimes,uint8_t *buff) {
	for (auto i : fileTrashtimes) {
		put32bit(&buff, i.first);
		put32bit(&buff, i.second);
	}
	for (auto i : dirTrashtimes) {
		put32bit(&buff, i.first);
		put32bit(&buff, i.second);
	}
}

uint8_t fs_geteattr(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t gmode,
			uint32_t feattrtab[16], uint32_t deattrtab[16]) {
	fsnode *p, *rn;
	(void)sesflags;
	memset(feattrtab, 0, 16 * sizeof(uint32_t));
	memset(deattrtab, 0, 16 * sizeof(uint32_t));
	if (!GMODE_ISVALID(gmode)) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (rootinode == MFS_ROOT_ID || rootinode == 0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	fsnodes_geteattr_recursive(p, gmode, feattrtab, deattrtab);
	return LIZARDFS_STATUS_OK;
}

#endif

uint8_t fs_setgoal(const FsContext &context, uint32_t inode, uint8_t goal, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	ChecksumUpdater cu(context.ts());
	if (!SMODE_ISVALID(smode) || !goal::isGoalValid(goal) ||
	    (smode & (SMODE_INCREASE | SMODE_DECREASE) && goal::isXorGoal(goal))) {
		return LIZARDFS_ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kAny);
	if (status != 0) {
		return status;
	}
	fsnode *p;
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (p->type != TYPE_DIRECTORY && p->type != TYPE_FILE && p->type != TYPE_TRASH &&
	    p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	uint32_t si = 0;
	uint32_t nci = 0;
	uint32_t nsi = 0;
	sassert(context.hasUidGidData());
	fsnodes_setgoal_recursive(p, context.ts(), context.uid(), goal, smode, &si, &nci, &nsi);
	if (context.isPersonalityMaster()) {
		if ((smode & SMODE_RMASK) == 0 && nsi > 0 && si == 0 && nci == 0) {
			return LIZARDFS_ERROR_EPERM;
		}
		*sinodes = si;
		*ncinodes = nci;
		*nsinodes = nsi;
		fs_changelog(context.ts(), "SETGOAL(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu8
		                           "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,
		             p->id, context.uid(), goal, smode, si, nci, nsi);
	} else {
		gMetadata->metaversion++;
		if ((*sinodes != si) || (*ncinodes != nci) || (*nsinodes != nsi)) {
			return LIZARDFS_ERROR_MISMATCH;
		}
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_settrashtime(const FsContext &context, uint32_t inode, uint32_t trashtime, uint8_t smode,
			uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	ChecksumUpdater cu(context.ts());
	if (!SMODE_ISVALID(smode)) {
		return LIZARDFS_ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kAny);
	if (status != 0) {
		return status;
	}
	fsnode *p;
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (p->type != TYPE_DIRECTORY && p->type != TYPE_FILE && p->type != TYPE_TRASH &&
	    p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	uint32_t si = 0;
	uint32_t nci = 0;
	uint32_t nsi = 0;
	sassert(context.hasUidGidData());
	fsnodes_settrashtime_recursive(p, context.ts(), context.uid(), trashtime, smode, &si, &nci,
	                               &nsi);
	if (context.isPersonalityMaster()) {
		if ((smode & SMODE_RMASK) == 0 && nsi > 0 && si == 0 && nci == 0) {
			return LIZARDFS_ERROR_EPERM;
		}
		*sinodes = si;
		*ncinodes = nci;
		*nsinodes = nsi;
		fs_changelog(context.ts(), "SETTRASHTIME(%" PRIu32 ",%" PRIu32 ",%" PRIu32
		                           ",%" PRIu8 "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,
		             p->id, context.uid(), trashtime, smode, si, nci, nsi);
	} else {
		gMetadata->metaversion++;
		if ((*sinodes != si) || (*ncinodes != nci) || (*nsinodes != nsi)) {
			return LIZARDFS_ERROR_MISMATCH;
		}
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_seteattr(const FsContext &context, uint32_t inode, uint8_t eattr, uint8_t smode,
			uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	ChecksumUpdater cu(context.ts());
	if (!SMODE_ISVALID(smode) ||
	    (eattr & (~(EATTR_NOOWNER | EATTR_NOACACHE | EATTR_NOECACHE | EATTR_NODATACACHE)))) {
		return LIZARDFS_ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	fsnode *p;
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}

	uint32_t si = 0;
	uint32_t nci = 0;
	uint32_t nsi = 0;
	sassert(context.hasUidGidData());
	fsnodes_seteattr_recursive(p, context.ts(), context.uid(), eattr, smode, &si, &nci, &nsi);
	if (context.isPersonalityMaster()) {
		if ((smode & SMODE_RMASK) == 0 && nsi > 0 && si == 0 && nci == 0) {
			return LIZARDFS_ERROR_EPERM;
		}
		*sinodes = si;
		*ncinodes = nci;
		*nsinodes = nsi;
		fs_changelog(context.ts(), "SETEATTR(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu8
		                           "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,
		             p->id, context.uid(), eattr, smode, si, nci, nsi);
	} else {
		gMetadata->metaversion++;
		if ((*sinodes != si) || (*ncinodes != nci) || (*nsinodes != nsi)) {
			return LIZARDFS_ERROR_MISMATCH;
		}
	}
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE

uint8_t fs_listxattr_leng(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t opened,
			uint32_t uid, uint32_t gid, void **xanode, uint32_t *xasize) {
	fsnode *p, *rn;

	*xasize = 0;
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (opened == 0) {
		if (!fsnodes_access(p, uid, gid, MODE_MASK_R, sesflags)) {
			return LIZARDFS_ERROR_EACCES;
		}
	}
	return xattr_listattr_leng(inode, xanode, xasize);
}

void fs_listxattr_data(void *xanode, uint8_t *xabuff) {
	xattr_listattr_data(xanode, xabuff);
}

uint8_t fs_setxattr(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t opened,
		uint32_t uid, uint32_t gid, uint8_t anleng, const uint8_t *attrname,
		uint32_t avleng, const uint8_t *attrvalue, uint8_t mode) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p, *rn;
	uint8_t status;

	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (opened == 0) {
		if (!fsnodes_access(p, uid, gid, MODE_MASK_W, sesflags)) {
			return LIZARDFS_ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng, attrname) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (mode > XATTR_SMODE_REMOVE) {
		return LIZARDFS_ERROR_EINVAL;
	}
	status = xattr_setattr(inode, anleng, attrname, avleng, attrvalue, mode);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	fsnodes_update_checksum(p);
	fs_changelog(ts, "SETXATTR(%" PRIu32 ",%s,%s,%" PRIu8 ")", inode,
	             fsnodes_escape_name(anleng, attrname), fsnodes_escape_name(avleng, attrvalue),
	             mode);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_getxattr(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t opened,
		uint32_t uid, uint32_t gid, uint8_t anleng, const uint8_t *attrname,
		uint32_t *avleng, uint8_t **attrvalue) {
	fsnode *p, *rn;

	if (rootinode == MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (opened == 0) {
		if (!fsnodes_access(p, uid, gid, MODE_MASK_R, sesflags)) {
			return LIZARDFS_ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng, attrname) < 0) {
		return LIZARDFS_ERROR_EINVAL;
	}
	return xattr_getattr(inode, anleng, attrname, avleng, attrvalue);
}

#endif /* #ifndef METARESTORE */

uint8_t fs_apply_setxattr(uint32_t ts, uint32_t inode, uint32_t anleng, const uint8_t *attrname,
			uint32_t avleng, const uint8_t *attrvalue, uint32_t mode) {
	fsnode *p;
	uint8_t status;
	if (anleng == 0 || anleng > MFS_XATTR_NAME_MAX || avleng > MFS_XATTR_SIZE_MAX ||
	    mode > XATTR_SMODE_REMOVE) {
		return LIZARDFS_ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	status = xattr_setattr(inode, anleng, attrname, avleng, attrvalue, mode);

	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	gMetadata->metaversion++;
	fsnodes_update_checksum(p);
	return status;
}

uint8_t fs_deleteacl(const FsContext &context, uint32_t inode, AclType type) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_deleteacl(p, type, context.ts());
	if (context.isPersonalityMaster()) {
		if (status == LIZARDFS_STATUS_OK) {
			fs_changelog(context.ts(), "DELETEACL(%" PRIu32 ",%c)", p->id,
			             (type == AclType::kAccess ? 'a' : 'd'));
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

#ifndef METARESTORE

uint8_t fs_setacl(const FsContext &context, uint32_t inode, AclType type, AccessControlList acl) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	std::string aclString = acl.toString();
	status = fsnodes_setacl(p, type, std::move(acl), context.ts());
	if (context.isPersonalityMaster()) {
		if (status == LIZARDFS_STATUS_OK) {
			fs_changelog(context.ts(), "SETACL(%" PRIu32 ",%c,%s)", p->id,
			             (type == AclType::kAccess ? 'a' : 'd'), aclString.c_str());
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

uint8_t fs_getacl(const FsContext &context, uint32_t inode, AclType type, AccessControlList &acl) {
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadOnly, SessionType::kAny);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kAny, MODE_MASK_EMPTY,
	                                        inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	return fsnodes_getacl(p, type, acl);
}

#endif /* #ifndef METARESTORE */

uint8_t fs_apply_setacl(uint32_t ts, uint32_t inode, char aclType, const char *aclString) {
	AccessControlList acl;
	try {
		acl = AccessControlList::fromString(aclString);
	} catch (Exception &) {
		return LIZARDFS_ERROR_EINVAL;
	}
	fsnode *p = fsnodes_id_to_node(inode);
	if (!p) {
		return LIZARDFS_ERROR_ENOENT;
	}
	AclType aclTypeEnum;
	if (!decodeChar("da", {AclType::kDefault, AclType::kAccess}, aclType, aclTypeEnum)) {
		return LIZARDFS_ERROR_EINVAL;
	}
	uint8_t status = fsnodes_setacl(p, aclTypeEnum, std::move(acl), ts);
	if (status == LIZARDFS_STATUS_OK) {
		gMetadata->metaversion++;
	}
	return status;
}

#ifndef METARESTORE
uint8_t fs_quota_get_all(uint8_t sesflags, uint32_t uid,
			std::vector<QuotaOwnerAndLimits> &results) {
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	results = gMetadata->gQuotaDatabase.getAll();
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_get(uint8_t sesflags, uint32_t uid, uint32_t gid,
		const std::vector<QuotaOwner> &owners,
		std::vector<QuotaOwnerAndLimits> &results) {
	std::vector<QuotaOwnerAndLimits> tmp;
	for (const QuotaOwner &owner : owners) {
		if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
			switch (owner.ownerType) {
			case QuotaOwnerType::kUser:
				if (uid != owner.ownerId) {
					return LIZARDFS_ERROR_EPERM;
				}
				break;
			case QuotaOwnerType::kGroup:
				if (gid != owner.ownerId && !(sesflags & SESFLAG_IGNOREGID)) {
					return LIZARDFS_ERROR_EPERM;
				}
				break;
			default:
				return LIZARDFS_ERROR_EINVAL;
			}
		}
		const QuotaLimits *result =
		        gMetadata->gQuotaDatabase.get(owner.ownerType, owner.ownerId);
		if (result) {
			tmp.emplace_back(owner, *result);
		}
	}
	results.swap(tmp);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_set(uint8_t sesflags, uint32_t uid, const std::vector<QuotaEntry> &entries) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	for (const QuotaEntry &entry : entries) {
		const QuotaOwner &owner = entry.entryKey.owner;
		gMetadata->gQuotaDatabase.set(entry.entryKey.rigor, entry.entryKey.resource,
		                              owner.ownerType, owner.ownerId, entry.limit);
		fs_changelog(ts, "SETQUOTA(%c,%c,%c,%" PRIu32 ",%" PRIu64 ")",
		             (entry.entryKey.rigor == QuotaRigor::kSoft) ? 'S' : 'H',
		             (entry.entryKey.resource == QuotaResource::kSize) ? 'S' : 'I',
		             (owner.ownerType == QuotaOwnerType::kUser) ? 'U' : 'G',
		             uint32_t{owner.ownerId}, uint64_t{entry.limit});
	}
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_apply_setquota(char rigor, char resource, char ownerType, uint32_t ownerId,
			uint64_t limit) {
	QuotaRigor quotaRigor = QuotaRigor::kSoft;
	QuotaResource quotaResource = QuotaResource::kSize;
	QuotaOwnerType quotaOwnerType = QuotaOwnerType::kUser;
	bool valid = true;
	valid &= decodeChar("SH", {QuotaRigor::kSoft, QuotaRigor::kHard}, rigor, quotaRigor);
	valid &= decodeChar("SI", {QuotaResource::kSize, QuotaResource::kInodes}, resource,
	                    quotaResource);
	valid &= decodeChar("UG", {QuotaOwnerType::kUser, QuotaOwnerType::kGroup}, ownerType,
	                    quotaOwnerType);
	if (!valid) {
		return LIZARDFS_ERROR_EINVAL;
	}
	gMetadata->metaversion++;
	gMetadata->gQuotaDatabase.set(quotaRigor, quotaResource, quotaOwnerType, ownerId, limit);
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint32_t fs_getdirpath_size(uint32_t inode) {
	fsnode *node;
	node = fsnodes_id_to_node(inode);
	if (node) {
		if (node->type != TYPE_DIRECTORY) {
			return 15;  // "(not directory)"
		} else {
			return 1 + fsnodes_getpath_size(node->parents);
		}
	} else {
		return 11;  // "(not found)"
	}
	return 0;  // unreachable
}

void fs_getdirpath_data(uint32_t inode, uint8_t *buff, uint32_t size) {
	fsnode *node;
	node = fsnodes_id_to_node(inode);
	if (node) {
		if (node->type != TYPE_DIRECTORY) {
			if (size >= 15) {
				memcpy(buff, "(not directory)", 15);
				return;
			}
		} else {
			if (size > 0) {
				buff[0] = '/';
				fsnodes_getpath_data(node->parents, buff + 1, size - 1);
				return;
			}
		}
	} else {
		if (size >= 11) {
			memcpy(buff, "(not found)", 11);
			return;
		}
	}
}

uint8_t fs_get_dir_stats(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t *inodes,
			uint32_t *dirs, uint32_t *files, uint32_t *chunks, uint64_t *length,
			uint64_t *size, uint64_t *rsize) {
	fsnode *p, *rn;
	statsrecord sr;
	(void)sesflags;
	if (rootinode == MFS_ROOT_ID || rootinode == 0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return LIZARDFS_ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return LIZARDFS_ERROR_EPERM;
			}
		}
	}
	if (p->type != TYPE_DIRECTORY && p->type != TYPE_FILE && p->type != TYPE_TRASH &&
	    p->type != TYPE_RESERVED) {
		return LIZARDFS_ERROR_EPERM;
	}
	fsnodes_get_stats(p, &sr);
	*inodes = sr.inodes;
	*dirs = sr.dirs;
	*files = sr.files;
	*chunks = sr.chunks;
	*length = sr.length;
	*size = sr.size;
	*rsize = sr.realsize;
	//      syslog(LOG_NOTICE,"using fast stats");
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_get_chunkid(const FsContext &context, uint32_t inode, uint32_t index,
			uint64_t *chunkid) {
	fsnode *p;
	uint8_t status = fsnodes_get_node_for_operation(context, ExpectedNodeType::kFile,
	                                                MODE_MASK_EMPTY, inode, &p);
	if (status != LIZARDFS_STATUS_OK) {
		return status;
	}
	if (index > MAX_INDEX) {
		return LIZARDFS_ERROR_INDEXTOOBIG;
	}
	if (index < p->data.fdata.chunks) {
		*chunkid = p->data.fdata.chunktab[index];
	} else {
		*chunkid = 0;
	}
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_add_tape_copy(const TapeKey &tapeKey, TapeserverId tapeserver) {
	fsnode *node = fsnodes_id_to_node(tapeKey.inode);
	if (node == NULL) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (node->type != TYPE_TRASH && node->type != TYPE_RESERVED && node->type != TYPE_FILE) {
		return LIZARDFS_ERROR_EINVAL;
	}
	if (node->mtime != tapeKey.mtime || node->data.fdata.length != tapeKey.fileLength) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	// Try to reuse an existing copy from this tapeserver
	auto &tapeCopies = gMetadata->tapeCopies[node->id];
	for (auto &tapeCopy : tapeCopies) {
		if (tapeCopy.server == tapeserver) {
			tapeCopy.state = TapeCopyState::kOk;
			return LIZARDFS_STATUS_OK;
		}
	}
	tapeCopies.emplace_back(TapeCopyState::kOk, tapeserver);
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_get_tape_copy_locations(uint32_t inode, std::vector<TapeCopyLocationInfo> &locations) {
	sassert(locations.empty());
	std::vector<TapeserverId> disconnectedTapeservers;
	fsnode *node = fsnodes_id_to_node(inode);
	if (node == NULL) {
		return LIZARDFS_ERROR_ENOENT;
	}
	auto it = gMetadata->tapeCopies.find(node->id);
	if (it == gMetadata->tapeCopies.end()) {
		return LIZARDFS_STATUS_OK;
	}
	for (auto &tapeCopy : it->second) {
		TapeserverListEntry tapeserverInfo;
		if (matotsserv_get_tapeserver_info(tapeCopy.server, tapeserverInfo) == LIZARDFS_STATUS_OK) {
			locations.emplace_back(tapeserverInfo, tapeCopy.state);
		} else {
			disconnectedTapeservers.push_back(tapeCopy.server);
		}
	}
	/* Lazy cleaning up of disconnected tapeservers */
	for (auto &tapeserverId : disconnectedTapeservers) {
		std::remove_if(it->second.begin(), it->second.end(),
		               [tapeserverId](const TapeCopy &copy) {
			               return copy.server == tapeserverId;
			       });
	}
	return LIZARDFS_STATUS_OK;
}
#endif

void fs_add_files_to_chunks() {
	uint32_t i, j;
	uint64_t chunkid;
	fsnode *f;
	for (i = 0; i < NODEHASHSIZE; i++) {
		for (f = gMetadata->nodehash[i]; f; f = f->next) {
			if (f->type == TYPE_FILE || f->type == TYPE_TRASH ||
			    f->type == TYPE_RESERVED) {
				for (j = 0; j < f->data.fdata.chunks; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid > 0) {
						chunk_add_file(chunkid, f->goal);
					}
				}
			}
		}
	}
}

uint64_t fs_getversion() {
	if (!gMetadata) {
		throw NoMetadataException();
	}
	return gMetadata->metaversion;
}

#ifndef METARESTORE
const GoalMap<Goal> &fs_get_goal_definitions() {
	return gGoalDefinitions;
}

const Goal &fs_get_goal_definition(uint8_t goalId) {
	return gGoalDefinitions[goalId];
}

#endif
