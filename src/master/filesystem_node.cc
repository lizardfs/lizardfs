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
#include "filesystem_node.h"

#include <cstdint>
#include <cstdlib>

#include "common/attributes.h"
#include "common/massert.h"
#include "master/chunks.h"
#include "master/datacachemgr.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_freenode.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_operations.h"
#include "master/fs_context.h"

#define LOOKUPNOHASHLIMIT 10

#define MAXFNAMELENG 255

// number of blocks in the last chunk before EOF
static uint32_t last_chunk_blocks(fsnode *node) {
	const uint64_t last_byte = node->data.fdata.length - 1;
	const uint32_t last_byte_offset = last_byte % MFSCHUNKSIZE;
	const uint32_t last_block = last_byte_offset / MFSBLOCKSIZE;
	const uint32_t block_count = last_block + 1;
	return block_count;
}

// does the last chunk exist and contain non-zero data?
static bool last_chunk_nonempty(fsnode *node) {
	const uint32_t chunks = node->data.fdata.chunks;
	if (chunks == 0) {
		// no non-zero chunks, return now
		return false;
	}
	// file has non-zero length and contains at least one chunk
	const uint64_t last_byte = node->data.fdata.length - 1;
	const uint32_t last_chunk = last_byte / MFSCHUNKSIZE;
	if (last_chunk < chunks) {
		// last chunk exists, check if it isn't the zero chunk
		return (node->data.fdata.chunktab[last_chunk] != 0);
	} else {
		// last chunk hasn't been allocated yet
		return false;
	}
}

// count chunks in a file, disregard sparse file holes
static uint32_t file_chunks(fsnode *node) {
	uint32_t count = 0;
	for (uint64_t i = 0; i < node->data.fdata.chunks; i++) {
		if (node->data.fdata.chunktab[i] != 0) {
			count++;
		}
	}
	return count;
}

// compute the "size" statistic for a file node
static uint64_t file_size(fsnode *node, uint32_t nonzero_chunks) {
	uint64_t size = (uint64_t)nonzero_chunks * (MFSCHUNKSIZE + MFSHDRSIZE);
	if (last_chunk_nonempty(node)) {
		size -= MFSCHUNKSIZE;
		size += last_chunk_blocks(node) * MFSBLOCKSIZE;
	}
	return size;
}

// compute the disk space cost of all parts of a xor chunk of given size
static uint32_t xor_chunk_realsize(uint32_t blocks, uint32_t level) {
	const uint32_t stripes = (blocks + level - 1) / level;
	uint32_t size = blocks * MFSBLOCKSIZE;  // file data
	size += stripes * MFSBLOCKSIZE;         // parity data
	size += 4096 * (level + 1);             // headers of data and parity parts
	return size;
}

// compute the "realsize" statistic for a file node
static uint64_t file_realsize(fsnode *node, uint32_t nonzero_chunks, uint64_t file_size) {
	const uint8_t goal = node->goal;
	if (goal::isOrdinaryGoal(goal)) {
#ifdef METARESTORE
		return file_size;  // Doesn't really matter. Metarestore doesn't need this value
		                   // anyway.
#else
		return file_size * gGoalDefinitions[goal].getExpectedCopies();
#endif
	}
	if (goal::isXorGoal(goal)) {
		const ChunkType::XorLevel level = goal::toXorLevel(goal);
		const uint32_t full_chunk_realsize = xor_chunk_realsize(MFSBLOCKSINCHUNK, level);
		uint64_t size = (uint64_t)nonzero_chunks * full_chunk_realsize;
		if (last_chunk_nonempty(node)) {
			size -= full_chunk_realsize;
			size += xor_chunk_realsize(last_chunk_blocks(node), level);
		}
		return size;
	}
	syslog(LOG_ERR, "file_realsize: inode %" PRIu32 " has unknown goal 0x%" PRIx8, node->id,
	       node->goal);
	return 0;
}

char *fsnodes_escape_name(uint32_t nleng, const uint8_t *name) {
	static char *escname[2] = {NULL, NULL};
	static uint32_t escnamesize[2] = {0, 0};
	static uint8_t buffid = 0;
	char *currescname = NULL;
	uint32_t i;
	uint8_t c;
	buffid = 1 - buffid;
	i = nleng;
	i = i * 3 + 1;
	if (i > escnamesize[buffid] || i == 0) {
		escnamesize[buffid] = ((i / 1000) + 1) * 1000;
		if (escname[buffid] != NULL) {
			free(escname[buffid]);
		}
		escname[buffid] = (char *)malloc(escnamesize[buffid]);
		passert(escname[buffid]);
	}
	i = 0;
	currescname = escname[buffid];
	passert(currescname);
	while (nleng > 0) {
		c = *name;
		if (c < 32 || c >= 127 || c == ',' || c == '%' || c == '(' || c == ')') {
			currescname[i++] = '%';
			currescname[i++] = "0123456789ABCDEF"[(c >> 4) & 0xF];
			currescname[i++] = "0123456789ABCDEF"[c & 0xF];
		} else {
			currescname[i++] = c;
		}
		name++;
		nleng--;
	}
	currescname[i] = 0;
	return currescname;
}

int fsnodes_nameisused(fsnode *node, uint16_t nleng, const uint8_t *name) {
	fsedge *ei;
	if (node->data.ddata.elements > LOOKUPNOHASHLIMIT) {
		ei = gMetadata->edgehash[EDGEHASHPOS(fsnodes_hash(node->id, nleng, name))];
		while (ei) {
			if (ei->parent == node && nleng == ei->nleng &&
			    memcmp((char *)(ei->name), (char *)name, nleng) == 0) {
				return 1;
			}
			ei = ei->next;
		}
	} else {
		ei = node->data.ddata.children;
		while (ei) {
			if (nleng == ei->nleng &&
			    memcmp((char *)(ei->name), (char *)name, nleng) == 0) {
				return 1;
			}
			ei = ei->nextchild;
		}
	}
	return 0;
}

/// searches for an edge with given name (`name`) in given directory (`node`)
fsedge *fsnodes_lookup(fsnode *node, uint16_t nleng, const uint8_t *name) {
	fsedge *ei;

	if (node->type != TYPE_DIRECTORY) {
		return NULL;
	}
	if (node->data.ddata.elements > LOOKUPNOHASHLIMIT) {
		ei = gMetadata->edgehash[EDGEHASHPOS(fsnodes_hash(node->id, nleng, name))];
		while (ei) {
			if (ei->parent == node && nleng == ei->nleng &&
			    memcmp((char *)(ei->name), (char *)name, nleng) == 0) {
				return ei;
			}
			ei = ei->next;
		}
	} else {
		ei = node->data.ddata.children;
		while (ei) {
			if (nleng == ei->nleng &&
			    memcmp((char *)(ei->name), (char *)name, nleng) == 0) {
				return ei;
			}
			ei = ei->nextchild;
		}
	}
	return NULL;
}

fsnode *fsnodes_id_to_node(uint32_t id) {
	fsnode *p;
	uint32_t nodepos = NODEHASHPOS(id);
	for (p = gMetadata->nodehash[nodepos]; p; p = p->next) {
		if (p->id == id) {
			return p;
		}
	}
	return NULL;
}

// returns true iff f is ancestor of p
bool fsnodes_isancestor(fsnode *f, fsnode *p) {
	fsedge *e;
	for (e = p->parents; e; e = e->nextparent) {  // check all parents of 'p' because 'p' can be
		                                      // any object, so it can be hardlinked
		p = e->parent;  // warning !!! since this point 'p' is used as temporary variable
		while (p) {
			if (f == p) {
				return 1;
			}
			if (p->parents) {
				p = p->parents->parent;  // here 'p' is always a directory so it
				                         // should have only one parent
			} else {
				p = NULL;
			}
		}
	}
	return 0;
}

// returns true iff p is reserved or in trash or f is ancestor of p
bool fsnodes_isancestor_or_node_reserved_or_trash(fsnode *f, fsnode *p) {
	// Return 1 if file is reservered:
	if (p && (p->type == TYPE_RESERVED || p->type == TYPE_TRASH)) {
		return 1;
	}
	// Or if f is ancestor of p
	return fsnodes_isancestor(f, p);
}

// quota

bool fsnodes_inode_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gMetadata->gQuotaDatabase.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, uid,
	                                            gid);
}

bool fsnodes_size_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gMetadata->gQuotaDatabase.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, uid,
	                                            gid);
}

void fsnodes_quota_register_inode(fsnode *node) {
	gMetadata->gQuotaDatabase.changeUsage(QuotaResource::kInodes, node->uid, node->gid, +1);
}

static void fsnodes_quota_unregister_inode(fsnode *node) {
	gMetadata->gQuotaDatabase.changeUsage(QuotaResource::kInodes, node->uid, node->gid, -1);
}

void fsnodes_quota_update_size(fsnode *node, int64_t delta) {
	if (delta != 0) {
		gMetadata->gQuotaDatabase.changeUsage(QuotaResource::kSize, node->uid, node->gid,
		                                      delta);
	}
}

// stats

void fsnodes_get_stats(fsnode *node, statsrecord *sr) {
	switch (node->type) {
	case TYPE_DIRECTORY:
		*sr = *(node->data.ddata.stats);
		sr->inodes++;
		sr->dirs++;
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		sr->inodes = 1;
		sr->dirs = 0;
		sr->files = 1;
		sr->chunks = file_chunks(node);
		sr->length = node->data.fdata.length;
		sr->size = file_size(node, sr->chunks);
		sr->realsize = file_realsize(node, sr->chunks, sr->size);
		break;
	case TYPE_SYMLINK:
		sr->inodes = 1;
		sr->files = 0;
		sr->dirs = 0;
		sr->chunks = 0;
		sr->length = node->data.sdata.pleng;
		sr->size = 0;
		sr->realsize = 0;
		break;
	default:
		sr->inodes = 1;
		sr->files = 0;
		sr->dirs = 0;
		sr->chunks = 0;
		sr->length = 0;
		sr->size = 0;
		sr->realsize = 0;
	}
}

int64_t fsnodes_get_size(fsnode *node) {
	statsrecord sr;
	fsnodes_get_stats(node, &sr);
	return sr.size;
}

static inline void fsnodes_sub_stats(fsnode *parent, statsrecord *sr) {
	statsrecord *psr;
	fsedge *e;
	if (parent) {
		psr = parent->data.ddata.stats;
		psr->inodes -= sr->inodes;
		psr->dirs -= sr->dirs;
		psr->files -= sr->files;
		psr->chunks -= sr->chunks;
		psr->length -= sr->length;
		psr->size -= sr->size;
		psr->realsize -= sr->realsize;
		if (parent != gMetadata->root) {
			for (e = parent->parents; e; e = e->nextparent) {
				fsnodes_sub_stats(e->parent, sr);
			}
		}
	}
}

void fsnodes_add_stats(fsnode *parent, statsrecord *sr) {
	statsrecord *psr;
	fsedge *e;
	if (parent) {
		psr = parent->data.ddata.stats;
		psr->inodes += sr->inodes;
		psr->dirs += sr->dirs;
		psr->files += sr->files;
		psr->chunks += sr->chunks;
		psr->length += sr->length;
		psr->size += sr->size;
		psr->realsize += sr->realsize;
		if (parent != gMetadata->root) {
			for (e = parent->parents; e; e = e->nextparent) {
				fsnodes_add_stats(e->parent, sr);
			}
		}
	}
}

void fsnodes_add_sub_stats(fsnode *parent, statsrecord *newsr, statsrecord *prevsr) {
	statsrecord sr;
	sr.inodes = newsr->inodes - prevsr->inodes;
	sr.dirs = newsr->dirs - prevsr->dirs;
	sr.files = newsr->files - prevsr->files;
	sr.chunks = newsr->chunks - prevsr->chunks;
	sr.length = newsr->length - prevsr->length;
	sr.size = newsr->size - prevsr->size;
	sr.realsize = newsr->realsize - prevsr->realsize;
	fsnodes_add_stats(parent, &sr);
}

void fsnodes_fill_attr(fsnode *node, fsnode *parent, uint32_t uid, uint32_t gid, uint32_t auid,
			uint32_t agid, uint8_t sesflags, Attributes &attr) {
#ifdef METARESTORE
	mabort("Bad code path - fsnodes_fill_attr() shall not be executed in metarestore context.");
#endif /* METARESTORE */
	uint8_t *ptr;
	uint16_t mode;
	uint32_t nlink;
	fsedge *e;
	(void)sesflags;
	ptr = attr;
	if (node->type == TYPE_TRASH || node->type == TYPE_RESERVED) {
		put8bit(&ptr, TYPE_FILE);
	} else {
		put8bit(&ptr, node->type);
	}
	mode = node->mode & 07777;
	if (parent) {
		if (parent->mode & (EATTR_NOECACHE << 12)) {
			mode |= (MATTR_NOECACHE << 12);
		}
	}
	if ((node->mode & ((EATTR_NOOWNER | EATTR_NOACACHE) << 12)) ||
	    (sesflags & SESFLAG_MAPALL)) {
		mode |= (MATTR_NOACACHE << 12);
	}
	if ((node->mode & (EATTR_NODATACACHE << 12)) == 0) {
		mode |= (MATTR_ALLOWDATACACHE << 12);
	}
	put16bit(&ptr, mode);
	if ((node->mode & (EATTR_NOOWNER << 12)) && uid != 0) {
		if (sesflags & SESFLAG_MAPALL) {
			put32bit(&ptr, auid);
			put32bit(&ptr, agid);
		} else {
			put32bit(&ptr, uid);
			put32bit(&ptr, gid);
		}
	} else {
		if (sesflags & SESFLAG_MAPALL && auid != 0) {
			if (node->uid == uid) {
				put32bit(&ptr, auid);
			} else {
				put32bit(&ptr, 0);
			}
			if (node->gid == gid) {
				put32bit(&ptr, agid);
			} else {
				put32bit(&ptr, 0);
			}
		} else {
			put32bit(&ptr, node->uid);
			put32bit(&ptr, node->gid);
		}
	}
	put32bit(&ptr, node->atime);
	put32bit(&ptr, node->mtime);
	put32bit(&ptr, node->ctime);
	nlink = 0;
	for (e = node->parents; e; e = e->nextparent) {
		nlink++;
	}
	switch (node->type) {
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		put32bit(&ptr, nlink);
		put64bit(&ptr, node->data.fdata.length);
		break;
	case TYPE_DIRECTORY:
		put32bit(&ptr, node->data.ddata.nlink);
		put64bit(&ptr, node->data.ddata.stats->length >>
		                       30);  // Rescale length to GB (reduces size to 32-bit length)
		break;
	case TYPE_SYMLINK:
		put32bit(&ptr, nlink);
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		put32bit(&ptr, node->data.sdata.pleng);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr, nlink);
		put32bit(&ptr, node->data.devdata.rdev);
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		break;
	default:
		put32bit(&ptr, nlink);
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
		*ptr++ = 0;
	}
}

void fsnodes_fill_attr(const FsContext &context, fsnode *node, fsnode *parent, Attributes &attr) {
#ifdef METARESTORE
	mabort("Bad code path - fsnodes_fill_attr() shall not be executed in metarestore context.");
#endif /* METARESTORE */
	sassert(context.hasSessionData() && context.hasUidGidData());
	fsnodes_fill_attr(node, parent, context.uid(), context.gid(), context.auid(),
	                  context.agid(), context.sesflags(), attr);
}

void fsnodes_remove_edge(uint32_t ts, fsedge *e) {
	statsrecord sr;
	if (gChecksumBackgroundUpdater.isEdgeIncluded(e)) {
		removeFromChecksum(gChecksumBackgroundUpdater.fsEdgesChecksum, e->checksum);
	}
	removeFromChecksum(gMetadata->fsEdgesChecksum, e->checksum);
	if (e->parent) {
		fsnodes_get_stats(e->child, &sr);
		fsnodes_sub_stats(e->parent, &sr);
		e->parent->mtime = e->parent->ctime = ts;
		e->parent->data.ddata.elements--;
		if (e->child->type == TYPE_DIRECTORY) {
			e->parent->data.ddata.nlink--;
		}
		fsnodes_update_checksum(e->parent);
	}
	if (e->child) {
		e->child->ctime = ts;
		fsnodes_update_checksum(e->child);
	}
	*(e->prevchild) = e->nextchild;
	if (e->nextchild) {
		e->nextchild->prevchild = e->prevchild;
	}
	*(e->prevparent) = e->nextparent;
	if (e->nextparent) {
		e->nextparent->prevparent = e->prevparent;
	}
	if (e->prev) {
		*(e->prev) = e->next;
		if (e->next) {
			e->next->prev = e->prev;
		}
	}
	delete e;
}

void fsnodes_link(uint32_t ts, fsnode *parent, fsnode *child, uint16_t nleng, const uint8_t *name) {
	fsedge *e;
	statsrecord sr;
	uint32_t hpos;

	e = new fsedge;
	e->nleng = nleng;
	e->name = (uint8_t *)malloc(nleng);
	passert(e->name);
	memcpy(e->name, name, nleng);
	e->child = child;
	e->parent = parent;
	e->nextchild = parent->data.ddata.children;
	if (e->nextchild) {
		e->nextchild->prevchild = &(e->nextchild);
	}
	parent->data.ddata.children = e;
	e->prevchild = &(parent->data.ddata.children);
	e->nextparent = child->parents;
	if (e->nextparent) {
		e->nextparent->prevparent = &(e->nextparent);
	}
	child->parents = e;
	e->prevparent = &(child->parents);
	hpos = EDGEHASHPOS(fsnodes_hash(parent->id, nleng, name));
	e->next = gMetadata->edgehash[hpos];
	if (e->next) {
		e->next->prev = &(e->next);
	}
	gMetadata->edgehash[hpos] = e;
	e->prev = &(gMetadata->edgehash[hpos]);
	e->checksum = 0;
	fsedges_update_checksum(e);

	parent->data.ddata.elements++;
	if (child->type == TYPE_DIRECTORY) {
		parent->data.ddata.nlink++;
	}
	fsnodes_get_stats(child, &sr);
	fsnodes_add_stats(parent, &sr);
	if (ts > 0) {
		parent->mtime = parent->ctime = ts;
		fsnodes_update_checksum(parent);
		child->ctime = ts;
		fsnodes_update_checksum(child);
	}
}

fsnode *fsnodes_create_node(uint32_t ts, fsnode *node, uint16_t nleng, const uint8_t *name,
			uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid,
			uint8_t copysgid, AclInheritance inheritacl, uint32_t req_inode) {
	fsnode *p;
	statsrecord *sr;
	uint32_t nodepos;
	p = new fsnode(type);
	gMetadata->nodes++;
	if (type == TYPE_DIRECTORY) {
		gMetadata->dirnodes++;
	}
	if (type == TYPE_FILE) {
		gMetadata->filenodes++;
	}
	/* create node */
	p->id = fsnodes_get_next_id(ts, req_inode);

	p->ctime = p->mtime = p->atime = ts;
	if (type == TYPE_DIRECTORY || type == TYPE_FILE) {
		p->goal = node->goal;
		p->trashtime = node->trashtime;
	} else {
		p->goal = DEFAULT_GOAL;
		p->trashtime = DEFAULT_TRASHTIME;
	}
	if (type == TYPE_DIRECTORY) {
		p->mode = (mode & 07777) | (node->mode & 0xF000);
	} else {
		p->mode = (mode & 07777) | (node->mode & (0xF000 & (~(EATTR_NOECACHE << 12))));
	}
	// If desired, node inherits permissions from parent's default ACL
	if (inheritacl == AclInheritance::kInheritAcl && node->defaultAcl) {
		if (p->type == TYPE_DIRECTORY) {
			p->defaultAcl.reset(new AccessControlList(*node->defaultAcl));
		}
		// Join ACL's access mask without cleaning sticky bits etc.
		p->mode &= ~0777 | (node->defaultAcl->mode);
		if (node->defaultAcl->extendedAcl) {
			p->extendedAcl.reset(new ExtendedAcl(*node->defaultAcl->extendedAcl));
		}
	} else {
		// Apply umask
		p->mode &= ~(umask & 0777);  // umask must be applied manually
	}
	p->uid = uid;
	if ((node->mode & 02000) == 02000) {  // set gid flag is set in the parent directory ?
		p->gid = node->gid;
		if (copysgid && type == TYPE_DIRECTORY) {
			p->mode |= 02000;
		}
	} else {
		p->gid = gid;
	}
	switch (type) {
	case TYPE_DIRECTORY:
		sr = (statsrecord *)malloc(sizeof(statsrecord));
		passert(sr);
		memset(sr, 0, sizeof(statsrecord));
		p->data.ddata.stats = sr;
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_FILE:
		p->data.fdata.length = 0;
		p->data.fdata.chunks = 0;
		p->data.fdata.chunktab = NULL;
		p->data.fdata.sessionids = NULL;
		break;
	case TYPE_SYMLINK:
		p->data.sdata.pleng = 0;
		p->data.sdata.path = NULL;
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = 0;
	}
	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = p;
	p->checksum = 0;
	fsnodes_update_checksum(p);
	fsnodes_link(ts, node, p, nleng, name);
	fsnodes_quota_register_inode(p);
	if (type == TYPE_FILE) {
		fsnodes_quota_update_size(p, +fsnodes_get_size(p));
	}
	return p;
}

#ifndef METARESTORE
uint32_t fsnodes_getpath_size(fsedge *e) {
	uint32_t size;
	fsnode *p;
	if (e == NULL) {
		return 0;
	}
	p = e->parent;
	size = e->nleng;
	while (p != gMetadata->root && p->parents) {
		size += p->parents->nleng + 1;
		p = p->parents->parent;
	}
	return size;
}

void fsnodes_getpath_data(fsedge *e, uint8_t *path, uint32_t size) {
	fsnode *p;
	if (e == NULL) {
		return;
	}
	if (size >= e->nleng) {
		size -= e->nleng;
		memcpy(path + size, e->name, e->nleng);
	} else if (size > 0) {
		memcpy(path, e->name + (e->nleng - size), size);
		size = 0;
	}
	if (size > 0) {
		path[--size] = '/';
	}
	p = e->parent;
	while (p != gMetadata->root && p->parents) {
		if (size >= p->parents->nleng) {
			size -= p->parents->nleng;
			memcpy(path + size, p->parents->name, p->parents->nleng);
		} else if (size > 0) {
			memcpy(path, p->parents->name + (p->parents->nleng - size), size);
			size = 0;
		}
		if (size > 0) {
			path[--size] = '/';
		}
		p = p->parents->parent;
	}
}
#endif /* METARESTORE */

void fsnodes_getpath(fsedge *e, uint16_t *pleng, uint8_t **path) {
	uint32_t size;
	uint8_t *ret;
	fsnode *p;

	p = e->parent;
	size = e->nleng;
	while (p != gMetadata->root && p->parents) {
		size += p->parents->nleng + 1;  // get first parent !!!
		p = p->parents->parent;  // when folders can be hardlinked it's the only way to
		                         // obtain path (one of them)
	}
	if (size > 65535) {
		syslog(LOG_WARNING, "path too long !!! - truncate");
		size = 65535;
	}
	*pleng = size;
	ret = (uint8_t *)malloc(size);
	passert(ret);
	size -= e->nleng;
	memcpy(ret + size, e->name, e->nleng);
	if (size > 0) {
		ret[--size] = '/';
	}
	p = e->parent;
	while (p != gMetadata->root && p->parents) {
		if (size >= p->parents->nleng) {
			size -= p->parents->nleng;
			memcpy(ret + size, p->parents->name, p->parents->nleng);
		} else {
			if (size > 0) {
				memcpy(ret, p->parents->name + (p->parents->nleng - size), size);
				size = 0;
			}
		}
		if (size > 0) {
			ret[--size] = '/';
		}
		p = p->parents->parent;
	}
	*path = ret;
}

#ifndef METARESTORE

uint32_t fsnodes_getdetachedsize(fsedge *start) {
	fsedge *e;
	uint32_t result = 0;
	for (e = start; e; e = e->nextchild) {
		if (e->nleng > 240) {
			result += 245;
		} else {
			result += 5 + e->nleng;
		}
	}
	return result;
}

void fsnodes_getdetacheddata(fsedge *start, uint8_t *dbuff) {
	fsedge *e;
	uint8_t *sptr;
	uint8_t c;
	for (e = start; e; e = e->nextchild) {
		if (e->nleng > 240) {
			*dbuff = 240;
			dbuff++;
			memcpy(dbuff, "(...)", 5);
			dbuff += 5;
			sptr = e->name + (e->nleng - 235);
			for (c = 0; c < 235; c++) {
				if (*sptr == '/') {
					*dbuff = '|';
				} else {
					*dbuff = *sptr;
				}
				sptr++;
				dbuff++;
			}
		} else {
			*dbuff = e->nleng;
			dbuff++;
			sptr = e->name;
			for (c = 0; c < e->nleng; c++) {
				if (*sptr == '/') {
					*dbuff = '|';
				} else {
					*dbuff = *sptr;
				}
				sptr++;
				dbuff++;
			}
		}
		put32bit(&dbuff, e->child->id);
	}
}

uint32_t fsnodes_getdirsize(fsnode *p, uint8_t withattr) {
	uint32_t result = ((withattr) ? 40 : 6) * 2 + 3;  // for '.' and '..'
	fsedge *e;
	for (e = p->data.ddata.children; e; e = e->nextchild) {
		result += ((withattr) ? 40 : 6) + e->nleng;
	}
	return result;
}

void fsnodes_getdirdata(uint32_t rootinode, uint32_t uid, uint32_t gid, uint32_t auid,
			uint32_t agid, uint8_t sesflags, fsnode *p, uint8_t *dbuff,
			uint8_t withattr) {
	fsedge *e;
	// '.' - self
	dbuff[0] = 1;
	dbuff[1] = '.';
	dbuff += 2;
	if (p->id != rootinode) {
		put32bit(&dbuff, p->id);
	} else {
		put32bit(&dbuff, MFS_ROOT_ID);
	}
	Attributes attr;
	if (withattr) {
		fsnodes_fill_attr(p, p, uid, gid, auid, agid, sesflags, attr);
		::memcpy(dbuff, attr, sizeof(attr));
		dbuff += sizeof(attr);
	} else {
		put8bit(&dbuff, TYPE_DIRECTORY);
	}
	// '..' - parent
	dbuff[0] = 2;
	dbuff[1] = '.';
	dbuff[2] = '.';
	dbuff += 3;
	if (p->id == rootinode) {  // root node should returns self as its parent
		put32bit(&dbuff, MFS_ROOT_ID);
		if (withattr) {
			fsnodes_fill_attr(p, p, uid, gid, auid, agid, sesflags, attr);
			::memcpy(dbuff, attr, sizeof(attr));
			dbuff += sizeof(attr);
		} else {
			put8bit(&dbuff, TYPE_DIRECTORY);
		}
	} else {
		if (p->parents && p->parents->parent->id != rootinode) {
			put32bit(&dbuff, p->parents->parent->id);
		} else {
			put32bit(&dbuff, MFS_ROOT_ID);
		}
		if (withattr) {
			if (p->parents) {
				fsnodes_fill_attr(p->parents->parent, p, uid, gid, auid, agid,
				                  sesflags, attr);
				::memcpy(dbuff, attr, sizeof(attr));
			} else {
				if (rootinode == MFS_ROOT_ID) {
					fsnodes_fill_attr(gMetadata->root, p, uid, gid, auid, agid,
					                  sesflags, attr);
					::memcpy(dbuff, attr, sizeof(attr));
				} else {
					fsnode *rn = fsnodes_id_to_node(rootinode);
					if (rn) {  // it should be always true because it's checked
						   // before, but better check than sorry
						fsnodes_fill_attr(rn, p, uid, gid, auid, agid,
						                  sesflags, attr);
						::memcpy(dbuff, attr, sizeof(attr));
					} else {
						memset(dbuff, 0, sizeof(attr));
					}
				}
			}
			dbuff += sizeof(attr);
		} else {
			put8bit(&dbuff, TYPE_DIRECTORY);
		}
	}
	// entries
	for (e = p->data.ddata.children; e; e = e->nextchild) {
		dbuff[0] = e->nleng;
		dbuff++;
		memcpy(dbuff, e->name, e->nleng);
		dbuff += e->nleng;
		put32bit(&dbuff, e->child->id);
		if (withattr) {
			fsnodes_fill_attr(e->child, p, uid, gid, auid, agid, sesflags, attr);
			::memcpy(dbuff, attr, sizeof(attr));
			dbuff += sizeof(attr);
		} else {
			put8bit(&dbuff, e->child->type);
		}
	}
}

void fsnodes_checkfile(fsnode *p, uint32_t chunkcount[CHUNK_MATRIX_SIZE]) {
	uint64_t chunkid;
	uint8_t count;
	for (int i = 0; i < CHUNK_MATRIX_SIZE; i++) {
		chunkcount[i] = 0;
	}
	for (uint32_t index = 0; index < p->data.fdata.chunks; index++) {
		chunkid = p->data.fdata.chunktab[index];
		if (chunkid > 0) {
			chunk_get_validcopies(chunkid, &count);
			if (count > CHUNK_MATRIX_SIZE - 1) {
				count = CHUNK_MATRIX_SIZE - 1;
			}
			chunkcount[count]++;
		}
	}
}
#endif

uint8_t fsnodes_appendchunks(uint32_t ts, fsnode *dstobj, fsnode *srcobj) {
	uint64_t chunkid, length;
	uint32_t i;
	uint32_t srcchunks, dstchunks;
	statsrecord psr, nsr;

	srcchunks = 0;
	for (i = 0; i < srcobj->data.fdata.chunks; i++) {
		if (srcobj->data.fdata.chunktab[i] != 0) {
			srcchunks = i + 1;
		}
	}
	if (srcchunks == 0) {
		return LIZARDFS_STATUS_OK;
	}
	dstchunks = 0;
	for (i = 0; i < dstobj->data.fdata.chunks; i++) {
		if (dstobj->data.fdata.chunktab[i] != 0) {
			dstchunks = i + 1;
		}
	}
	i = srcchunks + dstchunks - 1;  // last new chunk pos
	if (i > MAX_INDEX) {            // chain too long
		return LIZARDFS_ERROR_INDEXTOOBIG;
	}
	fsnodes_get_stats(dstobj, &psr);
	if (i >= dstobj->data.fdata.chunks) {
		uint32_t newsize;
		if (i < 8) {
			newsize = i + 1;
		} else if (i < 64) {
			newsize = (i & 0xFFFFFFF8) + 8;
		} else {
			newsize = (i & 0xFFFFFFC0) + 64;
		}
		if (dstobj->data.fdata.chunktab == NULL) {
			dstobj->data.fdata.chunktab =
			        (uint64_t *)malloc(sizeof(uint64_t) * newsize);
		} else {
			dstobj->data.fdata.chunktab = (uint64_t *)realloc(
			        dstobj->data.fdata.chunktab, sizeof(uint64_t) * newsize);
		}
		passert(dstobj->data.fdata.chunktab);
		for (i = dstobj->data.fdata.chunks; i < newsize; i++) {
			dstobj->data.fdata.chunktab[i] = 0;
		}
		dstobj->data.fdata.chunks = newsize;
	}

	for (i = 0; i < srcchunks; i++) {
		chunkid = srcobj->data.fdata.chunktab[i];
		dstobj->data.fdata.chunktab[i + dstchunks] = chunkid;
		if (chunkid > 0) {
			if (chunk_add_file(chunkid, dstobj->goal) != LIZARDFS_STATUS_OK) {
				syslog(LOG_ERR,
				       "structure error - chunk %016" PRIX64
				       " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",
				       chunkid, srcobj->id, i);
			}
		}
	}

	length = (((uint64_t)dstchunks) << MFSCHUNKBITS) + srcobj->data.fdata.length;
	if (dstobj->type == TYPE_TRASH) {
		gMetadata->trashspace -= dstobj->data.fdata.length;
		gMetadata->trashspace += length;
	} else if (dstobj->type == TYPE_RESERVED) {
		gMetadata->reservedspace -= dstobj->data.fdata.length;
		gMetadata->reservedspace += length;
	}
	dstobj->data.fdata.length = length;
	fsnodes_get_stats(dstobj, &nsr);
	fsnodes_quota_update_size(dstobj, nsr.size - psr.size);
	for (fsedge *e = dstobj->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	dstobj->mtime = ts;
	dstobj->atime = ts;
	srcobj->atime = ts;
	fsnodes_update_checksum(srcobj);
	fsnodes_update_checksum(dstobj);
	return LIZARDFS_STATUS_OK;
}

static inline void fsnodes_changefilegoal(fsnode *obj, uint8_t goal) {
	uint32_t i;
	uint8_t old_goal = obj->goal;
	statsrecord psr, nsr;
	fsedge *e;

	fsnodes_get_stats(obj, &psr);
	obj->goal = goal;
	nsr = psr;
	nsr.realsize = file_realsize(obj, nsr.chunks, nsr.size);
	for (e = obj->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	for (i = 0; i < obj->data.fdata.chunks; i++) {
		if (obj->data.fdata.chunktab[i] > 0) {
			chunk_change_file(obj->data.fdata.chunktab[i], old_goal, goal);
		}
	}
	fsnodes_update_checksum(obj);
}

void fsnodes_setlength(fsnode *obj, uint64_t length) {
	uint32_t i, chunks;
	uint64_t chunkid;
	statsrecord psr, nsr;
	fsnodes_get_stats(obj, &psr);
	if (obj->type == TYPE_TRASH) {
		gMetadata->trashspace -= obj->data.fdata.length;
		gMetadata->trashspace += length;
	} else if (obj->type == TYPE_RESERVED) {
		gMetadata->reservedspace -= obj->data.fdata.length;
		gMetadata->reservedspace += length;
	}
	obj->data.fdata.length = length;
	if (length > 0) {
		chunks = ((length - 1) >> MFSCHUNKBITS) + 1;
	} else {
		chunks = 0;
	}
	for (i = chunks; i < obj->data.fdata.chunks; i++) {
		chunkid = obj->data.fdata.chunktab[i];
		if (chunkid > 0) {
			if (chunk_delete_file(chunkid, obj->goal) != LIZARDFS_STATUS_OK) {
				syslog(LOG_ERR,
				       "structure error - chunk %016" PRIX64
				       " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",
				       chunkid, obj->id, i);
			}
		}
		obj->data.fdata.chunktab[i] = 0;
	}
	if (chunks > 0) {
		if (chunks < obj->data.fdata.chunks && obj->data.fdata.chunktab) {
			obj->data.fdata.chunktab = (uint64_t *)realloc(obj->data.fdata.chunktab,
			                                               sizeof(uint64_t) * chunks);
			passert(obj->data.fdata.chunktab);
			obj->data.fdata.chunks = chunks;
		}
	} else {
		if (obj->data.fdata.chunks > 0 && obj->data.fdata.chunktab) {
			free(obj->data.fdata.chunktab);
			obj->data.fdata.chunktab = NULL;
			obj->data.fdata.chunks = 0;
		}
	}
	fsnodes_get_stats(obj, &nsr);
	fsnodes_quota_update_size(obj, nsr.size - psr.size);
	for (fsedge *e = obj->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	fsnodes_update_checksum(obj);
}

void fsnodes_change_uid_gid(fsnode *p, uint32_t uid, uint32_t gid) {
	int64_t size = 0;
	fsnodes_quota_unregister_inode(p);
	if (p->type == TYPE_FILE || p->type == TYPE_TRASH || p->type == TYPE_RESERVED) {
		size = fsnodes_get_size(p);
		fsnodes_quota_update_size(p, -size);
	}
	p->uid = uid;
	p->gid = gid;
	fsnodes_quota_register_inode(p);
	if (p->type == TYPE_FILE || p->type == TYPE_TRASH || p->type == TYPE_RESERVED) {
		fsnodes_quota_update_size(p, +size);
	}
}

static inline void fsnodes_remove_node(uint32_t ts, fsnode *toremove) {
	uint32_t nodepos;
	fsnode **ptr;
	if (toremove->parents != NULL) {
		return;
	}
	// remove from idhash
	nodepos = NODEHASHPOS(toremove->id);
	ptr = &(gMetadata->nodehash[nodepos]);
	while (*ptr) {
		if (*ptr == toremove) {
			*ptr = toremove->next;
			break;
		}
		ptr = &((*ptr)->next);
	}
	if (gChecksumBackgroundUpdater.isNodeIncluded(toremove)) {
		removeFromChecksum(gChecksumBackgroundUpdater.fsNodesChecksum, toremove->checksum);
	}
	removeFromChecksum(gMetadata->fsNodesChecksum, toremove->checksum);
	// and free
	gMetadata->nodes--;
	if (toremove->type == TYPE_DIRECTORY) {
		gMetadata->dirnodes--;
	}
	if (toremove->type == TYPE_FILE || toremove->type == TYPE_TRASH ||
	    toremove->type == TYPE_RESERVED) {
		uint32_t i;
		uint64_t chunkid;
		fsnodes_quota_update_size(toremove, -fsnodes_get_size(toremove));
		gMetadata->filenodes--;
		for (i = 0; i < toremove->data.fdata.chunks; i++) {
			chunkid = toremove->data.fdata.chunktab[i];
			if (chunkid > 0) {
				if (chunk_delete_file(chunkid, toremove->goal) != LIZARDFS_STATUS_OK) {
					syslog(LOG_ERR, "structure error - chunk %016" PRIX64
					                " not found (inode: %" PRIu32
					                " ; index: %" PRIu32 ")",
					       chunkid, toremove->id, i);
				}
			}
		}
	}
	gMetadata->inode_pool.release(toremove->id, ts, true);
	xattr_removeinode(toremove->id);
	fsnodes_quota_unregister_inode(toremove);
#ifndef METARESTORE
	dcm_modify(toremove->id, 0);
#endif
	delete toremove;
}

void fsnodes_unlink(uint32_t ts, fsedge *e) {
	fsnode *child;
	uint16_t pleng = 0;
	uint8_t *path = NULL;

	child = e->child;
	if (child->parents->nextparent == NULL) {  // last link
		if (child->type == TYPE_FILE &&
		    (child->trashtime > 0 ||
		     child->data.fdata.sessionids !=
		             NULL)) {  // go to trash or reserved ? - get path
			fsnodes_getpath(e, &pleng, &path);
		}
	}
	fsnodes_remove_edge(ts, e);
	if (child->parents == NULL) {  // last link
		if (child->type == TYPE_FILE) {
			if (child->trashtime > 0) {
				child->type = TYPE_TRASH;
				child->ctime = ts;
				fsnodes_update_checksum(child);
				e = new fsedge;
				e->nleng = pleng;
				e->name = path;
				e->child = child;
				e->parent = NULL;
				e->nextchild = gMetadata->trash;
				e->nextparent = NULL;
				e->prevchild = &gMetadata->trash;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				e->next = NULL;
				e->prev = NULL;
				gMetadata->trash = e;
				child->parents = e;
				gMetadata->trashspace += child->data.fdata.length;
				gMetadata->trashnodes++;
				e->checksum = 0;
				fsedges_update_checksum(e);
			} else if (child->data.fdata.sessionids != NULL) {
				child->type = TYPE_RESERVED;
				fsnodes_update_checksum(child);
				e = new fsedge;
				e->nleng = pleng;
				e->name = path;
				e->child = child;
				e->parent = NULL;
				e->nextchild = gMetadata->reserved;
				e->nextparent = NULL;
				e->prevchild = &gMetadata->reserved;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				e->next = NULL;
				e->prev = NULL;
				gMetadata->reserved = e;
				child->parents = e;
				gMetadata->reservedspace += child->data.fdata.length;
				gMetadata->reservednodes++;
				e->checksum = 0;
				fsedges_update_checksum(e);
			} else {
				free(path);
				fsnodes_remove_node(ts, child);
			}
		} else {
			free(path);
			fsnodes_remove_node(ts, child);
		}
	} else {
		free(path);
	}
}

int fsnodes_purge(uint32_t ts, fsnode *p) {
	fsedge *e;
	e = p->parents;

	if (p->type == TYPE_TRASH) {
		gMetadata->trashspace -= p->data.fdata.length;
		gMetadata->trashnodes--;
		if (p->data.fdata.sessionids != NULL) {
			p->type = TYPE_RESERVED;
			fsnodes_update_checksum(p);
			gMetadata->reservedspace += p->data.fdata.length;
			gMetadata->reservednodes++;
			*(e->prevchild) = e->nextchild;
			if (e->nextchild) {
				e->nextchild->prevchild = e->prevchild;
			}
			e->nextchild = gMetadata->reserved;
			e->prevchild = &(gMetadata->reserved);
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			gMetadata->reserved = e;
			return 0;
		} else {
			fsnodes_remove_edge(ts, e);
			fsnodes_remove_node(ts, p);
			return 1;
		}
	} else if (p->type == TYPE_RESERVED) {
		gMetadata->reservedspace -= p->data.fdata.length;
		gMetadata->reservednodes--;
		fsnodes_remove_edge(ts, e);
		fsnodes_remove_node(ts, p);
		return 1;
	}
	return -1;
}

uint8_t fsnodes_undel(uint32_t ts, fsnode *node) {
	uint16_t pleng;
	const uint8_t *path;
	uint8_t is_new;
	uint32_t i, partleng, dots;
	fsedge *e, *pe;
	fsnode *p, *n;

	/* check path */
	e = node->parents;
	pleng = e->nleng;
	path = e->name;

	if (path == NULL) {
		return LIZARDFS_ERROR_CANTCREATEPATH;
	}
	while (*path == '/' && pleng > 0) {
		path++;
		pleng--;
	}
	if (pleng == 0) {
		return LIZARDFS_ERROR_CANTCREATEPATH;
	}
	partleng = 0;
	dots = 0;
	for (i = 0; i < pleng; i++) {
		if (path[i] == 0) {  // incorrect name character
			return LIZARDFS_ERROR_CANTCREATEPATH;
		} else if (path[i] == '/') {
			if (partleng == 0) {  // "//" in path
				return LIZARDFS_ERROR_CANTCREATEPATH;
			}
			if (partleng == dots && partleng <= 2) {  // '.' or '..' in path
				return LIZARDFS_ERROR_CANTCREATEPATH;
			}
			partleng = 0;
			dots = 0;
		} else {
			if (path[i] == '.') {
				dots++;
			}
			partleng++;
			if (partleng > MAXFNAMELENG) {
				return LIZARDFS_ERROR_CANTCREATEPATH;
			}
		}
	}
	if (partleng == 0) {  // last part canot be empty - it's the name of undeleted file
		return LIZARDFS_ERROR_CANTCREATEPATH;
	}
	if (partleng == dots && partleng <= 2) {  // '.' or '..' in path
		return LIZARDFS_ERROR_CANTCREATEPATH;
	}

	/* create path */
	n = NULL;
	p = gMetadata->root;
	is_new = 0;
	for (;;) {
		partleng = 0;
		while ((partleng < pleng) && (path[partleng] != '/')) {
			partleng++;
		}
		if (partleng == pleng) {  // last name
			if (fsnodes_nameisused(p, partleng, path)) {
				return LIZARDFS_ERROR_EEXIST;
			}
			// remove from trash and link to new parent
			node->type = TYPE_FILE;
			node->ctime = ts;
			fsnodes_update_checksum(node);
			fsnodes_link(ts, p, node, partleng, path);
			fsnodes_remove_edge(ts, e);
			gMetadata->trashspace -= node->data.fdata.length;
			gMetadata->trashnodes--;
			return LIZARDFS_STATUS_OK;
		} else {
			if (is_new == 0) {
				pe = fsnodes_lookup(p, partleng, path);
				if (pe == NULL) {
					is_new = 1;
				} else {
					n = pe->child;
					if (n->type != TYPE_DIRECTORY) {
						return LIZARDFS_ERROR_CANTCREATEPATH;
					}
				}
			}
			if (is_new == 1) {
				n = fsnodes_create_node(ts, p, partleng, path, TYPE_DIRECTORY, 0755,
				                        0, 0, 0, 0,
				                        AclInheritance::kDontInheritAcl);
			}
			p = n;
		}
		path += partleng + 1;
		pleng -= partleng + 1;
	}
}

#ifndef METARESTORE

void fsnodes_getgoal_recursive(fsnode *node, uint8_t gmode, GoalMap<uint32_t> &fgtab,
				GoalMap<uint32_t> &dgtab) {
	fsedge *e;

	if (node->type == TYPE_FILE || node->type == TYPE_TRASH || node->type == TYPE_RESERVED) {
		if (!goal::isGoalValid(node->goal)) {
			syslog(LOG_WARNING, "file inode %" PRIu32 ": unknown goal !!! - fixing",
			       node->id);
			fsnodes_changefilegoal(node, DEFAULT_GOAL);
		}
		fgtab[node->goal]++;
	} else if (node->type == TYPE_DIRECTORY) {
		if (!goal::isGoalValid(node->goal)) {
			syslog(LOG_WARNING,
			       "directory inode %" PRIu32 ": unknown goal !!! - fixing", node->id);
			node->goal = DEFAULT_GOAL;
		}
		dgtab[node->goal]++;
		if (gmode == GMODE_RECURSIVE) {
			for (e = node->data.ddata.children; e; e = e->nextchild) {
				fsnodes_getgoal_recursive(e->child, gmode, fgtab, dgtab);
			}
		}
	}
}

void fsnodes_gettrashtime_recursive(fsnode *node, uint8_t gmode,
	TrashtimeMap &fileTrashtimes, TrashtimeMap &dirTrashtimes) {
	fsedge *e;

	if (node->type == TYPE_FILE || node->type == TYPE_TRASH || node->type == TYPE_RESERVED) {
		fileTrashtimes[node->trashtime] += 1;
	} else if (node->type == TYPE_DIRECTORY) {
		dirTrashtimes[node->trashtime] += 1;
		if (gmode == GMODE_RECURSIVE) {
			for (e = node->data.ddata.children; e; e = e->nextchild) {
				fsnodes_gettrashtime_recursive(e->child, gmode, fileTrashtimes, dirTrashtimes);
			}
		}
	}
}

void fsnodes_geteattr_recursive(fsnode *node, uint8_t gmode, uint32_t feattrtab[16],
				uint32_t deattrtab[16]) {
	fsedge *e;

	if (node->type != TYPE_DIRECTORY) {
		feattrtab[(node->mode >> 12) &
		          (EATTR_NOOWNER | EATTR_NOACACHE | EATTR_NODATACACHE)]++;
	} else {
		deattrtab[(node->mode >> 12)]++;
		if (gmode == GMODE_RECURSIVE) {
			for (e = node->data.ddata.children; e; e = e->nextchild) {
				fsnodes_geteattr_recursive(e->child, gmode, feattrtab, deattrtab);
			}
		}
	}
}

static inline void fsnodes_enqueue_tape_copies(fsnode *node) {
	if (node->type != TYPE_FILE && node->type != TYPE_TRASH && node->type != TYPE_RESERVED) {
		return;
	}

	unsigned tapeGoalSize = fs_get_goal_definition(node->goal).tapeLabels().size();

	if (tapeGoalSize == 0) {
		return;
	}

	auto it = gMetadata->tapeCopies.find(node->id);
	unsigned tapeCopyCount = (it == gMetadata->tapeCopies.end() ? 0 : it->second.size());

	/* Create new TapeCopies instance if necessary */
	if (tapeGoalSize > tapeCopyCount && it == gMetadata->tapeCopies.end()) {
		it = gMetadata->tapeCopies.insert({node->id, TapeCopies()}).first;
	}

	/* Enqueue copies for tapeservers */
	TapeKey tapeKey(node->id, node->mtime, node->data.fdata.length);
	while (tapeGoalSize > tapeCopyCount) {
		TapeserverId id = matotsserv_enqueue_node(tapeKey);
		it->second.emplace_back(TapeCopyState::kCreating, id);
		tapeCopyCount++;
	}
}

bool fsnodes_has_tape_goal(fsnode *node) {
	return fs_get_goal_definition(node->goal).tapeLabels().size() > 0;
}

#endif

void fsnodes_setgoal_recursive(fsnode *node, uint32_t ts, uint32_t uid, uint8_t goal, uint8_t smode,
				uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	fsedge *e;

	if (node->type == TYPE_FILE || node->type == TYPE_DIRECTORY || node->type == TYPE_TRASH ||
	    node->type == TYPE_RESERVED) {
		if ((node->mode & (EATTR_NOOWNER << 12)) == 0 && uid != 0 && node->uid != uid) {
			(*nsinodes)++;
		} else {
			if ((smode & SMODE_TMASK) == SMODE_SET && node->goal != goal) {
				if (node->type != TYPE_DIRECTORY) {
					fsnodes_changefilegoal(node, goal);
					(*sinodes)++;
#ifndef METARESTORE
					if (matotsserv_can_enqueue_node()) {
						fsnodes_enqueue_tape_copies(node);
					}
#endif
				} else {
					node->goal = goal;
					(*sinodes)++;
				}
				node->ctime = ts;
				fsnodes_update_checksum(node);
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type == TYPE_DIRECTORY && (smode & SMODE_RMASK)) {
			for (e = node->data.ddata.children; e; e = e->nextchild) {
				fsnodes_setgoal_recursive(e->child, ts, uid, goal, smode, sinodes,
				                          ncinodes, nsinodes);
			}
		}
	}
}

void fsnodes_settrashtime_recursive(fsnode *node, uint32_t ts, uint32_t uid, uint32_t trashtime,
					uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
					uint32_t *nsinodes) {
	fsedge *e;
	uint8_t set;

	if (node->type == TYPE_FILE || node->type == TYPE_DIRECTORY || node->type == TYPE_TRASH ||
	    node->type == TYPE_RESERVED) {
		if ((node->mode & (EATTR_NOOWNER << 12)) == 0 && uid != 0 && node->uid != uid) {
			(*nsinodes)++;
		} else {
			set = 0;
			switch (smode & SMODE_TMASK) {
			case SMODE_SET:
				if (node->trashtime != trashtime) {
					node->trashtime = trashtime;
					set = 1;
				}
				break;
			case SMODE_INCREASE:
				if (node->trashtime < trashtime) {
					node->trashtime = trashtime;
					set = 1;
				}
				break;
			case SMODE_DECREASE:
				if (node->trashtime > trashtime) {
					node->trashtime = trashtime;
					set = 1;
				}
				break;
			}
			if (set) {
				(*sinodes)++;
				node->ctime = ts;
				fsnodes_update_checksum(node);
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type == TYPE_DIRECTORY && (smode & SMODE_RMASK)) {
			for (e = node->data.ddata.children; e; e = e->nextchild) {
				fsnodes_settrashtime_recursive(e->child, ts, uid, trashtime, smode,
				                               sinodes, ncinodes, nsinodes);
			}
		}
	}
}

void fsnodes_seteattr_recursive(fsnode *node, uint32_t ts, uint32_t uid, uint8_t eattr,
				uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
				uint32_t *nsinodes) {
	fsedge *e;
	uint8_t neweattr, seattr;

	if ((node->mode & (EATTR_NOOWNER << 12)) == 0 && uid != 0 && node->uid != uid) {
		(*nsinodes)++;
	} else {
		seattr = eattr;
		if (node->type != TYPE_DIRECTORY) {
			node->mode &= ~(EATTR_NOECACHE << 12);
			seattr &= ~(EATTR_NOECACHE);
		}
		neweattr = (node->mode >> 12);
		switch (smode & SMODE_TMASK) {
		case SMODE_SET:
			neweattr = seattr;
			break;
		case SMODE_INCREASE:
			neweattr |= seattr;
			break;
		case SMODE_DECREASE:
			neweattr &= ~seattr;
			break;
		}
		if (neweattr != (node->mode >> 12)) {
			node->mode = (node->mode & 0xFFF) | (((uint16_t)neweattr) << 12);
			(*sinodes)++;
			node->ctime = ts;
		} else {
			(*ncinodes)++;
		}
	}
	if (node->type == TYPE_DIRECTORY && (smode & SMODE_RMASK)) {
		for (e = node->data.ddata.children; e; e = e->nextchild) {
			fsnodes_seteattr_recursive(e->child, ts, uid, eattr, smode, sinodes,
			                           ncinodes, nsinodes);
		}
	}
	fsnodes_update_checksum(node);
}



uint8_t fsnodes_deleteacl(fsnode *p, AclType type, uint32_t ts) {
	if (type == AclType::kDefault) {
		if (p->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOTSUP;
		}
		p->defaultAcl.reset();
	} else {
		p->extendedAcl.reset();
	}
	p->ctime = ts;
	fsnodes_update_checksum(p);
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
uint8_t fsnodes_getacl(fsnode *p, AclType type, AccessControlList &acl) {
	if (type == AclType::kDefault) {
		if (p->type != TYPE_DIRECTORY || !p->defaultAcl) {
			return LIZARDFS_ERROR_ENOATTR;
		}
		acl = *(p->defaultAcl);
	} else {
		if (!p->extendedAcl) {
			return LIZARDFS_ERROR_ENOATTR;
		}
		acl.mode = (p->mode & 0777);
		acl.extendedAcl.reset(new ExtendedAcl(*p->extendedAcl));
	}
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fsnodes_setacl(fsnode *p, AclType type, AccessControlList acl, uint32_t ts) {
	if (type == AclType::kDefault) {
		if (p->type != TYPE_DIRECTORY) {
			return LIZARDFS_ERROR_ENOTSUP;
		}
		p->defaultAcl.reset(new AccessControlList(std::move(acl)));
	} else {
		p->mode = (p->mode & ~0777) | (acl.mode & 0777);
		p->extendedAcl = std::move(acl.extendedAcl);
	}
	p->ctime = ts;
	fsnodes_update_checksum(p);
	return LIZARDFS_STATUS_OK;
}

int fsnodes_namecheck(uint32_t nleng, const uint8_t *name) {
	uint32_t i;
	if (nleng == 0 || nleng > MAXFNAMELENG) {
		return -1;
	}
	if (name[0] == '.') {
		if (nleng == 1) {
			return -1;
		}
		if (nleng == 2 && name[1] == '.') {
			return -1;
		}
	}
	for (i = 0; i < nleng; i++) {
		if (name[i] == '\0' || name[i] == '/') {
			return -1;
		}
	}
	return 0;
}

int fsnodes_access(fsnode *node, uint32_t uid, uint32_t gid, uint8_t modemask, uint8_t sesflags) {
	uint8_t nodemode;
	if ((sesflags & SESFLAG_NOMASTERPERMCHECK) || uid == 0) {
		return 1;
	}
	if (uid == node->uid || (node->mode & (EATTR_NOOWNER << 12))) {
		nodemode = ((node->mode) >> 6) & 7;
	} else if (sesflags & SESFLAG_IGNOREGID) {
		nodemode = (((node->mode) >> 3) | (node->mode)) & 7;
	} else if (gid == node->gid) {
		nodemode = ((node->mode) >> 3) & 7;
	} else {
		nodemode = (node->mode & 7);
	}
	if ((nodemode & modemask) == modemask) {
		return 1;
	}
	return 0;
}

int fsnodes_sticky_access(fsnode *parent, fsnode *node, uint32_t uid) {
	if (uid == 0 || (parent->mode & 01000) == 0) {  // super user or sticky bit is not set
		return 1;
	}
	if (uid == parent->uid || (parent->mode & (EATTR_NOOWNER << 12)) || uid == node->uid ||
	    (node->mode & (EATTR_NOOWNER << 12))) {
		return 1;
	}
	return 0;
}

uint8_t verify_session(const FsContext &context, OperationMode operationMode,
			SessionType sessionType) {
	if (context.hasSessionData() && (context.sesflags() & SESFLAG_READONLY) &&
	    (operationMode == OperationMode::kReadWrite)) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (context.hasSessionData() && (context.rootinode() == 0) &&
	    (sessionType == SessionType::kNotMeta)) {
		return LIZARDFS_ERROR_ENOENT;
	}
	if (context.hasSessionData() && (context.rootinode() != 0) &&
	    (sessionType == SessionType::kOnlyMeta)) {
		return LIZARDFS_ERROR_EPERM;
	}
	return LIZARDFS_STATUS_OK;
}

/*
 * Treating rootinode as the root of the hierarchy, converts (rootinode, inode) to fsnode*
 * ie:
 * * if inode == rootinode, then returns root node
 * * if inode != rootinode, then returns some node
 * Checks for permissions needed to perform the operation (defined by modemask)
 * Can return a reserved node or a node from trash
 */
uint8_t fsnodes_get_node_for_operation(const FsContext &context, ExpectedNodeType expectedNodeType,
					uint8_t modemask, uint32_t inode, fsnode **ret) {
	fsnode *p;
	if (!context.hasSessionData()) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
	} else if (context.rootinode() == MFS_ROOT_ID || (context.rootinode() == 0)) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return LIZARDFS_ERROR_ENOENT;
		}
		if (context.rootinode() == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return LIZARDFS_ERROR_EPERM;
		}
	} else {
		fsnode *rn = fsnodes_id_to_node(context.rootinode());
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
	if ((expectedNodeType == ExpectedNodeType::kDirectory) && (p->type != TYPE_DIRECTORY)) {
		return LIZARDFS_ERROR_ENOTDIR;
	}
	if ((expectedNodeType == ExpectedNodeType::kNotDirectory) && (p->type == TYPE_DIRECTORY)) {
		return LIZARDFS_ERROR_EPERM;
	}
	if ((expectedNodeType == ExpectedNodeType::kFile) && (p->type != TYPE_FILE) &&
	    (p->type != TYPE_RESERVED) && (p->type != TYPE_TRASH)) {
		return LIZARDFS_ERROR_EPERM;
	}
	if (context.canCheckPermissions() &&
	    !fsnodes_access(p, context.uid(), context.gid(), modemask, context.sesflags())) {
		return LIZARDFS_ERROR_EACCES;
	}
	*ret = p;
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE

const GoalMap<Goal> &fsnodes_get_goal_definitions() {
	return gGoalDefinitions;
}

const Goal &fsnodes_get_goal_definition(uint8_t goalId) {
	return gGoalDefinitions[goalId];
}

#endif
