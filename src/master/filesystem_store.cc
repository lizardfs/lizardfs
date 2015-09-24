/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2015 Skytechnology sp. z o.o..

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
#include "master/filesystem_store.h"

#include <cstdio>
#include <vector>

#include "common/cwrap.h"
#include "common/main.h"
#include "common/setup.h"
#include "common/lizardfs_version.h"
#include "common/metadata.h"
#include "common/rotate_files.h"
#include "common/setup.h"

#include "master/changelog.h"
#include "master/filesystem.h"
#include "master/filesystem_xattr.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"
#include "master/filesystem_freenode.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_checksum.h"
#include "master/locks.h"
#include "master/matoclserv.h"
#include "master/matomlserv.h"
#include "master/metadata_dumper.h"
#include "master/restore.h"

constexpr uint8_t kMetadataVersionMooseFS = 0x15;
constexpr uint8_t kMetadataVersionLizardFS = 0x16;
constexpr uint8_t kMetadataVersionWithSections = 0x20;
constexpr uint8_t kMetadataVersionWithLockIds = 0x29;

char const MetadataStructureReadErrorMsg[] = "error reading metadata (structure)";

void xattr_store(FILE *fd) {
	uint8_t hdrbuff[4 + 1 + 4];
	uint8_t *ptr;
	uint32_t i;
	xattr_data_entry *xa;

	for (i = 0; i < XATTR_DATA_HASH_SIZE; i++) {
		for (xa = gMetadata->xattr_data_hash[i]; xa; xa = xa->next) {
			ptr = hdrbuff;
			put32bit(&ptr, xa->inode);
			put8bit(&ptr, xa->anleng);
			put32bit(&ptr, xa->avleng);
			if (fwrite(hdrbuff, 1, 4 + 1 + 4, fd) != (size_t)(4 + 1 + 4)) {
				syslog(LOG_NOTICE, "fwrite error");
				return;
			}
			if (fwrite(xa->attrname, 1, xa->anleng, fd) != (size_t)(xa->anleng)) {
				syslog(LOG_NOTICE, "fwrite error");
				return;
			}
			if (xa->avleng > 0) {
				if (fwrite(xa->attrvalue, 1, xa->avleng, fd) !=
				    (size_t)(xa->avleng)) {
					syslog(LOG_NOTICE, "fwrite error");
					return;
				}
			}
		}
	}
	memset(hdrbuff, 0, 4 + 1 + 4);
	if (fwrite(hdrbuff, 1, 4 + 1 + 4, fd) != (size_t)(4 + 1 + 4)) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

int xattr_load(FILE *fd, int ignoreflag) {
	uint8_t hdrbuff[4 + 1 + 4];
	const uint8_t *ptr;
	uint32_t inode;
	uint8_t anleng;
	uint32_t avleng;
	xattr_data_entry *xa;
	xattr_inode_entry *ih;
	uint32_t hash, ihash;

	while (1) {
		if (fread(hdrbuff, 1, 4 + 1 + 4, fd) != 4 + 1 + 4) {
			lzfs_pretty_errlog(LOG_ERR, "loading xattr: read error");
			return -1;
		}
		ptr = hdrbuff;
		inode = get32bit(&ptr);
		anleng = get8bit(&ptr);
		avleng = get32bit(&ptr);
		if (inode == 0) {
			return 1;
		}
		if (anleng == 0) {
			lzfs_pretty_syslog(LOG_ERR, "loading xattr: empty name");
			if (ignoreflag) {
				fseek(fd, anleng + avleng, SEEK_CUR);
				continue;
			} else {
				return -1;
			}
		}
		if (avleng > MFS_XATTR_SIZE_MAX) {
			lzfs_pretty_syslog(LOG_ERR, "loading xattr: value oversized");
			if (ignoreflag) {
				fseek(fd, anleng + avleng, SEEK_CUR);
				continue;
			} else {
				return -1;
			}
		}

		ihash = xattr_inode_hash_fn(inode);
		for (ih = gMetadata->xattr_inode_hash[ihash]; ih && ih->inode != inode;
			ih = ih->next) {
		}

		if (ih && ih->anleng + anleng + 1 > MFS_XATTR_LIST_MAX) {
			lzfs_pretty_syslog(LOG_ERR, "loading xattr: name list too long");
			if (ignoreflag) {
				fseek(fd, anleng + avleng, SEEK_CUR);
				continue;
			} else {
				return -1;
			}
		}

		xa = new xattr_data_entry;
		xa->inode = inode;
		xa->attrname = (uint8_t *)malloc(anleng);
		passert(xa->attrname);
		if (fread(xa->attrname, 1, anleng, fd) != (size_t)anleng) {
			int err = errno;
			delete xa;
			errno = err;
			lzfs_pretty_errlog(LOG_ERR, "loading xattr: read error");
			return -1;
		}
		xa->anleng = anleng;
		if (avleng > 0) {
			xa->attrvalue = (uint8_t *)malloc(avleng);
			passert(xa->attrvalue);
			if (fread(xa->attrvalue, 1, avleng, fd) != (size_t)avleng) {
				int err = errno;
				delete xa;
				errno = err;
				lzfs_pretty_errlog(LOG_ERR, "loading xattr: read error");
				return -1;
			}
		} else {
			xa->attrvalue = NULL;
		}
		xa->avleng = avleng;
		hash = xattr_data_hash_fn(inode, xa->anleng, xa->attrname);
		xa->next = gMetadata->xattr_data_hash[hash];
		if (xa->next) {
			xa->next->prev = &(xa->next);
		}
		xa->prev = gMetadata->xattr_data_hash + hash;
		gMetadata->xattr_data_hash[hash] = xa;

		if (ih) {
			xa->nextinode = ih->data_head;
			if (xa->nextinode) {
				xa->nextinode->previnode = &(xa->nextinode);
			}
			xa->previnode = &(ih->data_head);
			ih->data_head = xa;
			ih->anleng += anleng + 1U;
			ih->avleng += avleng;
		} else {
			ih = (xattr_inode_entry *)malloc(sizeof(xattr_inode_entry));
			passert(ih);
			ih->inode = inode;
			xa->nextinode = NULL;
			xa->previnode = &(ih->data_head);
			ih->data_head = xa;
			ih->anleng = anleng + 1U;
			ih->avleng = avleng;
			ih->next = gMetadata->xattr_inode_hash[ihash];
			gMetadata->xattr_inode_hash[ihash] = ih;
		}
	}
}

template <class... Args>
static void fs_store_generic(FILE *fd, Args &&... args) {
	static std::vector<uint8_t> buffer;
	buffer.clear();
	const uint32_t size = serializedSize(std::forward<Args>(args)...);
	serialize(buffer, size, std::forward<Args>(args)...);
	if (fwrite(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

template <class... Args>
static bool fs_load_generic(FILE *fd, Args &&... args) {
	static std::vector<uint8_t> buffer;
	uint32_t size;
	buffer.resize(4);
	if (fread(buffer.data(), 1, 4, fd) != 4) {
		throw Exception("fread error (size)");
	}
	deserialize(buffer, size);
	if (size == 0) {
		// marker
		return false;
	}
	buffer.resize(size);
	if (fread(buffer.data(), 1, size, fd) != size) {
		throw Exception("fread error (entry)");
	}
	deserialize(buffer, std::forward<Args>(args)...);
	return true;
}

static void fs_storeacl(fsnode *p, FILE *fd) {
	static std::vector<uint8_t> buffer;
	buffer.clear();
	if (!p) {
		// write end marker
		uint32_t marker = 0;
		serialize(buffer, marker);
	} else {
		uint32_t size = serializedSize(p->id, p->extendedAcl, p->defaultAcl);
		serialize(buffer, size, p->id, p->extendedAcl, p->defaultAcl);
	}
	if (fwrite(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

static int fs_loadacl(FILE *fd, int ignoreflag) {
	static std::vector<uint8_t> buffer;

	// initialize
	if (fd == nullptr) {
		return 0;
	}

	try {
		// Read size of the entry
		uint32_t size = 0;
		buffer.resize(serializedSize(size));
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}
		deserialize(buffer, size);
		if (size == 0) {
			// this is end marker
			return 1;
		} else if (size > 10000000) {
			throw Exception("strange size of entry: " + std::to_string(size),
				LIZARDFS_ERROR_ERANGE);
		}

		// Read the entry
		buffer.resize(size);
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}

		// Deserialize inode
		uint32_t inode;
		deserialize(buffer, inode);
		fsnode *p = fsnodes_id_to_node(inode);
		if (!p) {
			throw Exception("unknown inode: " + std::to_string(inode));
		}

		// Deserialize ACL
		deserialize(buffer, inode, p->extendedAcl, p->defaultAcl);
		return 0;
	} catch (Exception &ex) {
		lzfs_pretty_syslog(LOG_ERR, "loading acl: %s", ex.what());
		if (!ignoreflag || ex.status() != LIZARDFS_STATUS_OK) {
			return -1;
		} else {
			return 0;
		}
	}
}

void fs_storeedge(fsedge *e, FILE *fd) {
	uint8_t uedgebuff[4 + 4 + 2 + 65535];
	uint8_t *ptr;
	if (e == NULL) {  // last edge
		memset(uedgebuff, 0, 4 + 4 + 2);
		if (fwrite(uedgebuff, 1, 4 + 4 + 2, fd) != (size_t)(4 + 4 + 2)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		return;
	}
	ptr = uedgebuff;
	if (e->parent == NULL) {
		put32bit(&ptr, 0);
	} else {
		put32bit(&ptr, e->parent->id);
	}
	put32bit(&ptr, e->child->id);
	put16bit(&ptr, e->nleng);
	memcpy(ptr, e->name, e->nleng);
	if (fwrite(uedgebuff, 1, 4 + 4 + 2 + e->nleng, fd) != (size_t)(4 + 4 + 2 + e->nleng)) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

int fs_loadedge(FILE *fd, int ignoreflag) {
	uint8_t uedgebuff[4 + 4 + 2];
	const uint8_t *ptr;
	uint32_t parent_id;
	uint32_t child_id;
	uint32_t hpos;
	fsedge *e;
	statsrecord sr;
	static fsedge **root_tail;
	static fsedge **current_tail;
	static uint32_t current_parent_id;

	if (fd == NULL) {
		current_parent_id = 0;
		current_tail = NULL;
		root_tail = NULL;
		return 0;
	}

	if (fread(uedgebuff, 1, 4 + 4 + 2, fd) != 4 + 4 + 2) {
		lzfs_pretty_errlog(LOG_ERR, "loading edge: read error");
		return -1;
	}
	ptr = uedgebuff;
	parent_id = get32bit(&ptr);
	child_id = get32bit(&ptr);
	if (parent_id == 0 && child_id == 0) {  // last edge
		return 1;
	}
	e = new fsedge;
	e->nleng = get16bit(&ptr);
	if (e->nleng == 0) {
		lzfs_pretty_syslog(LOG_ERR,
		                   "loading edge: %" PRIu32 "->%" PRIu32 " error: empty name",
		                   parent_id, child_id);
		delete e;
		return -1;
	}
	e->name = (uint8_t *)malloc(e->nleng);
	passert(e->name);
	if (fread(e->name, 1, e->nleng, fd) != e->nleng) {
		lzfs_pretty_errlog(LOG_ERR, "loading edge: read error");
		delete e;
		return -1;
	}
	e->child = fsnodes_id_to_node(child_id);
	if (e->child == NULL) {
		lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
		                            " error: child not found",
		                   parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
		delete e;
		if (ignoreflag) {
			return 0;
		}
		return -1;
	}
	if (parent_id == 0) {
		if (e->child->type == TYPE_TRASH) {
			e->parent = NULL;
			e->nextchild = gMetadata->trash;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			gMetadata->trash = e;
			e->prevchild = &gMetadata->trash;
			e->next = NULL;
			e->prev = NULL;
			gMetadata->trashspace += e->child->data.fdata.length;
			gMetadata->trashnodes++;
		} else if (e->child->type == TYPE_RESERVED) {
			e->parent = NULL;
			e->nextchild = gMetadata->reserved;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			gMetadata->reserved = e;
			e->prevchild = &gMetadata->reserved;
			e->next = NULL;
			e->prev = NULL;
			gMetadata->reservedspace += e->child->data.fdata.length;
			gMetadata->reservednodes++;
		} else {
			lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
			                            " error: bad child type (%c)\n",
			                   parent_id, fsnodes_escape_name(e->nleng, e->name),
			                   child_id, e->child->type);
			delete e;
			return -1;
		}
	} else {
		e->parent = fsnodes_id_to_node(parent_id);
		if (e->parent == NULL) {
			lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
			                            " error: parent not found",
			                   parent_id, fsnodes_escape_name(e->nleng, e->name),
			                   child_id);
			if (ignoreflag) {
				e->parent = fsnodes_id_to_node(SPECIAL_INODE_ROOT);
				if (e->parent == NULL || e->parent->type != TYPE_DIRECTORY) {
					lzfs_pretty_syslog(
					        LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
					                 " root dir not found !!!",
					        parent_id, fsnodes_escape_name(e->nleng, e->name),
					        child_id);
					delete e;
					return -1;
				}
				lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
				                            " attaching node to root dir",
				                   parent_id,
				                   fsnodes_escape_name(e->nleng, e->name),
				                   child_id);
				parent_id = SPECIAL_INODE_ROOT;
			} else {
				lzfs_pretty_syslog(LOG_ERR,
				                   "use mfsmetarestore (option -i) to attach this "
				                   "node to root dir\n");
				delete e;
				return -1;
			}
		}
		if (e->parent->type != TYPE_DIRECTORY) {
			lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
			                            " error: bad parent type (%c)",
			                   parent_id, fsnodes_escape_name(e->nleng, e->name),
			                   child_id, e->parent->type);
			if (ignoreflag) {
				e->parent = fsnodes_id_to_node(SPECIAL_INODE_ROOT);
				if (e->parent == NULL || e->parent->type != TYPE_DIRECTORY) {
					lzfs_pretty_syslog(
					        LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
					                 " root dir not found !!!",
					        parent_id, fsnodes_escape_name(e->nleng, e->name),
					        child_id);
					delete e;
					return -1;
				}
				lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
				                            " attaching node to root dir",
				                   parent_id,
				                   fsnodes_escape_name(e->nleng, e->name),
				                   child_id);
				parent_id = SPECIAL_INODE_ROOT;
			} else {
				lzfs_pretty_syslog(LOG_ERR,
				                   "use mfsmetarestore (option -i) to attach this "
				                   "node to root dir\n");
				delete e;
				return -1;
			}
		}
		if (parent_id == SPECIAL_INODE_ROOT) {  // special case - because of 'ignoreflag' and
			                         // possibility of attaching orphans into root node
			if (root_tail == NULL) {
				root_tail = &(e->parent->data.ddata.children);
			}
		} else if (current_parent_id != parent_id) {
			if (e->parent->data.ddata.children) {
				syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
				                " error: parent node sequence error",
				       parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
				if (ignoreflag) {
					current_tail = &(e->parent->data.ddata.children);
					while (*current_tail) {
						current_tail = &((*current_tail)->nextchild);
					}
				} else {
					delete e;
					return -1;
				}
			} else {
				current_tail = &(e->parent->data.ddata.children);
			}
			current_parent_id = parent_id;
		}
		e->nextchild = NULL;
		if (parent_id == SPECIAL_INODE_ROOT) {
			*(root_tail) = e;
			e->prevchild = root_tail;
			root_tail = &(e->nextchild);
		} else {
			*(current_tail) = e;
			e->prevchild = current_tail;
			current_tail = &(e->nextchild);
		}
		e->parent->data.ddata.elements++;
		if (e->child->type == TYPE_DIRECTORY) {
			e->parent->data.ddata.nlink++;
		}
		hpos = EDGEHASHPOS(fsnodes_hash(e->parent->id, e->nleng, e->name));
		e->next = gMetadata->edgehash[hpos];
		if (e->next) {
			e->next->prev = &(e->next);
		}
		gMetadata->edgehash[hpos] = e;
		e->prev = &(gMetadata->edgehash[hpos]);
	}
	e->nextparent = e->child->parents;
	if (e->nextparent) {
		e->nextparent->prevparent = &(e->nextparent);
	}
	e->child->parents = e;
	e->prevparent = &(e->child->parents);
	if (e->parent) {
		fsnodes_get_stats(e->child, &sr);
		fsnodes_add_stats(e->parent, &sr);
	}
	return 0;
}

void fs_storenode(fsnode *f, FILE *fd) {
	uint8_t unodebuff[1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2 + 8 * 65536 +
	                  4 * 65536 + 4];
	uint8_t *ptr, *chptr;
	uint32_t i, indx, ch, sessionids;
	sessionidrec *sessionidptr;

	if (f == NULL) {  // last node
		fputc(0, fd);
		return;
	}
	ptr = unodebuff;
	put8bit(&ptr, f->type);
	put32bit(&ptr, f->id);
	put8bit(&ptr, f->goal);
	put16bit(&ptr, f->mode);
	put32bit(&ptr, f->uid);
	put32bit(&ptr, f->gid);
	put32bit(&ptr, f->atime);
	put32bit(&ptr, f->mtime);
	put32bit(&ptr, f->ctime);
	put32bit(&ptr, f->trashtime);
	switch (f->type) {
	case TYPE_DIRECTORY:
	case TYPE_SOCKET:
	case TYPE_FIFO:
		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr, f->data.devdata.rdev);
		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		break;
	case TYPE_SYMLINK:
		put32bit(&ptr, f->data.sdata.pleng);
		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		if (fwrite(f->data.sdata.path, 1, f->data.sdata.pleng, fd) !=
		    (size_t)(f->data.sdata.pleng)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		put64bit(&ptr, f->data.fdata.length);
		ch = 0;
		for (indx = 0; indx < f->data.fdata.chunks; indx++) {
			if (f->data.fdata.chunktab[indx] != 0) {
				ch = indx + 1;
			}
		}
		put32bit(&ptr, ch);
		sessionids = 0;
		for (sessionidptr = f->data.fdata.sessionids; sessionidptr && sessionids < 65535;
		     sessionidptr = sessionidptr->next) {
			sessionids++;
		}
		put16bit(&ptr, sessionids);

		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}

		indx = 0;
		while (ch > 65536) {
			chptr = ptr;
			for (i = 0; i < 65536; i++) {
				put64bit(&chptr, f->data.fdata.chunktab[indx]);
				indx++;
			}
			if (fwrite(ptr, 1, 8 * 65536, fd) != (size_t)(8 * 65536)) {
				syslog(LOG_NOTICE, "fwrite error");
				return;
			}
			ch -= 65536;
		}

		chptr = ptr;
		for (i = 0; i < ch; i++) {
			put64bit(&chptr, f->data.fdata.chunktab[indx]);
			indx++;
		}

		sessionids = 0;
		for (sessionidptr = f->data.fdata.sessionids; sessionidptr && sessionids < 65535;
		     sessionidptr = sessionidptr->next) {
			put32bit(&chptr, sessionidptr->sessionid);
			sessionids++;
		}

		if (fwrite(ptr, 1, 8 * ch + 4 * sessionids, fd) !=
		    (size_t)(8 * ch + 4 * sessionids)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
	}
}

int fs_loadnode(FILE *fd) {
	uint8_t unodebuff[4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2 + 8 * 65536 + 4 * 65536 +
	                  4];
	const uint8_t *ptr, *chptr;
	uint8_t type;
	uint32_t i, indx, pleng, ch, sessionids, sessionid;
	fsnode *p;
	sessionidrec *sessionidptr;
	uint32_t nodepos;
	statsrecord *sr;

	if (fd == NULL) {
		return 0;
	}

	type = fgetc(fd);
	if (type == 0) {  // last node
		return 1;
	}
	p = new fsnode(type);
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		if (fread(unodebuff, 1, 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			delete p;
			return -1;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		if (fread(unodebuff, 1, 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			delete p;
			return -1;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		if (fread(unodebuff, 1, 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2, fd) !=
		    4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			delete p;
			return -1;
		}
		break;
	default:
		lzfs_pretty_syslog(LOG_ERR, "loading node: unrecognized node type: %c", type);
		delete p;
		return -1;
	}
	ptr = unodebuff;
	p->id = get32bit(&ptr);
	p->goal = get8bit(&ptr);
	p->mode = get16bit(&ptr);
	p->uid = get32bit(&ptr);
	p->gid = get32bit(&ptr);
	p->atime = get32bit(&ptr);
	p->mtime = get32bit(&ptr);
	p->ctime = get32bit(&ptr);
	p->trashtime = get32bit(&ptr);
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
	case TYPE_SOCKET:
	case TYPE_FIFO:
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = get32bit(&ptr);
		break;
	case TYPE_SYMLINK:
		pleng = get32bit(&ptr);
		p->data.sdata.pleng = pleng;
		if (pleng > 0) {
			p->data.sdata.path = (uint8_t *)malloc(pleng);
			passert(p->data.sdata.path);
			if (fread(p->data.sdata.path, 1, pleng, fd) != pleng) {
				lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
				delete p;
				return -1;
			}
		} else {
			p->data.sdata.path = NULL;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		p->data.fdata.length = get64bit(&ptr);
		ch = get32bit(&ptr);
		p->data.fdata.chunks = ch;
		sessionids = get16bit(&ptr);
		if (ch > 0) {
			p->data.fdata.chunktab = (uint64_t *)malloc(sizeof(uint64_t) * ch);
			passert(p->data.fdata.chunktab);
		} else {
			p->data.fdata.chunktab = NULL;
		}
		indx = 0;
		while (ch > 65536) {
			chptr = ptr;
			if (fread((uint8_t *)ptr, 1, 8 * 65536, fd) != 8 * 65536) {
				lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
				delete p;
				return -1;
			}
			for (i = 0; i < 65536; i++) {
				p->data.fdata.chunktab[indx] = get64bit(&chptr);
				indx++;
			}
			ch -= 65536;
		}
		if (fread((uint8_t *)ptr, 1, 8 * ch + 4 * sessionids, fd) !=
		    8 * ch + 4 * sessionids) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			delete p;
			return -1;
		}
		for (i = 0; i < ch; i++) {
			p->data.fdata.chunktab[indx] = get64bit(&ptr);
			indx++;
		}
		p->data.fdata.sessionids = NULL;
		while (sessionids) {
			sessionid = get32bit(&ptr);
			sessionidptr = sessionidrec_malloc();
			sessionidptr->sessionid = sessionid;
			sessionidptr->next = p->data.fdata.sessionids;
			p->data.fdata.sessionids = sessionidptr;
#ifndef METARESTORE
			matoclserv_add_open_file(sessionid, p->id);
#endif
			sessionids--;
		}
		fsnodes_quota_update_size(p, +fsnodes_get_size(p));
	}
	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = p;
	gMetadata->inode_pool.markAsAcquired(p->id);
	gMetadata->nodes++;
	if (type == TYPE_DIRECTORY) {
		gMetadata->dirnodes++;
	}
	if (type == TYPE_FILE || type == TYPE_TRASH || type == TYPE_RESERVED) {
		gMetadata->filenodes++;
	}
	fsnodes_quota_register_inode(p);
	return 0;
}

void fs_storenodes(FILE *fd) {
	uint32_t i;
	fsnode *p;
	for (i = 0; i < NODEHASHSIZE; i++) {
		for (p = gMetadata->nodehash[i]; p; p = p->next) {
			fs_storenode(p, fd);
		}
	}
	fs_storenode(NULL, fd);  // end marker
}

void fs_storeedgelist(fsedge *e, FILE *fd) {
	while (e) {
		fs_storeedge(e, fd);
		e = e->nextchild;
	}
}

void fs_storeedges_rec(fsnode *f, FILE *fd) {
	fsedge *e;
	fs_storeedgelist(f->data.ddata.children, fd);
	for (e = f->data.ddata.children; e; e = e->nextchild) {
		if (e->child->type == TYPE_DIRECTORY) {
			fs_storeedges_rec(e->child, fd);
		}
	}
}

void fs_storeedges(FILE *fd) {
	fs_storeedges_rec(gMetadata->root, fd);
	fs_storeedgelist(gMetadata->trash, fd);
	fs_storeedgelist(gMetadata->reserved, fd);
	fs_storeedge(NULL, fd);  // end marker
}

static void fs_storeacls(FILE *fd) {
	for (uint32_t i = 0; i < NODEHASHSIZE; ++i) {
		for (fsnode *p = gMetadata->nodehash[i]; p; p = p->next) {
			if (p->extendedAcl || p->defaultAcl) {
				fs_storeacl(p, fd);
			}
		}
	}
	fs_storeacl(nullptr, fd);  // end marker
}

static void fs_storequotas(FILE *fd) {
	const std::vector<QuotaEntry> &entries = gMetadata->gQuotaDatabase.getEntries();
	fs_store_generic(fd, entries);
}

static void fs_storelocks(FILE *fd) {
	gMetadata->flock_locks.store(fd);
	gMetadata->posix_locks.store(fd);
}

int fs_lostnode(fsnode *p) {
	uint8_t artname[40];
	uint32_t i, l;
	i = 0;
	do {
		if (i == 0) {
			l = snprintf((char *)artname, 40, "lost_node_%" PRIu32, p->id);
		} else {
			l = snprintf((char *)artname, 40, "lost_node_%" PRIu32 ".%" PRIu32, p->id,
			             i);
		}
		if (!fsnodes_nameisused(gMetadata->root, l, artname)) {
			fsnodes_link(0, gMetadata->root, p, l, artname);
			return 1;
		}
		i++;
	} while (i);
	return -1;
}

int fs_checknodes(int ignoreflag) {
	uint32_t i;
	fsnode *p;
	for (i = 0; i < NODEHASHSIZE; i++) {
		for (p = gMetadata->nodehash[i]; p; p = p->next) {
			if (p->parents == NULL && p != gMetadata->root) {
				lzfs_pretty_syslog(LOG_ERR, "found orphaned inode: %" PRIu32,
				                   p->id);
				if (ignoreflag) {
					if (fs_lostnode(p) < 0) {
						return -1;
					}
				} else {
					lzfs_pretty_syslog(LOG_ERR,
					                   "use mfsmetarestore (option -i) to "
					                   "attach this node to root dir\n");
					return -1;
				}
			}
		}
	}
	return 1;
}

int fs_loadnodes(FILE *fd) {
	int s;
	fs_loadnode(NULL);
	do {
		s = fs_loadnode(fd);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);
	return 0;
}

int fs_loadedges(FILE *fd, int ignoreflag) {
	int s;
	fs_loadedge(NULL, ignoreflag);  // init
	do {
		s = fs_loadedge(fd, ignoreflag);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);
	return 0;
}

static int fs_loadacls(FILE *fd, int ignoreflag) {
	fs_loadacl(NULL, ignoreflag);  // init
	int s = 0;
	do {
		s = fs_loadacl(fd, ignoreflag);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);
	return 0;
}

static int fs_loadquotas(FILE *fd, int ignoreflag) {
	try {
		std::vector<QuotaEntry> entries;
		fs_load_generic(fd, entries);
		for (const auto &entry : entries) {
			gMetadata->gQuotaDatabase.set(entry.entryKey.rigor, entry.entryKey.resource,
			                              entry.entryKey.owner.ownerType,
			                              entry.entryKey.owner.ownerId, entry.limit);
		}
	} catch (Exception &ex) {
		lzfs_pretty_syslog(LOG_ERR, "loading quotas: %s", ex.what());
		if (!ignoreflag || ex.status() != LIZARDFS_STATUS_OK) {
			return -1;
		}
	}
	return 0;
}

static int fs_loadlocks(FILE *fd, int ignoreflag) {
	try {
		gMetadata->flock_locks.load(fd);
		gMetadata->posix_locks.load(fd);
	} catch (Exception &ex) {
		lzfs_pretty_syslog(LOG_ERR, "loading locks: %s", ex.what());
		if (!ignoreflag || ex.status() != LIZARDFS_STATUS_OK) {
			return -1;
		}
	}
	return 0;
}

void fs_storefree(FILE *fd) {
	uint8_t wbuff[8 * 1024], *ptr;

	uint32_t l = gMetadata->inode_pool.detainedCount();

	ptr = wbuff;
	put32bit(&ptr, l);
	if (fwrite(wbuff, 1, 4, fd) != (size_t)4) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
	l = 0;
	ptr = wbuff;

	for (const auto &n : gMetadata->inode_pool) {
		if (l == 1024) {
			if (fwrite(wbuff, 1, 8 * 1024, fd) != (size_t)(8 * 1024)) {
				syslog(LOG_NOTICE, "fwrite error");
				return;
			}
			l = 0;
			ptr = wbuff;
		}
		put32bit(&ptr, n.id);
		put32bit(&ptr, n.ts);
		l++;
	}
	if (l > 0) {
		if (fwrite(wbuff, 1, 8 * l, fd) != (size_t)(8 * l)) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
	}
}

int fs_loadfree(FILE *fd) {
	uint8_t rbuff[8 * 1024];
	const uint8_t *ptr;
	uint32_t l, t;

	if (fread(rbuff, 1, 4, fd) != 4) {
		lzfs_pretty_errlog(LOG_ERR, "loading free nodes: read error");
		return -1;
	}
	ptr = rbuff;
	t = get32bit(&ptr);

	l = 0;
	while (t > 0) {
		if (l == 0) {
			if (t > 1024) {
				if (fread(rbuff, 1, 8 * 1024, fd) != 8 * 1024) {
					lzfs_pretty_errlog(LOG_ERR,
					                   "loading free nodes: read error");
					return -1;
				}
				l = 1024;
			} else {
				if (fread(rbuff, 1, 8 * t, fd) != 8 * t) {
					lzfs_pretty_errlog(LOG_ERR,
					                   "loading free nodes: read error");
					return -1;
				}
				l = t;
			}
			ptr = rbuff;
		}

		uint32_t id = get32bit(&ptr);
		uint32_t ts = get32bit(&ptr);

		gMetadata->inode_pool.detain(id, ts, true);

		l--;
		t--;
	}
	return 0;
}

static int process_section(const char *label, uint8_t (&hdr)[16], uint8_t *&ptr,
		off_t &offbegin, off_t &offend, FILE *&fd) {
	offend = ftello(fd);
	memcpy(hdr, label, 8);
	ptr = hdr + 8;
	put64bit(&ptr, offend - offbegin - 16);
	fseeko(fd, offbegin, SEEK_SET);
	if (fwrite(hdr, 1, 16, fd) != (size_t)16) {
		syslog(LOG_NOTICE, "fwrite error");
		return LIZARDFS_ERROR_IO;
	}
	offbegin = offend;
	fseeko(fd, offbegin + 16, SEEK_SET);
	return LIZARDFS_STATUS_OK;
}

void fs_store(FILE *fd, uint8_t fver) {
	uint8_t hdr[16];
	uint8_t *ptr;
	off_t offbegin, offend;

	ptr = hdr;
	put32bit(&ptr, gMetadata->maxnodeid);
	put64bit(&ptr, gMetadata->metaversion);
	put32bit(&ptr, gMetadata->nextsessionid);
	if (fwrite(hdr, 1, 16, fd) != (size_t)16) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
	if (fver >= kMetadataVersionWithSections) {
		offbegin = ftello(fd);
		fseeko(fd, offbegin + 16, SEEK_SET);
	} else {
		offbegin = 0;  // makes some old compilers happy
	}
	fs_storenodes(fd);
	if (fver >= kMetadataVersionWithSections) {
		if (process_section("NODE 1.0", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}
	}
	fs_storeedges(fd);
	if (fver >= kMetadataVersionWithSections) {
		if (process_section("EDGE 1.0", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}
	}
	fs_storefree(fd);
	if (fver >= kMetadataVersionWithSections) {
		if (process_section("FREE 1.0", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}
		xattr_store(fd);
		if (process_section("XATR 1.0", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}
		fs_storeacls(fd);
		if (process_section("ACLS 1.0", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}
		fs_storequotas(fd);
		if (process_section("QUOT 1.1", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}
		fs_storelocks(fd);
		if (process_section("FLCK 1.0", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}
	}
	chunk_store(fd);
	if (fver >= kMetadataVersionWithSections) {
		if (process_section("CHNK 1.0", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
			return;
		}

		fseeko(fd, offend, SEEK_SET);
		memcpy(hdr, "[MFS EOF MARKER]", 16);
		if (fwrite(hdr, 1, 16, fd) != (size_t)16) {
			syslog(LOG_NOTICE, "fwrite error");
			return;
		}
	}
}

void fs_store_fd(FILE *fd) {
#if LIZARDFS_VERSHEX >= LIZARDFS_VERSION(2, 9, 0)
	/* Note LIZARDFSSIGNATURE instead of MFSSIGNATURE! */
	const char hdr[] = LIZARDFSSIGNATURE "M 2.9";
	const uint8_t metadataVersion = kMetadataVersionWithLockIds;
#elif LIZARDFS_VERSHEX >= LIZARDFS_VERSION(1, 6, 29)
	const char hdr[] = MFSSIGNATURE "M 2.0";
	const uint8_t metadataVersion = kMetadataVersionWithSections;
#else
	const char hdr[] = MFSSIGNATURE "M 1.6";
	const uint8_t metadataVersion = kMetadataVersionLizardFS;
#endif

	if (fwrite(&hdr, 1, sizeof(hdr) - 1, fd) != sizeof(hdr) - 1) {
		syslog(LOG_NOTICE, "fwrite error");
	} else {
		fs_store(fd, metadataVersion);
	}
}

uint64_t fs_loadversion(FILE *fd) {
	uint8_t hdr[12];
	const uint8_t *ptr;
	uint64_t fversion;

	if (fread(hdr, 1, 12, fd) != 12) {
		return 0;
	}
	ptr = hdr + 4;
	fversion = get64bit(&ptr);
	return fversion;
}

int fs_load(FILE *fd, int ignoreflag, uint8_t fver) {
	uint8_t hdr[16];
	const uint8_t *ptr;
	off_t offbegin;
	uint64_t sleng;

	if (fread(hdr, 1, 16, fd) != 16) {
		lzfs_pretty_syslog(LOG_ERR, "error loading header");
		return -1;
	}
	ptr = hdr;
	gMetadata->maxnodeid = get32bit(&ptr);
	gMetadata->metaversion = get64bit(&ptr);
	gMetadata->nextsessionid = get32bit(&ptr);

	if (fver < kMetadataVersionWithSections) {
		lzfs_pretty_syslog_attempt(
		        LOG_INFO,
		        "loading objects (files,directories,etc.) from the metadata file");
		fflush(stderr);
		if (fs_loadnodes(fd) < 0) {
#ifndef METARESTORE
			lzfs_pretty_syslog(LOG_ERR, "error reading metadata (node)");
#endif
			return -1;
		}
		lzfs_pretty_syslog_attempt(LOG_INFO, "loading names");
		fflush(stderr);
		if (fs_loadedges(fd, ignoreflag) < 0) {
#ifndef METARESTORE
			lzfs_pretty_syslog(LOG_ERR, "error reading metadata (edge)");
#endif
			return -1;
		}
		lzfs_pretty_syslog_attempt(LOG_INFO,
		                           "loading deletion timestamps from the metadata file");
		fflush(stderr);
		if (fs_loadfree(fd) < 0) {
#ifndef METARESTORE
			lzfs_pretty_syslog(LOG_ERR, "error reading metadata (free)");
#endif
			return -1;
		}
		lzfs_pretty_syslog_attempt(LOG_INFO, "loading chunks data from the metadata file");
		fflush(stderr);
		if (chunk_load(fd, false) < 0) {
			fprintf(stderr, "error\n");
#ifndef METARESTORE
			lzfs_pretty_syslog(LOG_ERR, "error reading metadata (chunks)");
#endif
			return -1;
		}
	} else {  // metadata with sections
		while (1) {
			if (fread(hdr, 1, 16, fd) != 16) {
				lzfs_pretty_syslog(
				        LOG_ERR,
				        "error reading section header from the metadata file");
				return -1;
			}
			if (memcmp(hdr, "[MFS EOF MARKER]", 16) == 0) {
				break;
			}
			ptr = hdr + 8;
			sleng = get64bit(&ptr);
			offbegin = ftello(fd);
			if (memcmp(hdr, "NODE 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_INFO,
				                           "loading objects "
				                           "(files,directories,etc.) from the "
				                           "metadata file");
				fflush(stderr);
				if (fs_loadnodes(fd) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading metadata (node)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "EDGE 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_INFO,
				                           "loading names from the metadata file");
				fflush(stderr);
				if (fs_loadedges(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading metadata (edge)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "FREE 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				        LOG_INFO,
				        "loading deletion timestamps from the metadata file");
				fflush(stderr);
				if (fs_loadfree(fd) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading metadata (free)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "XATR 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				        LOG_INFO,
				        "loading extra attributes (xattr) from the metadata file");
				fflush(stderr);
				if (xattr_load(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading metadata (xattr)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "ACLS 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				        LOG_INFO,
				        "loading access control lists from the metadata file");
				fflush(stderr);
				if (fs_loadacls(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading access control lists");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "QUOT 1.0", 8) == 0) {
				lzfs_pretty_syslog(LOG_WARNING,
				                   "old quota entries found, ignoring");
				fseeko(fd, sleng, SEEK_CUR);
			} else if (memcmp(hdr, "QUOT 1.1", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				        LOG_INFO, "loading quota entries from the metadata file");
				fflush(stderr);
				if (fs_loadquotas(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR, "error reading quota entries");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "LOCK 1.0", 8) == 0) {
				fseeko(fd, sleng, SEEK_CUR);
			} else if (memcmp(hdr, "FLCK 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_ERR, "loading file locks from the metadata file");
				if (fs_loadlocks(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading metadata (chunks)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "CHNK 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				        LOG_INFO, "loading chunks data from the metadata file");
				fflush(stderr);
				bool loadLockIds = (fver == kMetadataVersionWithLockIds);
				if (chunk_load(fd, loadLockIds) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading metadata (chunks)");
#endif
					return -1;
				}
			} else {
				hdr[8] = 0;
				if (ignoreflag) {
					lzfs_pretty_syslog(LOG_WARNING,
					                   "unknown section found (leng:%" PRIu64
					                   ",name:%s) - all data from this section "
					                   "will be lost",
					                   sleng, hdr);
					fseeko(fd, sleng, SEEK_CUR);
				} else {
					lzfs_pretty_syslog(
					        LOG_ERR,
					        "error: unknown section found (leng:%" PRIu64
					        ",name:%s)",
					        sleng, hdr);
					return -1;
				}
			}
			if ((off_t)(offbegin + sleng) != ftello(fd)) {
				lzfs_pretty_syslog(
				        LOG_WARNING,
				        "not all section has been read - file corrupted");
				if (ignoreflag == 0) {
					return -1;
				}
			}
		}
	}

	lzfs_pretty_syslog_attempt(LOG_INFO,
	                           "checking filesystem consistency of the metadata file");
	fflush(stderr);
	gMetadata->root = fsnodes_id_to_node(SPECIAL_INODE_ROOT);
	if (gMetadata->root == NULL) {
		lzfs_pretty_syslog(LOG_ERR, "error reading metadata (root node not found)");
		return -1;
	}
	if (fs_checknodes(ignoreflag) < 0) {
		return -1;
	}
	return 0;
}

#ifndef METARESTORE
void fs_new(void) {
	uint32_t nodepos;
	statsrecord *sr;
	gMetadata->maxnodeid = SPECIAL_INODE_ROOT;
	gMetadata->metaversion = 1;
	gMetadata->nextsessionid = 1;
	gMetadata->root = new fsnode(TYPE_DIRECTORY);
	gMetadata->root->id = SPECIAL_INODE_ROOT;
	gMetadata->root->ctime = gMetadata->root->mtime = gMetadata->root->atime = main_time();
	gMetadata->root->goal = DEFAULT_GOAL;
	gMetadata->root->trashtime = DEFAULT_TRASHTIME;
	gMetadata->root->mode = 0777;
	gMetadata->root->uid = 0;
	gMetadata->root->gid = 0;
	sr = (statsrecord *)malloc(sizeof(statsrecord));
	passert(sr);
	memset(sr, 0, sizeof(statsrecord));
	gMetadata->root->data.ddata.stats = sr;
	gMetadata->root->data.ddata.children = NULL;
	gMetadata->root->data.ddata.elements = 0;
	gMetadata->root->data.ddata.nlink = 2;
	gMetadata->root->parents = NULL;
	nodepos = NODEHASHPOS(gMetadata->root->id);
	gMetadata->root->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = gMetadata->root;
	gMetadata->inode_pool.markAsAcquired(gMetadata->root->id);
	chunk_newfs();
	gMetadata->nodes = 1;
	gMetadata->dirnodes = 1;
	gMetadata->filenodes = 0;
	fs_checksum(ChecksumMode::kForceRecalculate);
	fsnodes_quota_register_inode(gMetadata->root);
}
#endif

int fs_emergency_storeall(const std::string &fname) {
	cstream_t fd(fopen(fname.c_str(), "w"));
	if (fd == nullptr) {
		return -1;
	}

	fs_store_fd(fd.get());

	if (ferror(fd.get()) != 0) {
		return -1;
	}
	lzfs_pretty_syslog(LOG_WARNING,
	                   "metadata were stored to emergency file: %s - please copy this file to "
	                   "your default location as '%s'",
	                   fname.c_str(), kMetadataFilename);
	return 0;
}

int fs_emergency_saves() {
#if defined(LIZARDFS_HAVE_PWD_H) && defined(LIZARDFS_HAVE_GETPWUID)
	struct passwd *p;
#endif
	if (fs_emergency_storeall(kMetadataEmergencyFilename) == 0) {
		return 0;
	}
#if defined(LIZARDFS_HAVE_PWD_H) && defined(LIZARDFS_HAVE_GETPWUID)
	p = getpwuid(getuid());
	if (p) {
		std::string fname = p->pw_dir;
		fname.append("/").append(kMetadataEmergencyFilename);
		if (fs_emergency_storeall(fname) == 0) {
			return 0;
		}
	}
#endif
	std::string metadata_emergency_filename = kMetadataEmergencyFilename;
	if (fs_emergency_storeall("/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/tmp/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/var/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/share/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/var/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/share/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	return -1;
}

#ifndef METARESTORE

/*
 * Load and apply changelogs.
 */
void fs_load_changelogs() {
	metadataserver::Personality personality = metadataserver::getPersonality();
	metadataserver::setPersonality(metadataserver::Personality::kShadow);
	/*
	 * We need to load 3 changelog files in extreme case.
	 * If we are being run as Shadow we need to download two
	 * changelog files:
	 * 1 - current changelog => "changelog.mfs.1"
	 * 2 - previous changelog in case Shadow connects during metadata dump,
	 *     that is "changelog.mfs.2"
	 * Beside this we received changelog lines that we stored in
	 * yet another changelog file => "changelog.mfs"
	 *
	 * If we are master we only really care for:
	 * "changelog.mfs.1" and "changelog.mfs" files.
	 */
	static const std::string changelogs[]{std::string(kChangelogFilename) + ".2",
	                                      std::string(kChangelogFilename) + ".1",
	                                      kChangelogFilename};
	restore_setverblevel(gVerbosity);
	bool oldExists = false;
	try {
		for (const std::string &s : changelogs) {
			std::string fullFileName =
			        fs::getCurrentWorkingDirectoryNoThrow() + "/" + s;
			if (fs::exists(s)) {
				oldExists = true;
				uint64_t first = changelogGetFirstLogVersion(s);
				uint64_t last = changelogGetLastLogVersion(s);
				if (last >= first) {
					if (last >= fs_getversion()) {
						fs_load_changelog(s);
					}
				} else {
					throw InitializeException(
					        "changelog " + fullFileName +
					        " inconsistent, "
					        "use mfsmetarestore to recover the filesystem; "
					        "current fs version: " +
					        std::to_string(fs_getversion()) +
					        ", first change in the file: " +
					        std::to_string(first));
				}
			} else if (oldExists && s != kChangelogFilename) {
				lzfs_pretty_syslog(LOG_WARNING, "changelog `%s' missing",
				                   fullFileName.c_str());
			}
		}
	} catch (const FilesystemException &ex) {
		throw FilesystemException("error loading changelogs: " + ex.message());
	}
	fs_storeall(MetadataDumper::DumpType::kForegroundDump);
	metadataserver::setPersonality(personality);
}

/*
 * Load and apply given changelog file.
 */
void fs_load_changelog(const std::string &path) {
	std::string fullFileName = fs::getCurrentWorkingDirectoryNoThrow() + "/" + path;
	std::ifstream changelog(path);
	std::string line;
	size_t end = 0;
	sassert(gMetadata->metaversion > 0);

	uint64_t first = 0;
	uint64_t id = 0;
	uint64_t skippedEntries = 0;
	uint64_t appliedEntries = 0;
	while (std::getline(changelog, line).good()) {
		id = stoull(line, &end);
		if (id < fs_getversion()) {
			++skippedEntries;
			continue;
		} else if (!first) {
			first = id;
		}
		++appliedEntries;
		uint8_t status = restore(path.c_str(), id, line.c_str() + end,
		                         RestoreRigor::kIgnoreParseErrors);
		if (status != LIZARDFS_STATUS_OK) {
			throw MetadataConsistencyException("can't apply changelog " + fullFileName,
			                                   status);
		}
	}
	if (appliedEntries > 0) {
		lzfs_pretty_syslog_attempt(LOG_NOTICE, "%s: %" PRIu64 " changes applied (%" PRIu64
		                                       " to %" PRIu64 "), %" PRIu64 " skipped",
		                           fullFileName.c_str(), appliedEntries, first, id,
		                           skippedEntries);
	} else if (skippedEntries > 0) {
		lzfs_pretty_syslog_attempt(LOG_NOTICE, "%s: skipped all %" PRIu64 " entries",
		                           fullFileName.c_str(), skippedEntries);
	} else {
		lzfs_pretty_syslog_attempt(LOG_NOTICE, "%s: file empty (ignored)",
		                           fullFileName.c_str());
	}
}

#endif

void fs_loadall(const std::string& fname,int ignoreflag) {
	cstream_t fd(fopen(fname.c_str(), "r"));
	std::string fnameWithPath;
	if (fname.front() == '/') {
		fnameWithPath = fname;
	} else {
		fnameWithPath = fs::getCurrentWorkingDirectoryNoThrow() + "/" + fname;
	}
	if (fd == nullptr) {
		throw FilesystemException("can't open metadata file: " + errorString(errno));
	}
	lzfs_pretty_syslog(LOG_INFO,"opened metadata file %s", fnameWithPath.c_str());
	uint8_t hdr[8];
	if (fread(hdr,1,8,fd.get())!=8) {
		throw MetadataConsistencyException("can't read metadata header");
	}
#ifndef METARESTORE
	if (metadataserver::isMaster()) {
		if (memcmp(hdr, "MFSM NEW", 8) == 0) {    // special case - create new file system
			fs_new();
			lzfs_pretty_syslog(LOG_NOTICE, "empty filesystem created");
			// after creating new filesystem always create "back" file for using in metarestore
			fs_storeall(MetadataDumper::kForegroundDump);
			return;
		}
	}
#endif /* #ifndef METARESTORE */
	uint8_t metadataVersion;
	if (memcmp(hdr,MFSSIGNATURE "M 1.5",8)==0) {
		metadataVersion = kMetadataVersionMooseFS;
	} else if (memcmp(hdr,MFSSIGNATURE "M 1.6",8)==0) {
		metadataVersion = kMetadataVersionLizardFS;
	} else if (memcmp(hdr,MFSSIGNATURE "M 2.0",8)==0) {
		metadataVersion = kMetadataVersionWithSections;
		/* Note LIZARDFSSIGNATURE instead of MFSSIGNATURE! */
	} else if (memcmp(hdr, LIZARDFSSIGNATURE "M 2.9", 8) == 0) {
		metadataVersion = kMetadataVersionWithLockIds;
	} else {
		throw MetadataConsistencyException("wrong metadata header version");
	}

	if (fs_load(fd.get(), ignoreflag, metadataVersion) < 0) {
		throw MetadataConsistencyException(MetadataStructureReadErrorMsg);
	}
	if (ferror(fd.get())!=0) {
		throw MetadataConsistencyException(MetadataStructureReadErrorMsg);
	}
	lzfs_pretty_syslog_attempt(LOG_INFO,"connecting files and chunks");
	fs_add_files_to_chunks();
	unlink(kMetadataTmpFilename);
	lzfs_pretty_syslog_attempt(LOG_INFO, "calculating checksum of the metadata");
	fs_checksum(ChecksumMode::kForceRecalculate);
#ifndef METARESTORE
	lzfs_pretty_syslog(LOG_INFO,
			"metadata file %s read ("
			"%" PRIu32 " inodes including "
			"%" PRIu32 " directory inodes and "
			"%" PRIu32 " file inodes, "
			"%" PRIu32 " chunks)",
			fnameWithPath.c_str(),
			gMetadata->nodes, gMetadata->dirnodes, gMetadata->filenodes, chunk_count());
#else
	lzfs_pretty_syslog(LOG_INFO, "metadata file %s read", fnameWithPath.c_str());
#endif
	return;
}

#ifndef METARESTORE

// Broadcasts information about status of the freshly finished
// metadata save process to interested modules.
void fs_broadcast_metadata_saved(uint8_t status) {
	matomlserv_broadcast_metadata_saved(status);
	matoclserv_broadcast_metadata_saved(status);
}

/*!
 * Commits successful metadata dump by renaming files.
 *
 * \return true iff up to date metadata.mfs file was created
 */
bool fs_commit_metadata_dump() {
	rotateFiles(kMetadataFilename, gStoredPreviousBackMetaCopies);
	try {
		fs::rename(kMetadataTmpFilename, kMetadataFilename);
		DEBUG_LOG("master.fs.stored");
		return true;
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_ERR, "renaming %s to %s failed: %s",
				kMetadataTmpFilename, kMetadataFilename, ex.what());
	}

	// The previous step didn't return, so let's try to save us in other way
	std::string alternativeName = kMetadataFilename + std::to_string(main_time());
	try {
		fs::rename(kMetadataTmpFilename, alternativeName);
		lzfs_pretty_syslog(LOG_ERR, "emergency metadata file created as %s", alternativeName.c_str());
		return false;
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_ERR, "renaming %s to %s failed: %s",
				kMetadataTmpFilename, alternativeName.c_str(), ex.what());
	}

	// Nothing can be done...
	lzfs_pretty_syslog_attempt(LOG_ERR, "trying to create emergency metadata file in foreground");
	fs_emergency_saves();
	return false;
}

// returns false in case of an error
uint8_t fs_storeall(MetadataDumper::DumpType dumpType) {
	if (gMetadata == nullptr) {
		// Periodic dump in shadow master or a request from lizardfs-admin
		syslog(LOG_INFO, "Can't save metadata because no metadata is loaded");
		return LIZARDFS_ERROR_NOTPOSSIBLE;
	}
	if (metadataDumper.inProgress()) {
		syslog(LOG_ERR, "previous metadata save process hasn't finished yet - do not start another one");
		return LIZARDFS_ERROR_TEMP_NOTPOSSIBLE;
	}

	fs_erase_message_from_lockfile(); // We are going to do some changes in the data dir right now
	changelog_rotate();
	matomlserv_broadcast_logrotate();
	// child == true says that we forked
	// bg may be changed to dump in foreground in case of a fork error
	bool child = metadataDumper.start(dumpType, fs_checksum(ChecksumMode::kGetCurrent));
	uint8_t status = LIZARDFS_STATUS_OK;

	if (dumpType == MetadataDumper::kForegroundDump) {
		cstream_t fd(fopen(kMetadataTmpFilename, "w"));
		if (fd == nullptr) {
			syslog(LOG_ERR, "can't open metadata file");
			// try to save in alternative location - just in case
			fs_emergency_saves();
			if (child) {
				exit(1);
			}
			fs_broadcast_metadata_saved(LIZARDFS_ERROR_IO);
			return LIZARDFS_ERROR_IO;
		}

		fs_store_fd(fd.get());

		if (ferror(fd.get()) != 0) {
			syslog(LOG_ERR, "can't write metadata");
			fd.reset();
			unlink(kMetadataTmpFilename);
			// try to save in alternative location - just in case
			fs_emergency_saves();
			if (child) {
				exit(1);
			}
			fs_broadcast_metadata_saved(LIZARDFS_ERROR_IO);
			return LIZARDFS_ERROR_IO;
		} else {
			if (fflush(fd.get()) == EOF) {
				lzfs_pretty_errlog(LOG_ERR, "metadata fflush failed");
			} else if (fsync(fileno(fd.get())) == -1) {
				lzfs_pretty_errlog(LOG_ERR, "metadata fsync failed");
			}
			fd.reset();
			if (!child) {
				// rename backups if no child was created, otherwise this is handled by pollServe
				status = fs_commit_metadata_dump() ? LIZARDFS_STATUS_OK : LIZARDFS_ERROR_IO;
			}
		}
		if (child) {
			printf("OK\n"); // give mfsmetarestore another chance
			exit(0);
		}
		fs_broadcast_metadata_saved(status);
	}
	sassert(!child);
	return status;
}

#endif
