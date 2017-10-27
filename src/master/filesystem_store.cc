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
#include <fstream>
#include <vector>

#include "common/cwrap.h"
#include "common/event_loop.h"
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
#include "master/filesystem_quota.h"
#include "master/filesystem_store_acl.h"
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
				lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
				return;
			}
			if (fwrite(xa->attrname, 1, xa->anleng, fd) != (size_t)(xa->anleng)) {
				lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
				return;
			}
			if (xa->avleng > 0) {
				if (fwrite(xa->attrvalue, 1, xa->avleng, fd) !=
				    (size_t)(xa->avleng)) {
					lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
					return;
				}
			}
		}
	}
	memset(hdrbuff, 0, 4 + 1 + 4);
	if (fwrite(hdrbuff, 1, 4 + 1 + 4, fd) != (size_t)(4 + 1 + 4)) {
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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

void fs_storeedge(FSNodeDirectory* parent, FSNode* child, const std::string &name, FILE *fd) {
	uint8_t uedgebuff[4 + 4 + 2 + 65535];
	uint8_t *ptr;
	if (child == nullptr) {  // last edge
		memset(uedgebuff, 0, 4 + 4 + 2);
		if (fwrite(uedgebuff, 1, 4 + 4 + 2, fd) != (size_t)(4 + 4 + 2)) {
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		return;
	}
	ptr = uedgebuff;
	if (parent == nullptr) {
		put32bit(&ptr, 0);
	} else {
		put32bit(&ptr, parent->id);
	}
	put32bit(&ptr, child->id);
	put16bit(&ptr, name.length());
	memcpy(ptr, name.c_str(), name.length());
	if (fwrite(uedgebuff, 1, 4 + 4 + 2 + name.length(), fd) != (size_t)(4 + 4 + 2 + name.length())) {
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

int fs_loadedge(FILE *fd, int ignoreflag) {
	uint8_t uedgebuff[4 + 4 + 2];
	const uint8_t *ptr;
	uint32_t parent_id;
	uint32_t child_id;
	statsrecord sr;
	static uint32_t current_parent_id;

	if (fd == NULL) {
		current_parent_id = 0;
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
	auto nleng = get16bit(&ptr);
	if (nleng == 0) {
		lzfs_pretty_syslog(LOG_ERR,
		                   "loading edge: %" PRIu32 "->%" PRIu32 " error: empty name",
		                   parent_id, child_id);
		return -1;
	}
	std::vector<char> name_buffer(nleng);
	if (fread(name_buffer.data(), 1, nleng, fd) != nleng) {
		lzfs_pretty_errlog(LOG_ERR, "loading edge: read error");
		return -1;
	}

	std::string name(name_buffer.begin(), name_buffer.end());

	FSNode* child = fsnodes_id_to_node(child_id);

	if (child == nullptr) {
		lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
		                            " error: child not found",
		                   parent_id, fsnodes_escape_name(name).c_str(), child_id);
		if (ignoreflag) {
			return 0;
		}
		return -1;
	}

	if (parent_id == 0) {
		if (child->type == FSNode::kTrash) {
			gMetadata->trash.insert({TrashPathKey(child), hstorage::Handle(name)});

			gMetadata->trashspace += static_cast<FSNodeFile*>(child)->length;
			gMetadata->trashnodes++;
		} else if (child->type == FSNode::kReserved) {
			gMetadata->reserved.insert({child->id, hstorage::Handle(name)});

			gMetadata->reservedspace += static_cast<FSNodeFile*>(child)->length;
			gMetadata->reservednodes++;
		} else {
			lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
			                            " error: bad child type (%c)\n",
			                   parent_id, fsnodes_escape_name(name).c_str(),
			                   child_id, child->type);
			return -1;
		}
	} else {
		FSNodeDirectory *parent = fsnodes_id_to_node<FSNodeDirectory>(parent_id);

		if (parent == NULL) {
			lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
			                            " error: parent not found",
			                   parent_id, fsnodes_escape_name(name).c_str(),
			                   child_id);
			if (ignoreflag) {
				parent = fsnodes_id_to_node<FSNodeDirectory>(SPECIAL_INODE_ROOT);
				if (parent == NULL || parent->type != FSNode::kDirectory) {
					lzfs_pretty_syslog(
					        LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
					                 " root dir not found !!!",
					        parent_id, fsnodes_escape_name(name).c_str(),
					        child_id);
					return -1;
				}
				lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
				                            " attaching node to root dir",
				                   parent_id,
				                   fsnodes_escape_name(name).c_str(),
				                   child_id);
				parent_id = SPECIAL_INODE_ROOT;
			} else {
				lzfs_pretty_syslog(LOG_ERR,
				                   "use mfsmetarestore (option -i) to attach this "
				                   "node to root dir\n");
				return -1;
			}
		}
		if (parent->type != FSNode::kDirectory) {
			lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
			                            " error: bad parent type (%c)",
			                   parent_id, fsnodes_escape_name(name).c_str(),
			                   child_id, parent->type);
			if (ignoreflag) {
				parent = fsnodes_id_to_node<FSNodeDirectory>(SPECIAL_INODE_ROOT);
				if (parent == NULL || parent->type != FSNode::kDirectory) {
					lzfs_pretty_syslog(
					        LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
					                 " root dir not found !!!",
					        parent_id, fsnodes_escape_name(name).c_str(),
					        child_id);
					return -1;
				}
				lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
				                            " attaching node to root dir",
				                   parent_id,
				                   fsnodes_escape_name(name).c_str(),
				                   child_id);
				parent_id = SPECIAL_INODE_ROOT;
			} else {
				lzfs_pretty_syslog(LOG_ERR,
				                   "use mfsmetarestore (option -i) to attach this "
				                   "node to root dir\n");
				return -1;
			}
		}
		if (current_parent_id != parent_id) {
			if (parent->entries.size() > 0) {
				lzfs_pretty_syslog(LOG_ERR, "loading edge: %" PRIu32 ",%s->%" PRIu32
				                " error: parent node sequence error",
				       parent_id, fsnodes_escape_name(name).c_str(), child_id);
				return -1;
			}
			current_parent_id = parent_id;
		}

		auto it = parent->entries.insert({hstorage::Handle(name), child}).first;
		parent->entries_hash ^= (*it).first.hash();

		child->parent.push_back(parent->id);

		if (child->type == FSNode::kDirectory) {
			parent->nlink++;
		}

		fsnodes_get_stats(child, &sr);
		fsnodes_add_stats(parent, &sr);

	}
	return 0;
}

void fs_storenode(FSNode *f, FILE *fd) {
	uint8_t unodebuff[1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2 + 8 * 65536 +
	                  4 * 65536 + 4];
	uint8_t *ptr, *chptr;
	uint32_t i, indx, ch, sessionids;
	std::string name;

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

	FSNodeFile *node_file = static_cast<FSNodeFile*>(f);

	switch (f->type) {
	case FSNode::kDirectory:
	case FSNode::kSocket:
	case FSNode::kFifo:
		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4)) {
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
		put32bit(&ptr, static_cast<FSNodeDevice*>(f)->rdev);
		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4)) {
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		break;
	case FSNode::kSymlink:
		name = (std::string)static_cast<FSNodeSymlink*>(f)->path;
		put32bit(&ptr, name.length());
		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4)) {
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		if (fwrite(name.c_str(), 1, name.length(), fd) !=
		    (size_t)(static_cast<FSNodeSymlink*>(f)->path_length)) {
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
			return;
		}
		break;
	case FSNode::kFile:
	case FSNode::kTrash:
	case FSNode::kReserved:
		put64bit(&ptr, node_file->length);
		ch = node_file->chunkCount();
		put32bit(&ptr, ch);
		sessionids = std::min<int>(node_file->sessionid.size(), 65535);
		put16bit(&ptr, sessionids);

		if (fwrite(unodebuff, 1, 1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2, fd) !=
		    (size_t)(1 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2)) {
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
			return;
		}

		indx = 0;
		while (ch > 65536) {
			chptr = ptr;
			for (i = 0; i < 65536; i++) {
				put64bit(&chptr, node_file->chunks[indx]);
				indx++;
			}
			if (fwrite(ptr, 1, 8 * 65536, fd) != (size_t)(8 * 65536)) {
				lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
				return;
			}
			ch -= 65536;
		}

		chptr = ptr;
		for (i = 0; i < ch; i++) {
			put64bit(&chptr, node_file->chunks[indx]);
			indx++;
		}

		sessionids = 0;
		for(const auto &sid : node_file->sessionid) {
			if (sessionids >= 65535) {
				break;
			}
			put32bit(&chptr, sid);
			sessionids++;
		}

		if (fwrite(ptr, 1, 8 * ch + 4 * sessionids, fd) !=
		    (size_t)(8 * ch + 4 * sessionids)) {
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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
	FSNode *p;
	uint32_t nodepos;
	std::vector<char> name_buffer;

	if (fd == NULL) {
		return 0;
	}

	type = fgetc(fd);
	if (type == 0) {  // last node
		return 1;
	}
	switch (type) {
	case FSNode::kDirectory:
	case FSNode::kFifo:
	case FSNode::kSocket:
		if (fread(unodebuff, 1, 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			return -1;
		}
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
	case FSNode::kSymlink:
		if (fread(unodebuff, 1, 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4, fd) !=
		    4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			return -1;
		}
		break;
	case FSNode::kFile:
	case FSNode::kTrash:
	case FSNode::kReserved:
		if (fread(unodebuff, 1, 4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2, fd) !=
		    4 + 1 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 2) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			return -1;
		}
		break;
	default:
		lzfs_pretty_syslog(LOG_ERR, "loading node: unrecognized node type: %c", type);
		return -1;
	}
	p = FSNode::create(type);
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
	FSNodeFile *node_file = static_cast<FSNodeFile*>(p);
	switch (type) {
	case FSNode::kDirectory:
		break;
	case FSNode::kSocket:
	case FSNode::kFifo:
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
		static_cast<FSNodeDevice*>(p)->rdev = get32bit(&ptr);
		break;
	case FSNode::kSymlink:
		pleng = get32bit(&ptr);
		static_cast<FSNodeSymlink*>(p)->path_length = pleng;

		if (pleng > 0) {
			name_buffer.resize(pleng);
			if (fread(name_buffer.data(), 1, pleng, fd) != pleng) {
				lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
				FSNode::destroy(p);
				return -1;
			}
			static_cast<FSNodeSymlink*>(p)->path = HString(name_buffer.begin(), name_buffer.end());
		}
		break;
	case FSNode::kFile:
	case FSNode::kTrash:
	case FSNode::kReserved:
		node_file->length = get64bit(&ptr);
		ch = get32bit(&ptr);
		sessionids = get16bit(&ptr);
		node_file->chunks.resize(ch);

		indx = 0;
		while (ch > 65536) {
			chptr = ptr;
			if (fread((uint8_t *)ptr, 1, 8 * 65536, fd) != 8 * 65536) {
				lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
				FSNode::destroy(p);
				return -1;
			}
			for (i = 0; i < 65536; i++) {
				node_file->chunks[indx] = get64bit(&chptr);
				indx++;
			}
			ch -= 65536;
		}
		if (fread((uint8_t *)ptr, 1, 8 * ch + 4 * sessionids, fd) !=
		    8 * ch + 4 * sessionids) {
			lzfs_pretty_errlog(LOG_ERR, "loading node: read error");
			FSNode::destroy(p);
			return -1;
		}
		for (i = 0; i < ch; i++) {
			node_file->chunks[indx] = get64bit(&ptr);
			indx++;
		}
		while (sessionids) {
			sessionid = get32bit(&ptr);
			node_file->sessionid.push_back(sessionid);
#ifndef METARESTORE
			matoclserv_add_open_file(sessionid, p->id);
#endif
			sessionids--;
		}
		fsnodes_quota_update(p, {{QuotaResource::kSize, +fsnodes_get_size(p)}});
	}
	nodepos = NODEHASHPOS(p->id);
	p->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = p;
	gMetadata->inode_pool.markAsAcquired(p->id);
	gMetadata->nodes++;
	if (type == FSNode::kDirectory) {
		gMetadata->dirnodes++;
	}
	if (type == FSNode::kFile || type == FSNode::kTrash || type == FSNode::kReserved) {
		gMetadata->filenodes++;
	}
	fsnodes_quota_update(p, {{QuotaResource::kInodes, +1}});
	return 0;
}

void fs_storenodes(FILE *fd) {
	uint32_t i;
	FSNode *p;
	for (i = 0; i < NODEHASHSIZE; i++) {
		for (p = gMetadata->nodehash[i]; p; p = p->next) {
			fs_storenode(p, fd);
		}
	}
	fs_storenode(NULL, fd);  // end marker
}

void fs_storeedgelist(FSNodeDirectory *parent, FILE *fd) {
	for (const auto &entry : parent->entries) {
		fs_storeedge(parent, entry.second, (std::string)entry.first, fd);
	}
}

void fs_storeedgelist(const TrashPathContainer &data, FILE *fd) {
	for (const auto &entry : data) {
		FSNode *child = fsnodes_id_to_node(entry.first.id);
		fs_storeedge(nullptr, child, (std::string)entry.second, fd);
	}
}

void fs_storeedgelist(const ReservedPathContainer &data, FILE *fd) {
	for (const auto &entry : data) {
		FSNode *child = fsnodes_id_to_node(entry.first);
		fs_storeedge(nullptr, child, (std::string)entry.second, fd);
	}
}

void fs_storeedges_rec(FSNodeDirectory *f, FILE *fd) {
	fs_storeedgelist(f, fd);
	for(const auto &entry : f->entries) {
		if (entry.second->type == FSNode::kDirectory) {
			fs_storeedges_rec(static_cast<FSNodeDirectory*>(entry.second), fd);
		}
	}
}

void fs_storeedges(FILE *fd) {
	fs_storeedges_rec(gMetadata->root, fd);
	fs_storeedgelist(gMetadata->trash, fd);
	fs_storeedgelist(gMetadata->reserved, fd);
	fs_storeedge(nullptr, nullptr, std::string(), fd);  // end marker
}

static void fs_storequotas(FILE *fd) {
	const std::vector<QuotaEntry> &entries = gMetadata->quota_database.getEntries();
	fs_store_generic(fd, entries);
}

static void fs_storelocks(FILE *fd) {
	gMetadata->flock_locks.store(fd);
	gMetadata->posix_locks.store(fd);
}

int fs_lostnode(FSNode *p) {
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
		HString name((const char*)artname, l);
		if (!fsnodes_nameisused(gMetadata->root, name)) {
			fsnodes_link(0, gMetadata->root, p, name);
			return 1;
		}
		i++;
	} while (i);
	return -1;
}

int fs_checknodes(int ignoreflag) {
	uint32_t i;
	FSNode *p;
	for (i = 0; i < NODEHASHSIZE; i++) {
		for (p = gMetadata->nodehash[i]; p; p = p->next) {
			if (p->parent.empty() && p != gMetadata->root && (p->type != FSNode::kTrash) && (p->type != FSNode::kReserved)) {
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

static int fs_loadquotas(FILE *fd, int ignoreflag) {
	try {
		std::vector<QuotaEntry> entries;
		fs_load_generic(fd, entries);
		for (const auto &entry : entries) {
			gMetadata->quota_database.set(entry.entryKey.owner.ownerType,
			                              entry.entryKey.owner.ownerId, entry.entryKey.rigor,
			                              entry.entryKey.resource, entry.limit);
		}
		gMetadata->quota_checksum = gMetadata->quota_database.checksum();
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
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
		return;
	}
	l = 0;
	ptr = wbuff;

	for (const auto &n : gMetadata->inode_pool) {
		if (l == 1024) {
			if (fwrite(wbuff, 1, 8 * 1024, fd) != (size_t)(8 * 1024)) {
				lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
			return;
		}
	}
}

int fs_loadfree(FILE *fd, uint64_t section_size = 0) {
	uint8_t rbuff[8 * 1024];
	const uint8_t *ptr;
	uint32_t l, t;

	if (fread(rbuff, 1, 4, fd) != 4) {
		lzfs_pretty_errlog(LOG_ERR, "loading free nodes: read error");
		return -1;
	}
	ptr = rbuff;
	t = get32bit(&ptr);

	if (section_size && t != (section_size - 4) / 8) {
		lzfs_pretty_errlog(LOG_INFO, "loading free nodes: section size doesn't match number of free nodes");
		t = (section_size - 4) / 8;
	}

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
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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
		fs_store_acls(fd);
		if (process_section("ACLS 1.2", hdr, ptr, offbegin, offend, fd) != LIZARDFS_STATUS_OK) {
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
			lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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
		lzfs_pretty_syslog(LOG_NOTICE, "fwrite error");
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
		    LOG_INFO, "loading objects (files,directories,etc.) from the metadata file");
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
		lzfs_pretty_syslog_attempt(LOG_INFO, "loading deletion timestamps from the metadata file");
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
#ifndef METARESTORE
			lzfs_pretty_syslog(LOG_ERR, "error reading metadata (chunks)");
#endif
			return -1;
		}
	} else { // metadata with sections
		while (1) {
			if (fread(hdr, 1, 16, fd) != 16) {
				lzfs_pretty_syslog(LOG_ERR, "error reading section header from the metadata file");
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
					lzfs_pretty_syslog(LOG_ERR, "error reading metadata (node)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "EDGE 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_INFO, "loading names from the metadata file");
				fflush(stderr);
				if (fs_loadedges(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR, "error reading metadata (edge)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "FREE 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_INFO,
				                           "loading deletion timestamps from the metadata file");
				fflush(stderr);
				if (fs_loadfree(fd, sleng) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR, "error reading metadata (free)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "XATR 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				    LOG_INFO, "loading extra attributes (xattr) from the metadata file");
				fflush(stderr);
				if (xattr_load(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR, "error reading metadata (xattr)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "ACLS 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_INFO,
				                           "loading access control lists from the metadata file");
				fflush(stderr);
				if (fs_load_legacy_acls(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR,
					                   "error reading access control lists");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "ACLS 1.1", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				        LOG_INFO,
				        "loading access control lists from the metadata file");
				fflush(stderr);
				if (fs_load_posix_acls(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR, "error reading access control lists");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "ACLS 1.2", 8) == 0) {
				lzfs_pretty_syslog_attempt(
				        LOG_INFO,
				        "loading access control lists from the metadata file");
				fflush(stderr);
				if (fs_load_acls(fd, ignoreflag) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR, "error reading access control lists");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "QUOT 1.0", 8) == 0) {
				lzfs_pretty_syslog(LOG_WARNING, "old quota entries found, ignoring");
				fseeko(fd, sleng, SEEK_CUR);
			} else if (memcmp(hdr, "QUOT 1.1", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_INFO,
				                           "loading quota entries from the metadata file");
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
					lzfs_pretty_syslog(LOG_ERR, "error reading metadata (chunks)");
#endif
					return -1;
				}
			} else if (memcmp(hdr, "CHNK 1.0", 8) == 0) {
				lzfs_pretty_syslog_attempt(LOG_INFO, "loading chunks data from the metadata file");
				fflush(stderr);
				bool loadLockIds = (fver == kMetadataVersionWithLockIds);
				if (chunk_load(fd, loadLockIds) < 0) {
#ifndef METARESTORE
					lzfs_pretty_syslog(LOG_ERR, "error reading metadata (chunks)");
#endif
					return -1;
				}
			} else {
				hdr[8] = 0;
				if (ignoreflag) {
					lzfs_pretty_syslog(LOG_WARNING, "unknown section found (leng:%" PRIu64
					                                ",name:%s) - all data from this section "
					                                "will be lost",
					                   sleng, hdr);
					fseeko(fd, sleng, SEEK_CUR);
				} else {
					lzfs_pretty_syslog(LOG_ERR,
					                   "error: unknown section found (leng:%" PRIu64 ",name:%s)",
					                   sleng, hdr);
					return -1;
				}
			}
			if ((off_t)(offbegin + sleng) != ftello(fd)) {
				lzfs_pretty_syslog(LOG_WARNING, "not all section has been read - file corrupted");
				if (ignoreflag == 0) {
					return -1;
				}
			}
		}
	}

	lzfs_pretty_syslog_attempt(LOG_INFO,
	                           "checking filesystem consistency of the metadata file");
	fflush(stderr);
	gMetadata->root = fsnodes_id_to_node<FSNodeDirectory>(SPECIAL_INODE_ROOT);
	if (gMetadata->root == NULL) {
		lzfs_pretty_syslog(LOG_ERR, "error reading metadata (root node not found)");
		return -1;
	}
	if (gMetadata->root->type != FSNode::kDirectory) {
		lzfs_pretty_syslog(LOG_ERR, "error reading metadata (root node not a directory)");
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
	gMetadata->maxnodeid = SPECIAL_INODE_ROOT;
	gMetadata->metaversion = 1;
	gMetadata->nextsessionid = 1;
	gMetadata->root = static_cast<FSNodeDirectory*>(FSNode::create(FSNode::kDirectory));
	gMetadata->root->id = SPECIAL_INODE_ROOT;
	gMetadata->root->ctime = gMetadata->root->mtime = gMetadata->root->atime = eventloop_time();
	gMetadata->root->goal = DEFAULT_GOAL;
	gMetadata->root->trashtime = DEFAULT_TRASHTIME;
	gMetadata->root->mode = 0777;
	gMetadata->root->uid = 0;
	gMetadata->root->gid = 0;
	nodepos = NODEHASHPOS(gMetadata->root->id);
	gMetadata->root->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = gMetadata->root;
	gMetadata->inode_pool.markAsAcquired(gMetadata->root->id);
	chunk_newfs();
	gMetadata->nodes = 1;
	gMetadata->dirnodes = 1;
	gMetadata->filenodes = 0;
	fs_checksum(ChecksumMode::kForceRecalculate);
	fsnodes_quota_update(gMetadata->root, {{QuotaResource::kInodes, +1}});
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
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.stored");
		return true;
	} catch (Exception& ex) {
		lzfs_pretty_syslog(LOG_ERR, "renaming %s to %s failed: %s",
				kMetadataTmpFilename, kMetadataFilename, ex.what());
	}

	// The previous step didn't return, so let's try to save us in other way
	std::string alternativeName = kMetadataFilename + std::to_string(eventloop_time());
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
		lzfs_pretty_syslog(LOG_INFO, "Can't save metadata because no metadata is loaded");
		return LIZARDFS_ERROR_NOTPOSSIBLE;
	}
	if (metadataDumper.inProgress()) {
		lzfs_pretty_syslog(LOG_ERR, "previous metadata save process hasn't finished yet - do not start another one");
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
			lzfs_pretty_syslog(LOG_ERR, "can't open metadata file");
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
			lzfs_pretty_syslog(LOG_ERR, "can't write metadata");
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
