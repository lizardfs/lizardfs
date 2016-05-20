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

#include "master/filesystem_freenode.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"

#ifdef METARESTORE

void fs_dumpedge(FSNodeDirectory *parent, FSNode *child, const std::string &name) {
	if (parent == NULL) {
		if (child->type == FSNode::kTrash) {
			printf("E|p:     TRASH|c:%10" PRIu32 "|n:%s\n", child->id,
			       fsnodes_escape_name(name).c_str());
		} else if (child->type == FSNode::kReserved) {
			printf("E|p:  RESERVED|c:%10" PRIu32 "|n:%s\n", child->id,
			       fsnodes_escape_name(name).c_str());
		} else {
			printf("E|p:      NULL|c:%10" PRIu32 "|n:%s\n", child->id,
			       fsnodes_escape_name(name).c_str());
		}
	} else {
		printf("E|p:%10" PRIu32 "|c:%10" PRIu32 "|n:%s\n", parent->id, child->id,
		       fsnodes_escape_name(name).c_str());
	}
}

void fs_dumpnode(FSNode *f) {
	char c;
	uint32_t i, ch;

	c = '?';
	switch (f->type) {
	case FSNode::kDirectory:
		c = 'D';
		break;
	case FSNode::kSocket:
		c = 'S';
		break;
	case FSNode::kFifo:
		c = 'F';
		break;
	case FSNode::kBlockDev:
		c = 'B';
		break;
	case FSNode::kCharDev:
		c = 'C';
		break;
	case FSNode::kSymlink:
		c = 'L';
		break;
	case FSNode::kFile:
		c = '-';
		break;
	case FSNode::kTrash:
		c = 'T';
		break;
	case FSNode::kReserved:
		c = 'R';
		break;
	}

	printf("%c|i:%10" PRIu32 "|#:%" PRIu8 "|e:%1" PRIX16 "|m:%04" PRIo16 "|u:%10" PRIu32
	       "|g:%10" PRIu32 "|a:%10" PRIu32 ",m:%10" PRIu32 ",c:%10" PRIu32 "|t:%10" PRIu32,
	       c, f->id, f->goal, (uint16_t)(f->mode >> 12), (uint16_t)(f->mode & 0xFFF), f->uid,
	       f->gid, f->atime, f->mtime, f->ctime, f->trashtime);

	if (f->type == FSNode::kBlockDev || f->type == FSNode::kCharDev) {
		printf("|d:%5" PRIu32 ",%5" PRIu32 "\n", static_cast<FSNodeDevice*>(f)->rdev >> 16,
		       static_cast<FSNodeDevice*>(f)->rdev & 0xFFFF);
	} else if (f->type == FSNode::kSymlink) {
		printf("|p:%s\n", fsnodes_escape_name((std::string)static_cast<FSNodeSymlink*>(f)->path).c_str());
	} else if (f->type == FSNode::kFile || f->type == FSNode::kTrash || f->type == FSNode::kReserved) {
		FSNodeFile *node_file = static_cast<FSNodeFile*>(f);
		printf("|l:%20" PRIu64 "|c:(", node_file->length);
		ch = node_file->chunkCount();
		for (i = 0; i < ch; i++) {
			if (node_file->chunks[i] != 0) {
				printf("%016" PRIX64, node_file->chunks[i]);
			} else {
				printf("N");
			}
			if (i + 1 < ch) {
				printf(",");
			}
		}
		printf(")|r:(");
		i = 0;
		for(const auto &sessionid : node_file->sessionid) {
			if (i > 0) {
				printf(",");
			}
			printf("%" PRIu32, sessionid);
			++i;
		}
		printf(")\n");
	} else {
		printf("\n");
	}
}

void fs_dumpnodes() {
	uint32_t i;
	FSNode *p;
	for (i = 0; i < NODEHASHSIZE; i++) {
		for (p = gMetadata->nodehash[i]; p; p = p->next) {
			fs_dumpnode(p);
		}
	}
}

void fs_dumpedgelist(FSNodeDirectory *parent) {
	for (const auto &entry : parent->entries) {
		fs_dumpedge(parent, entry.second, (std::string)entry.first);
	}
}

void fs_dumpedgelist(const TrashPathContainer &data) {
	for (const auto &entry : data) {
		FSNode *child = fsnodes_id_to_node(entry.first.id);
		fs_dumpedge(nullptr, child, (std::string)entry.second);
	}
}

void fs_dumpedgelist(const ReservedPathContainer &data) {
	for (const auto &entry : data) {
		FSNode *child = fsnodes_id_to_node(entry.first);
		fs_dumpedge(nullptr, child, (std::string)entry.second);
	}
}

void fs_dumpedges(FSNodeDirectory *parent) {
	fs_dumpedgelist(parent);
	for (const auto &entry : parent->entries) {
		FSNode *child = entry.second;
		if (child->type == FSNode::kDirectory) {
			fs_dumpedges(static_cast<FSNodeDirectory*>(child));
		}
	}
}

void fs_dumpfree() {
	for (const auto &n : gMetadata->inode_pool) {
		printf("I|i:%10" PRIu32 "|f:%10" PRIu32 "\n", n.id, n.ts);
	}
}

void xattr_dump() {
	uint32_t i;
	xattr_data_entry *xa;

	for (i = 0; i < XATTR_DATA_HASH_SIZE; i++) {
		for (xa = gMetadata->xattr_data_hash[i]; xa; xa = xa->next) {
			printf("X|i:%10" PRIu32 "|n:%s|v:%s\n", xa->inode,
			       fsnodes_escape_name(std::string((char*)xa->attrname, xa->anleng)).c_str(),
			       fsnodes_escape_name(std::string((char*)xa->attrvalue, xa->avleng)).c_str());
		}
	}
}

void fs_dump(void) {
	fs_dumpnodes();
	fs_dumpedges(gMetadata->root);
	fs_dumpedgelist(gMetadata->trash);
	fs_dumpedgelist(gMetadata->reserved);
	fs_dumpfree();
	xattr_dump();
}

#endif
