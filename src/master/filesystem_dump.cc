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

void fs_dumpedge(fsedge *e) {
	if (e->parent == NULL) {
		if (e->child->type == TYPE_TRASH) {
			printf("E|p:     TRASH|c:%10" PRIu32 "|n:%s\n", e->child->id,
			       fsnodes_escape_name((std::string)e->name).c_str());
		} else if (e->child->type == TYPE_RESERVED) {
			printf("E|p:  RESERVED|c:%10" PRIu32 "|n:%s\n", e->child->id,
			       fsnodes_escape_name((std::string)e->name).c_str());
		} else {
			printf("E|p:      NULL|c:%10" PRIu32 "|n:%s\n", e->child->id,
			       fsnodes_escape_name((std::string)e->name).c_str());
		}
	} else {
		printf("E|p:%10" PRIu32 "|c:%10" PRIu32 "|n:%s\n", e->parent->id, e->child->id,
		       fsnodes_escape_name((std::string)e->name).c_str());
	}
}

void fs_dumpnode(fsnode *f) {
	char c;
	uint32_t i, ch;
	sessionidrec *sessionidptr;

	c = '?';
	switch (f->type) {
	case TYPE_DIRECTORY:
		c = 'D';
		break;
	case TYPE_SOCKET:
		c = 'S';
		break;
	case TYPE_FIFO:
		c = 'F';
		break;
	case TYPE_BLOCKDEV:
		c = 'B';
		break;
	case TYPE_CHARDEV:
		c = 'C';
		break;
	case TYPE_SYMLINK:
		c = 'L';
		break;
	case TYPE_FILE:
		c = '-';
		break;
	case TYPE_TRASH:
		c = 'T';
		break;
	case TYPE_RESERVED:
		c = 'R';
		break;
	}

	printf("%c|i:%10" PRIu32 "|#:%" PRIu8 "|e:%1" PRIX16 "|m:%04" PRIo16 "|u:%10" PRIu32
	       "|g:%10" PRIu32 "|a:%10" PRIu32 ",m:%10" PRIu32 ",c:%10" PRIu32 "|t:%10" PRIu32,
	       c, f->id, f->goal, (uint16_t)(f->mode >> 12), (uint16_t)(f->mode & 0xFFF), f->uid,
	       f->gid, f->atime, f->mtime, f->ctime, f->trashtime);

	if (f->type == TYPE_BLOCKDEV || f->type == TYPE_CHARDEV) {
		printf("|d:%5" PRIu32 ",%5" PRIu32 "\n", f->data.devdata.rdev >> 16,
		       f->data.devdata.rdev & 0xFFFF);
	} else if (f->type == TYPE_SYMLINK) {
		printf("|p:%s\n", fsnodes_escape_name((std::string)f->symlink_path()).c_str());
	} else if (f->type == TYPE_FILE || f->type == TYPE_TRASH || f->type == TYPE_RESERVED) {
		printf("|l:%20" PRIu64 "|c:(", f->data.fdata.length);
		ch = 0;
		for (i = 0; i < f->data.fdata.chunks; i++) {
			if (f->data.fdata.chunktab[i] != 0) {
				ch = i + 1;
			}
		}
		for (i = 0; i < ch; i++) {
			if (f->data.fdata.chunktab[i] != 0) {
				printf("%016" PRIX64, f->data.fdata.chunktab[i]);
			} else {
				printf("N");
			}
			if (i + 1 < ch) {
				printf(",");
			}
		}
		printf(")|r:(");
		for (sessionidptr = f->data.fdata.sessionids; sessionidptr;
		     sessionidptr = sessionidptr->next) {
			printf("%" PRIu32, sessionidptr->sessionid);
			if (sessionidptr->next) {
				printf(",");
			}
		}
		printf(")\n");
	} else {
		printf("\n");
	}
}

void fs_dumpnodes() {
	uint32_t i;
	fsnode *p;
	for (i = 0; i < NODEHASHSIZE; i++) {
		for (p = gMetadata->nodehash[i]; p; p = p->next) {
			fs_dumpnode(p);
		}
	}
}

void fs_dumpedgelist(fsedge *e) {
	while (e) {
		fs_dumpedge(e);
		e = e->nextchild;
	}
}

void fs_dumpedges(fsnode *f) {
	fsedge *e;
	fs_dumpedgelist(f->data.ddata.children);
	for (e = f->data.ddata.children; e; e = e->nextchild) {
		if (e->child->type == TYPE_DIRECTORY) {
			fs_dumpedges(e->child);
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
