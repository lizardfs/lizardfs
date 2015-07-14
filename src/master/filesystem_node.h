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

#pragma once

#include "common/platform.h"

#include <cstdint>
#include <memory>

#include "common/access_control_list.h"
#include "common/acl_type.h"
#include "common/attributes.h"
#include "common/extended_acl.h"
#include "common/goal_map.h"
#include "master/filesystem_bst.h"
#include "master/fs_context.h"

#define NODEHASHBITS (22)
#define NODEHASHSIZE (1 << NODEHASHBITS)
#define NODEHASHPOS(nodeid) ((nodeid) & (NODEHASHSIZE - 1))
#define NODECHECKSUMSEED 12345

#define EDGEHASHBITS (22)
#define EDGEHASHSIZE (1 << EDGEHASHBITS)
#define EDGEHASHPOS(hash) ((hash) & (EDGEHASHSIZE - 1))
#define EDGECHECKSUMSEED 1231241261

#define MAX_INDEX 0x7FFFFFFF

enum class AclInheritance { kInheritAcl, kDontInheritAcl };

// Arguments for verify_session
enum class SessionType { kNotMeta, kOnlyMeta, kAny };
enum class OperationMode { kReadWrite, kReadOnly };
enum class ExpectedNodeType { kFile, kDirectory, kNotDirectory, kAny };

class fsnode;

typedef struct _sessionidrec {
	uint32_t sessionid;
	struct _sessionidrec *next;
} sessionidrec;

struct fsedge {
	fsnode *child, *parent;
	struct fsedge *nextchild, *nextparent;
	struct fsedge **prevchild, **prevparent;
	struct fsedge *next, **prev;
	uint64_t checksum;
	uint16_t nleng;
	uint8_t *name;

	fsedge() : name(nullptr) {
	}

	~fsedge() {
		free(name);
	}
};
void free(fsedge *);  // disable freeing using free at link time :)

struct statsrecord {
	uint32_t inodes;
	uint32_t dirs;
	uint32_t files;
	uint32_t chunks;
	uint64_t length;
	uint64_t size;
	uint64_t realsize;
};

class fsnode {
public:
	std::unique_ptr<ExtendedAcl> extendedAcl;
	std::unique_ptr<AccessControlList> defaultAcl;
	uint32_t id;
	uint32_t ctime, mtime, atime;
	uint8_t type;
	uint8_t goal;
	uint16_t mode;  // only 12 lowest bits are used for mode, in unix standard upper 4 are used
	                // for object type, but since there is field "type" this bits can be used as
	                // extra flags
	uint32_t uid;
	uint32_t gid;
	uint32_t trashtime;
	union _data {
		struct _ddata {  // type==TYPE_DIRECTORY
			fsedge *children;
			uint32_t nlink;
			uint32_t elements;
			statsrecord *stats;
		} ddata;
		struct _sdata {  // type==TYPE_SYMLINK
			uint32_t pleng;
			uint8_t *path;
		} sdata;
		struct _devdata {
			uint32_t rdev;  // type==TYPE_BLOCKDEV ; type==TYPE_CHARDEV
		} devdata;
		struct _fdata {  // type==TYPE_FILE ; type==TYPE_TRASH ; type==TYPE_RESERVED
			uint64_t length;
			uint64_t *chunktab;
			uint32_t chunks;
			sessionidrec *sessionids;
		} fdata;
	} data;
	fsedge *parents;
	fsnode *next;
	uint64_t checksum;

	fsnode(uint8_t type) : type(type) {
		if (type == TYPE_DIRECTORY) {
			data.ddata.stats = nullptr;
		} else if (type == TYPE_SYMLINK) {
			data.sdata.path = nullptr;
		} else if (type == TYPE_FILE || type == TYPE_RESERVED || type == TYPE_TRASH) {
			data.fdata.chunktab = nullptr;
		}
	}

	~fsnode() {
		if (type == TYPE_DIRECTORY) {
			free(data.ddata.stats);
		} else if (type == TYPE_SYMLINK) {
			free(data.sdata.path);
		} else if (type == TYPE_FILE || type == TYPE_RESERVED || type == TYPE_TRASH) {
			free(data.fdata.chunktab);
		}
	}
};

void free(fsnode *);  // disable freeing using free at link time :)

inline uint32_t fsnodes_hash(uint32_t parentid, uint16_t nleng, const uint8_t *name) {
	uint32_t hash, i;
	hash = ((parentid * 0x5F2318BD) + nleng);
	for (i = 0; i < nleng; i++) {
		hash = hash * 33 + name[i];
	}
	return hash;
}

