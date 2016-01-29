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

#include <array>
#include <cstdint>
#include <unordered_map>
#include <memory>

#include "common/access_control_list.h"
#include "common/acl_type.h"
#include "common/attributes.h"
#include "common/extended_acl.h"
#include "common/goal.h"
#include "master/fs_context.h"
#include "master/hstring_storage.h"

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

typedef std::unordered_map<uint32_t, uint32_t> TrashtimeMap;
typedef std::array<uint32_t, GoalId::kMax + 1> GoalStatistics;

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
	hstorage::Handle name;

	fsedge() : name() {
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
			std::array<uint8_t, sizeof(hstorage::Handle)> path_storage;
			uint32_t pleng;
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
			new (data.sdata.path_storage.data()) hstorage::Handle();
		} else if (type == TYPE_FILE || type == TYPE_RESERVED || type == TYPE_TRASH) {
			data.fdata.chunktab = nullptr;
		}
	}

	~fsnode() {
		if (type == TYPE_DIRECTORY) {
			free(data.ddata.stats);
		} else if (type == TYPE_SYMLINK) {

			reinterpret_cast<hstorage::Handle*>(data.sdata.path_storage.data())->~Handle();
		} else if (type == TYPE_FILE || type == TYPE_RESERVED || type == TYPE_TRASH) {
			free(data.fdata.chunktab);
		}
	}

	hstorage::Handle& symlink_path() {
		return *reinterpret_cast<hstorage::Handle*>(data.sdata.path_storage.data());
	}
	const hstorage::Handle& symlink_path() const {
		return *reinterpret_cast<const hstorage::Handle*>(data.sdata.path_storage.data());
	}
};

void free(fsnode *);  // disable freeing using free at link time :)

inline uint32_t fsnodes_hash(uint32_t parentid, const hstorage::Handle &name) {
	return (parentid * 0x5F2318BD) + name.hash();
}

inline uint32_t fsnodes_hash(uint32_t parentid, const HString &name) {
	return (parentid * 0x5F2318BD) + (hstorage::Handle::HashType)name.hash();
}

std::string fsnodes_escape_name(const std::string &name);
int fsnodes_purge(uint32_t ts, fsnode *p);
uint32_t fsnodes_getdetachedsize(fsedge *start);
void fsnodes_getdetacheddata(fsedge *start, uint8_t *dbuff);
void fsnodes_getpath(fsedge *e, std::string &path);
fsnode *fsnodes_id_to_node(uint32_t id);
void fsnodes_fill_attr(fsnode *node, fsnode *parent, uint32_t uid, uint32_t gid, uint32_t auid,
	uint32_t agid, uint8_t sesflags, Attributes &attr);
void fsnodes_fill_attr(const FsContext &context, fsnode *node, fsnode *parent, Attributes &attr);

uint8_t verify_session(const FsContext &context, OperationMode operationMode,
	SessionType sessionType);

uint8_t fsnodes_get_node_for_operation(const FsContext &context, ExpectedNodeType expectedNodeType,
	uint8_t modemask, uint32_t inode, fsnode **ret);
uint8_t fsnodes_undel(uint32_t ts, fsnode *node);

int fsnodes_namecheck(const std::string &name);
fsedge *fsnodes_lookup(fsnode *node, const HString &name);
void fsnodes_get_stats(fsnode *node, statsrecord *sr);
bool fsnodes_isancestor_or_node_reserved_or_trash(fsnode *f, fsnode *p);
int fsnodes_access(fsnode *node, uint32_t uid, uint32_t gid, uint8_t modemask, uint8_t sesflags);

void fsnodes_setlength(fsnode *obj, uint64_t length);
void fsnodes_change_uid_gid(fsnode *p, uint32_t uid, uint32_t gid);
int fsnodes_nameisused(fsnode *node, const HString &name);
bool fsnodes_inode_quota_exceeded(uint32_t uid, uint32_t gid);

fsnode *fsnodes_create_node(uint32_t ts, fsnode *node, const HString &name,
			uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid,
			uint8_t copysgid, AclInheritance inheritacl, uint32_t req_inode=0);

void fsnodes_add_stats(fsnode *parent, statsrecord *sr);
int fsnodes_sticky_access(fsnode *parent, fsnode *node, uint32_t uid);
void fsnodes_unlink(uint32_t ts, fsedge *e);
bool fsnodes_isancestor(fsnode *f, fsnode *p);
void fsnodes_remove_edge(uint32_t ts, fsedge *e);
void fsnodes_link(uint32_t ts, fsnode *parent, fsnode *child, const HString &name);

uint8_t fsnodes_appendchunks(uint32_t ts, fsnode *dstobj, fsnode *srcobj);
uint32_t fsnodes_getdirsize(fsnode *p, uint8_t withattr);
void fsnodes_getdirdata(uint32_t rootinode, uint32_t uid, uint32_t gid, uint32_t auid,
	uint32_t agid, uint8_t sesflags, fsnode *p, uint8_t *dbuff,
	uint8_t withattr);
void fsnodes_checkfile(fsnode *p, uint32_t chunkcount[CHUNK_MATRIX_SIZE]);

bool fsnodes_has_tape_goal(fsnode *node);
void fsnodes_add_sub_stats(fsnode *parent, statsrecord *newsr, statsrecord *prevsr);

void fsnodes_getgoal_recursive(fsnode *node, uint8_t gmode, GoalStatistics &fgtab,
		GoalStatistics &dgtab);

void fsnodes_gettrashtime_recursive(fsnode *node, uint8_t gmode,
	TrashtimeMap &fileTrashtimes, TrashtimeMap &dirTrashtimes);
void fsnodes_geteattr_recursive(fsnode *node, uint8_t gmode, uint32_t feattrtab[16],
	uint32_t deattrtab[16]);
void fsnodes_setgoal_recursive(fsnode *node, uint32_t ts, uint32_t uid, uint8_t goal, uint8_t smode,
	uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
void fsnodes_settrashtime_recursive(fsnode *node, uint32_t ts, uint32_t uid, uint32_t trashtime,
	uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
	uint32_t *nsinodes);
void fsnodes_seteattr_recursive(fsnode *node, uint32_t ts, uint32_t uid, uint8_t eattr,
	uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
	uint32_t *nsinodes);
uint8_t fsnodes_deleteacl(fsnode *p, AclType type, uint32_t ts);

uint8_t fsnodes_setacl(fsnode *p, AclType type, AccessControlList acl, uint32_t ts);
uint8_t fsnodes_getacl(fsnode *p, AclType type, AccessControlList &acl);

uint32_t fsnodes_getpath_size(fsedge *e);
void fsnodes_getpath_data(fsedge *e, uint8_t *path, uint32_t size);

int64_t fsnodes_get_size(fsnode *node);

