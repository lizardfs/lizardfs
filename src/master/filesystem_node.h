/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2017
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
#include "master/filesystem_node_types.h"
#include "master/filesystem_metadata.h"
#include "protocol/directory_entry.h"
#include "protocol/named_inode_entry.h"

inline uint32_t fsnodes_hash(uint32_t parentid, const hstorage::Handle &name) {
	return (parentid * 0x5F2318BD) + name.hash();
}

inline uint32_t fsnodes_hash(uint32_t parentid, const HString &name) {
	return (parentid * 0x5F2318BD) + (hstorage::Handle::HashType)name.hash();
}

namespace detail {

inline FSNode *fsnodes_id_to_node_internal(uint32_t id) {
	FSNode *p;
	uint32_t nodepos = NODEHASHPOS(id);
	for (p = gMetadata->nodehash[nodepos]; p; p = p->next) {
		if (p->id == id) {
			return p;
		}
	}
	return nullptr;
}

template<class NodeType>
inline void fsnodes_check_node_type(const NodeType *node) {
	assert(node);
	(void)node;
}

template<>
inline void fsnodes_check_node_type(const FSNodeFile *node) {
	assert(node && (node->type == FSNode::kFile || node->type == FSNode::kTrash || node->type == FSNode::kReserved));
	(void)node;
}

template<>
inline void fsnodes_check_node_type(const FSNodeDirectory *node) {
	assert(node && node->type == FSNode::kDirectory);
	(void)node;
}

template<>
inline void fsnodes_check_node_type(const FSNodeSymlink *node) {
	assert(node && node->type == FSNode::kSymlink);
	(void)node;
}

template<>
inline void fsnodes_check_node_type(const FSNodeDevice *node) {
	assert(node && (node->type == FSNode::kBlockDev || node->type == FSNode::kCharDev));
	(void)node;
}

} // detail

/// searches for an edge with given name (`name`) in given directory (`node`)
inline FSNode *fsnodes_lookup(FSNodeDirectory *node, const HString &name) {
	auto it = node->find(name);
	if (it != node->end()) {
		return (*it).second;
	}

	return nullptr;
}

template<class NodeType>
inline NodeType *fsnodes_id_to_node_verify(uint32_t id) {
	NodeType *node = static_cast<NodeType*>(detail::fsnodes_id_to_node_internal(id));
	detail::fsnodes_check_node_type(node);
	return node;
}

template<class NodeType = FSNode>
inline NodeType *fsnodes_id_to_node(uint32_t id) {
	return static_cast<NodeType*>(detail::fsnodes_id_to_node_internal(id));
}

inline void fsnodes_update_ctime(FSNode *node, uint32_t ctime) {
	if (node->type == FSNode::kTrash && node->ctime != ctime) {
		auto old_key = TrashPathKey(node);
		node->ctime = ctime;
		auto it = gMetadata->trash.find(old_key);
		if (it != gMetadata->trash.end()) {
			hstorage::Handle path = std::move((*it).second);
			gMetadata->trash.erase(it);
			gMetadata->trash.insert({TrashPathKey(node), std::move(path)});
		}
	} else {
		node->ctime = ctime;
	}
}

std::string fsnodes_escape_name(const std::string &name);
int fsnodes_purge(uint32_t ts, FSNode *p);
uint32_t fsnodes_getdetachedsize(const TrashPathContainer &data);
void fsnodes_getdetacheddata(const TrashPathContainer &data, uint8_t *dbuff);
void fsnodes_getdetacheddata(const TrashPathContainer &data, uint32_t off, uint32_t max_entries, std::vector<NamedInodeEntry> &entries);
uint32_t fsnodes_getdetachedsize(const ReservedPathContainer &data);
void fsnodes_getdetacheddata(const ReservedPathContainer &data, uint8_t *dbuff);
void fsnodes_getdetacheddata(const ReservedPathContainer &data, uint32_t off, uint32_t max_entries, std::vector<NamedInodeEntry> &entries);
void fsnodes_getpath(FSNodeDirectory *parent, FSNode *child, std::string &path);
void fsnodes_fill_attr(FSNode *node, FSNode *parent, uint32_t uid, uint32_t gid, uint32_t auid,
	uint32_t agid, uint8_t sesflags, Attributes &attr);
void fsnodes_fill_attr(const FsContext &context, FSNode *node, FSNode *parent, Attributes &attr);

uint8_t verify_session(const FsContext &context, OperationMode operationMode,
	SessionType sessionType);

uint8_t fsnodes_get_node_for_operation(const FsContext &context, ExpectedNodeType expectedNodeType,
	uint8_t modemask, uint32_t inode, FSNode **ret, FSNodeDirectory **ret_rn = nullptr);
uint8_t fsnodes_undel(uint32_t ts, FSNodeFile *node);

int fsnodes_namecheck(const std::string &name);
void fsnodes_get_stats(FSNode *node, statsrecord *sr);
bool fsnodes_isancestor_or_node_reserved_or_trash(FSNodeDirectory *f, FSNode *p);
int fsnodes_access(const FsContext &context, FSNode *node, uint8_t modemask);

void fsnodes_setlength(FSNodeFile *obj, uint64_t length);
void fsnodes_change_uid_gid(FSNode *p, uint32_t uid, uint32_t gid);
int fsnodes_nameisused(FSNodeDirectory *node, const HString &name);
bool fsnodes_inode_quota_exceeded(uint32_t uid, uint32_t gid);

FSNode *fsnodes_create_node(uint32_t ts, FSNodeDirectory *node, const HString &name,
			uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid,
			uint8_t copysgid, AclInheritance inheritacl, uint32_t req_inode=0);

void fsnodes_add_stats(FSNodeDirectory *parent, statsrecord *sr);
int fsnodes_sticky_access(FSNode *parent, FSNode *node, uint32_t uid);
void fsnodes_unlink(uint32_t ts, FSNodeDirectory *parent, const HString &node_name, FSNode *node);
bool fsnodes_isancestor(FSNodeDirectory *f, FSNode *p);
void fsnodes_remove_edge(uint32_t ts, FSNodeDirectory *parent, const HString &node_name, FSNode *node);
void fsnodes_link(uint32_t ts, FSNodeDirectory *parent, FSNode *child, const HString &name);

uint8_t fsnodes_appendchunks(uint32_t ts, FSNodeFile *dstobj, FSNodeFile *srcobj);
void fsnodes_changefilegoal(FSNodeFile *obj, uint8_t goal);
uint32_t fsnodes_getdirsize(const FSNodeDirectory *p, uint8_t withattr);
void fsnodes_getdirdata(uint32_t rootinode, uint32_t uid, uint32_t gid, uint32_t auid,
	uint32_t agid, uint8_t sesflags, FSNodeDirectory *p, uint8_t *dbuff,
	uint8_t withattr);
namespace legacy {
/**
 * This implementation was not removed so as to support pre-3.13 client (mfsmount) using
 * old LIZ_FUSE_GETDIR packet version (0 = kLegacyClient).
 */
void fsnodes_getdir(uint32_t rootinode, uint32_t uid, uint32_t gid, uint32_t auid,
		uint32_t agid, uint8_t sesflags, FSNodeDirectory *p,
		uint64_t first_entry, uint64_t number_of_entries,
		std::vector<legacy::DirectoryEntry> &dir_entries);
} // namespace legacy
void fsnodes_getdir(uint32_t rootinode, uint32_t uid, uint32_t gid, uint32_t auid,
		uint32_t agid, uint8_t sesflags, FSNodeDirectory *p,
		uint64_t first_entry, uint64_t number_of_entries,
		std::vector<DirectoryEntry> &dir_entries);
void fsnodes_checkfile(FSNodeFile *p, uint32_t chunkcount[CHUNK_MATRIX_SIZE]);

bool fsnodes_has_tape_goal(FSNode *node);
void fsnodes_add_sub_stats(FSNodeDirectory *parent, statsrecord *newsr, statsrecord *prevsr);

void fsnodes_getgoal_recursive(FSNode *node, uint8_t gmode, GoalStatistics &fgtab,
		GoalStatistics &dgtab);

void fsnodes_gettrashtime_recursive(FSNode *node, uint8_t gmode,
	TrashtimeMap &fileTrashtimes, TrashtimeMap &dirTrashtimes);
void fsnodes_geteattr_recursive(FSNode *node, uint8_t gmode, uint32_t feattrtab[16],
	uint32_t deattrtab[16]);
void fsnodes_enqueue_tape_copies(FSNode *node);
void fsnodes_setgoal_recursive(FSNode *node, uint32_t ts, uint32_t uid, uint8_t goal, uint8_t smode,
	uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
void fsnodes_settrashtime_recursive(FSNode *node, uint32_t ts, uint32_t uid, uint32_t trashtime,
	uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
	uint32_t *nsinodes);
void fsnodes_seteattr_recursive(FSNode *node, uint32_t ts, uint32_t uid, uint8_t eattr,
	uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
	uint32_t *nsinodes);
uint8_t fsnodes_deleteacl(FSNode *p, AclType type, uint32_t ts);

uint8_t fsnodes_setacl(FSNode *p, const RichACL &acl, uint32_t ts);
uint8_t fsnodes_setacl(FSNode *p, AclType type, const AccessControlList &acl, uint32_t ts);
uint8_t fsnodes_getacl(FSNode *p, RichACL &acl);

uint32_t fsnodes_getpath_size(FSNodeDirectory *parent, FSNode *child);
void fsnodes_getpath_data(FSNodeDirectory *parent, FSNode *child, uint8_t *path, uint32_t size);

int64_t fsnodes_get_size(FSNode *node);
FSNodeDirectory *fsnodes_get_first_parent(FSNode *node);
