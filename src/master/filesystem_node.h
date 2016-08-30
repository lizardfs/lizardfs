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
#include "common/compact_vector.h"

#ifdef LIZARDFS_HAVE_64BIT_JUDY
#  include "common/judy_map.h"
#else
#  include <map>
#  include "common/flat_map.h"
#endif

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

typedef std::unordered_map<uint32_t, uint32_t> TrashtimeMap;
typedef std::array<uint32_t, GoalId::kMax + 1> GoalStatistics;

struct statsrecord {
	uint32_t inodes;
	uint32_t dirs;
	uint32_t files;
	uint32_t chunks;
	uint64_t length;
	uint64_t size;
	uint64_t realsize;
};

/*! \brief Node containing common meta data for each file system object (file or directory).
 *
 * Node size = 68B
 *
 * Estimating (taking into account directory and file node size) 150B per file.
 *
 * 10K files will occupy 1.5MB
 * 10M files will occupy 1.5GB
 * 1G files will occupy 150GB
 * 4G files will occupy 600GB
 */
struct FSNode {
	enum {
		kFile = TYPE_FILE,
		kDirectory = TYPE_DIRECTORY,
		kSymlink = TYPE_SYMLINK,
		kFifo = TYPE_FIFO,
		kBlockDev = TYPE_BLOCKDEV,
		kCharDev = TYPE_CHARDEV,
		kSocket = TYPE_SOCKET,
		kTrash = TYPE_TRASH,
		kReserved = TYPE_RESERVED,
		kUnknown = TYPE_UNKNOWN
	};

	uint32_t id; /*!< Unique number identifying node. */
	uint32_t ctime; /*!< Change time. */
	uint32_t mtime; /*!< Modification time. */
	uint32_t atime; /*!< Access time. */
	uint8_t type; /*!< Node type. (file, directory, symlink, ...) */
	uint8_t goal; /*!< Goal id. */
	uint16_t mode;  /*!< Only 12 lowest bits are used for mode, in unix standard upper 4 are used
	                 for object type, but since there is field "type" this bits can be used as
	                 extra flags. */
	uint32_t uid; /*!< User id. */
	uint32_t gid; /*!< Group id. */
	uint32_t trashtime; /*!< Trash time. */

	std::unique_ptr<ExtendedAcl> extendedAcl; /*!< Access control list. */
	compact_vector<uint32_t, uint32_t> parent; /*!< Parent nodes ids. To reduce memory usage ids
	                                                are stored instead of pointers to FSNode. */

	FSNode   *next; /*!< Next field used for storing FSNode in hash map. */
	uint64_t checksum; /*!< Node checksum. */

	FSNode(uint8_t t) {
		type = t;
		next = nullptr;
		checksum = 0;
	}

	/*! \brief Static function used for creating proper node for given type.
	 * \param type Type of node to create.
	 * \return Pointer to created node.
	 */
	static FSNode *create(uint8_t type);

	/*! \brief Static function used for erasing node (uses node's type
	 * for correct invocation of destructors).
	 *
	 * \param node Pointer to node that should be erased.
	 */
	static void destroy(FSNode *node);
};

/*! \brief Node used for storing file object.
 *
 * Node size = 68B + 28B + 8 * chunks_count + 4 * session_count
 * Avg size (assuming 1 chunk and session id) = 96 + 8 + 4 ~ 110B
 */
struct FSNodeFile : public FSNode {
	uint64_t length;
	compact_vector<uint32_t> sessionid;
	compact_vector<uint64_t, uint32_t> chunks;

	FSNodeFile(uint8_t t) : FSNode(t), length(), sessionid(), chunks() {
		assert(t == kFile || t == kTrash || t == kReserved);
	}

	uint32_t chunkCount() const {
		for(uint32_t i = chunks.size(); i > 0; --i) {
			if (chunks[i-1] != 0) {
				return i;
			}
		}

		return 0;
	}
};

/*! \brief Node used for storing symbolic link.
 *
 * Node size = 68 + 10 = 78B
 */
struct FSNodeSymlink : public FSNode {
	hstorage::Handle path;
	uint16_t path_length;

	FSNodeSymlink() : FSNode(kSymlink), path_length() {
	}
};

/*! \brief Node used for storing device object.
 *
 * Node size = 68 + 4 = 72B
 */
struct FSNodeDevice : public FSNode {
	uint32_t rdev;

	FSNodeDevice(uint8_t device_type) : FSNode(device_type), rdev() {
	}
};

/*! \brief Node used for storing directory.
 *
 * Node size = 70 + 60 + 16 * entries_count
 * Avg size (10 files) ~ 330B (33B per file)
 */
struct FSNodeDirectory : public FSNode {
#ifdef LIZARDFS_HAVE_64BIT_JUDY
	typedef judy_map<hstorage::Handle, FSNode *> EntriesContainer;
#else
	struct HandleCompare {
		bool operator()(const hstorage::Handle &a, const hstorage::Handle &b) const {
			return a.data() < b.data();
		}
	};
	typedef flat_map<hstorage::Handle, FSNode *, std::vector<std::pair<hstorage::Handle, FSNode *>>,
	HandleCompare> EntriesContainer;
#endif

	typedef EntriesContainer::iterator iterator;
	typedef EntriesContainer::const_iterator const_iterator;

	std::unique_ptr<AccessControlList> defaultAcl; /*!< Default access control list for directory. */
	EntriesContainer entries; /*!< Directory entries (entry: name + pointer to child node). */
	statsrecord stats; /*!< Directory statistics (including subdirectories). */
	uint32_t nlink; /*!< Number of directories linking to this directory. */
	uint16_t entries_hash;

	FSNodeDirectory() : FSNode(kDirectory) {
		memset(&stats, 0, sizeof(stats));
		nlink = 2;
		entries_hash = 0;
	}

	~FSNodeDirectory() {
	}


	/*! \brief Find directory entry with given name.
	 *
	 * \param name Name of entry to find.
	 * \return If node is found returns iterator pointing to directory entry containing node,
	 *         otherwise entries.end().
	 */
	iterator find(const HString& name) {
		uint64_t name_hash = (hstorage::Handle::HashType)name.hash();

		auto it = entries.lower_bound(hstorage::Handle(name_hash << hstorage::Handle::kHashShift));
		for (; it != entries.end(); ++it) {
			if (((*it).first.data() >> hstorage::Handle::kHashShift) != name_hash) {
				break;
			}
			if ((*it).first == name) {
				return it;
			}
		}

		return entries.end();
	}

	/*! \brief Find directory entry with given node.
	 *
	 * \param node Node to find.
	 * \return If node is found returns iterator pointing to directory entry containing node,
	 *         otherwise entries.end().
	 */
	iterator find(const FSNode *node) {
		auto it = entries.begin();
		for (; it != entries.end(); ++it) {
			if ((*it).second == node) {
				break;
			}
		}

		return it;
	}

	/*! \brief Find directory entry with given node.
	 *
	 * \param node Node to find.
	 * \return If node is found returns iterator pointing to directory entry containing node,
	 *         otherwise entries.end().
	 */
	const_iterator find(const FSNode *node) const {
		auto it = entries.begin();
		for (; it != entries.end(); ++it) {
			if ((*it).second == node) {
				break;
			}
		}

		return it;
	}

	/*! \brief Returns name for specified node.
	 *
	 * \param node Pointer to node.
	 * \return If node is found returns name associated with this node,
	 *         otherwise returns empty string.
	 */
	std::string getChildName(const FSNode *node) const {
		auto it = find(node);
		if (it != entries.end()) {
			return (std::string)(*it).first;
		}
		return std::string();
	}

	iterator begin() {
		return entries.begin();
	}

	iterator end() {
		return entries.end();
	}

	const_iterator begin() const {
		return entries.begin();
	}

	const_iterator end() const {
		return entries.end();
	}
};

struct TrashPathKey {
	explicit TrashPathKey(const FSNode *node) :
#ifdef WORDS_BIGENDIAN
	    timestamp(std::min((uint64_t)node->atime + node->trashtime, (uint64_t)UINT32_MAX)),
	    id(node->id)
#else
	    id(node->id),
	    timestamp(std::min((uint64_t)node->atime + node->trashtime, (uint64_t)UINT32_MAX))
#endif
	{}

	bool operator<(const TrashPathKey &other) const {
		return std::make_pair(timestamp, id) < std::make_pair(other.timestamp, other.id);
	}

#ifdef WORDS_BIGENDIAN
	uint32_t timestamp;
	uint32_t id;
#else
	uint32_t id;
	uint32_t timestamp;
#endif
};

#ifdef LIZARDFS_HAVE_64BIT_JUDY
typedef judy_map<TrashPathKey, hstorage::Handle> TrashPathContainer;
typedef judy_map<uint32_t, hstorage::Handle> ReservedPathContainer;
#else
typedef std::map<TrashPathKey, hstorage::Handle> TrashPathContainer;
typedef std::map<uint32_t, hstorage::Handle> ReservedPathContainer;
#endif

inline uint32_t fsnodes_hash(uint32_t parentid, const hstorage::Handle &name) {
	return (parentid * 0x5F2318BD) + name.hash();
}

inline uint32_t fsnodes_hash(uint32_t parentid, const HString &name) {
	return (parentid * 0x5F2318BD) + (hstorage::Handle::HashType)name.hash();
}

namespace detail {

FSNode *fsnodes_id_to_node_internal(uint32_t id);

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

template<class NodeType>
NodeType *fsnodes_id_to_node_verify(uint32_t id) {
	NodeType *node = static_cast<NodeType*>(detail::fsnodes_id_to_node_internal(id));
	detail::fsnodes_check_node_type(node);
	return node;
}

template<class NodeType = FSNode>
NodeType *fsnodes_id_to_node(uint32_t id) {
	return static_cast<NodeType*>(detail::fsnodes_id_to_node_internal(id));
}

std::string fsnodes_escape_name(const std::string &name);
int fsnodes_purge(uint32_t ts, FSNode *p);
uint32_t fsnodes_getdetachedsize(const TrashPathContainer &data);
void fsnodes_getdetacheddata(const TrashPathContainer &data, uint8_t *dbuff);
uint32_t fsnodes_getdetachedsize(const ReservedPathContainer &data);
void fsnodes_getdetacheddata(const ReservedPathContainer &data, uint8_t *dbuff);
void fsnodes_getpath(FSNodeDirectory *parent, FSNode *child, std::string &path);
void fsnodes_fill_attr(FSNode *node, FSNode *parent, uint32_t uid, uint32_t gid, uint32_t auid,
	uint32_t agid, uint8_t sesflags, Attributes &attr);
void fsnodes_fill_attr(const FsContext &context, FSNode *node, FSNode *parent, Attributes &attr);

uint8_t verify_session(const FsContext &context, OperationMode operationMode,
	SessionType sessionType);

uint8_t fsnodes_get_node_for_operation(const FsContext &context, ExpectedNodeType expectedNodeType,
	uint8_t modemask, uint32_t inode, FSNode **ret);
uint8_t fsnodes_undel(uint32_t ts, FSNodeFile *node);

int fsnodes_namecheck(const std::string &name);
FSNode *fsnodes_lookup(FSNodeDirectory *node, const HString &name);
void fsnodes_get_stats(FSNode *node, statsrecord *sr);
bool fsnodes_isancestor_or_node_reserved_or_trash(FSNodeDirectory *f, FSNode *p);
int fsnodes_access(FSNode *node, uint32_t uid, uint32_t gid, uint8_t modemask, uint8_t sesflags);

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
uint32_t fsnodes_getdirsize(const FSNodeDirectory *p, uint8_t withattr);
void fsnodes_getdirdata(uint32_t rootinode, uint32_t uid, uint32_t gid, uint32_t auid,
	uint32_t agid, uint8_t sesflags, FSNodeDirectory *p, uint8_t *dbuff,
	uint8_t withattr);
void fsnodes_checkfile(FSNodeFile *p, uint32_t chunkcount[CHUNK_MATRIX_SIZE]);

bool fsnodes_has_tape_goal(FSNode *node);
void fsnodes_add_sub_stats(FSNodeDirectory *parent, statsrecord *newsr, statsrecord *prevsr);

void fsnodes_getgoal_recursive(FSNode *node, uint8_t gmode, GoalStatistics &fgtab,
		GoalStatistics &dgtab);

void fsnodes_gettrashtime_recursive(FSNode *node, uint8_t gmode,
	TrashtimeMap &fileTrashtimes, TrashtimeMap &dirTrashtimes);
void fsnodes_geteattr_recursive(FSNode *node, uint8_t gmode, uint32_t feattrtab[16],
	uint32_t deattrtab[16]);
void fsnodes_setgoal_recursive(FSNode *node, uint32_t ts, uint32_t uid, uint8_t goal, uint8_t smode,
	uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes);
void fsnodes_settrashtime_recursive(FSNode *node, uint32_t ts, uint32_t uid, uint32_t trashtime,
	uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
	uint32_t *nsinodes);
void fsnodes_seteattr_recursive(FSNode *node, uint32_t ts, uint32_t uid, uint8_t eattr,
	uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes,
	uint32_t *nsinodes);
uint8_t fsnodes_deleteacl(FSNode *p, AclType type, uint32_t ts);

uint8_t fsnodes_setacl(FSNode *p, AclType type, AccessControlList acl, uint32_t ts);
uint8_t fsnodes_getacl(FSNode *p, AclType type, AccessControlList &acl);

uint32_t fsnodes_getpath_size(FSNodeDirectory *parent, FSNode *child);
void fsnodes_getpath_data(FSNodeDirectory *parent, FSNode *child, uint8_t *path, uint32_t size);

int64_t fsnodes_get_size(FSNode *node);
