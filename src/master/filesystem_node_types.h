/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2016
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
enum class ExpectedNodeType { kFile, kDirectory, kNotDirectory, kFileOrDirectory, kAny };

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

	std::unique_ptr<RichACL> acl; /*!< Access control list. */
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

	iterator find_nth(EntriesContainer::size_type nth) {
		return entries.find_nth(nth);
	}

	const_iterator find_nth(EntriesContainer::size_type nth) const {
		return entries.find_nth(nth);
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
	    timestamp(std::min((uint64_t)node->ctime + node->trashtime, (uint64_t)UINT32_MAX)),
	    id(node->id)
#else
	    id(node->id),
	    timestamp(std::min((uint64_t)node->ctime + node->trashtime, (uint64_t)UINT32_MAX))
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
