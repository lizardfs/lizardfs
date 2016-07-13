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

#pragma once

#include "common/platform.h"

#include <map>
#include <unordered_map>

#include "common/tape_copies.h"
#include "common/special_inode_defs.h"
#include "master/chunks.h"
#include "master/id_pool_detainer.h"
#include "master/filesystem_checksum_background_updater.h"
#include "master/filesystem_freenode.h"
#include "master/filesystem_node.h"
#include "master/filesystem_xattr.h"
#include "master/locks.h"
#include "master/metadata_dumper.h"
#include "master/quota_database.h"
#include "master/task_manager.h"

/** Metadata of the filesystem.
 *  All the static variables managed by function in this file which form metadata of the filesystem.
 */
struct FilesystemMetadata {
public:
	std::unordered_map<uint32_t, TapeCopies> tapeCopies;
	xattr_inode_entry *xattr_inode_hash[XATTR_INODE_HASH_SIZE];
	xattr_data_entry *xattr_data_hash[XATTR_DATA_HASH_SIZE];
	IdPoolDetainer<uint32_t, uint32_t> inode_pool;
	TrashPathContainer trash;
	ReservedPathContainer reserved;
	FSNodeDirectory *root;
	FSNode *nodehash[NODEHASHSIZE];
	TaskManager task_manager;
	FileLocks flock_locks;
	FileLocks posix_locks;

	uint32_t maxnodeid;
	uint32_t nextsessionid;
	uint32_t nodes;
	uint64_t metaversion;
	uint64_t trashspace;
	uint64_t reservedspace;
	uint32_t trashnodes;
	uint32_t reservednodes;
	uint32_t filenodes;
	uint32_t dirnodes;

	QuotaDatabase quota_database;

	uint64_t fsNodesChecksum;
	uint64_t xattrChecksum;
	uint64_t quota_checksum;

	FilesystemMetadata()
	    : tapeCopies{},
	      xattr_inode_hash{},
	      xattr_data_hash{},
	      inode_pool{MFS_INODE_REUSE_DELAY, 12,
	                 MAX_REGULAR_INODE, MAX_REGULAR_INODE,
	                 32 * 8 * 1024, 8 * 1024, 10},
	      trash{},
	      reserved{},
	      root{},
	      nodehash{},
	      task_manager{},
	      flock_locks{},
	      posix_locks{},
	      maxnodeid{},
	      nextsessionid{},
	      nodes{},
	      metaversion{},
	      trashspace{},
	      reservedspace{},
	      trashnodes{},
	      reservednodes{},
	      filenodes{},
	      dirnodes{},
	      quota_database{},
	      fsNodesChecksum{},
	      xattrChecksum{},
	      quota_checksum{quota_database.checksum()} {
	}

	~FilesystemMetadata() {
		// Free memory allocated in xattr_inode_hash hashmap
		for (uint32_t i = 0; i < XATTR_INODE_HASH_SIZE; ++i) {
			freeListConnectedUsingNext(xattr_inode_hash[i]);
		}

		// Free memory allocated in xattr_data_hash hashmap
		for (uint32_t i = 0; i < XATTR_DATA_HASH_SIZE; ++i) {
			deleteListConnectedUsingNext(xattr_data_hash[i]);
		}

		// Free memory allocated in nodehash hashmap
		for (uint32_t i = 0; i < NODEHASHSIZE; ++i) {
			FSNode *node = nodehash[i];
			while (node != nullptr) {
				FSNode *next = node->next;
				FSNode::destroy(node);
				node = next;
			}
		}
	}

private:
	// Frees a C-style list with elements connected using e->next pointer and allocated using
	// malloc
	template <typename T>
	void freeListConnectedUsingNext(T *&list) {
		while (list != nullptr) {
			T *next = list->next;
			free(list);
			list = next;
		}
	}

	// Frees a C-style list with elements connected using e->next pointer and allocated using
	// new
	template <typename T>
	void deleteListConnectedUsingNext(T *&list) {
		while (list != nullptr) {
			T *next = list->next;
			delete list;
			list = next;
		}
	}
};

extern FilesystemMetadata *gMetadata;
extern ChecksumBackgroundUpdater gChecksumBackgroundUpdater;
extern bool gDisableChecksumVerification;
extern uint32_t gTestStartTime;

#ifndef METARESTORE
extern std::map<int, Goal> gGoalDefinitions;
extern MetadataDumper metadataDumper;
extern bool gAtimeDisabled;
extern bool gMagicAutoFileRepair;
#endif
