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
#include "master/filesystem_checksum.h"

#include <cstdint>

#include "common/event_loop.h"
#include "common/hashfn.h"
#include "common/platform.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_xattr.h"

static uint64_t fsnodes_checksum(FSNode *node, bool full_update = false) {
	if (!node) {
		return 0;
	}
	uint64_t seed = 0x4660fe60565ba616;  // random number
	hashCombine(seed, node->type, node->id, node->goal, node->mode, node->uid, node->gid,
	            node->atime, node->mtime, node->ctime, node->trashtime);
	switch (node->type) {
	case FSNode::kDirectory:
		if (full_update) {
			static_cast<FSNodeDirectory*>(node)->entries_hash = 0;
			for(const auto &entry : *static_cast<FSNodeDirectory*>(node)) {
				static_cast<FSNodeDirectory*>(node)->entries_hash ^= entry.first.hash();
			}
		}
		hashCombine(seed, static_cast<const FSNodeDirectory*>(node)->entries_hash);
		break;
	case FSNode::kSocket:
	case FSNode::kFifo:
		break;
	case FSNode::kBlockDev:
	case FSNode::kCharDev:
		hashCombine(seed, static_cast<const FSNodeDevice*>(node)->rdev);
		break;
	case FSNode::kSymlink:
		hashCombine(seed, static_cast<const FSNodeSymlink*>(node)->path.hash());
		break;
	case FSNode::kFile:
	case FSNode::kTrash:
	case FSNode::kReserved:
		hashCombine(seed, static_cast<const FSNodeFile*>(node)->length);
		// first chunk's id
		if (static_cast<const FSNodeFile*>(node)->length == 0 || static_cast<const FSNodeFile*>(node)->chunks.size() == 0) {
			hashCombine(seed, static_cast<uint64_t>(0));
		} else {
			hashCombine(seed, static_cast<const FSNodeFile*>(node)->chunks[0]);
		}
		// last chunk's id
		uint32_t lastchunk = (static_cast<const FSNodeFile*>(node)->length - 1) / MFSCHUNKSIZE;
		if (static_cast<const FSNodeFile*>(node)->length == 0 || lastchunk >= static_cast<const FSNodeFile*>(node)->chunks.size()) {
			hashCombine(seed, static_cast<uint64_t>(0));
		} else {
			hashCombine(seed, static_cast<const FSNodeFile*>(node)->chunks[lastchunk]);
		}
	}
	return seed;
}

void fsnodes_checksum_add_to_background(FSNode *node) {
	if (!node) {
		return;
	}
	removeFromChecksum(gMetadata->fsNodesChecksum, node->checksum);
	node->checksum = fsnodes_checksum(node);
	addToChecksum(gMetadata->fsNodesChecksum, node->checksum);
	addToChecksum(gChecksumBackgroundUpdater.fsNodesChecksum, node->checksum);
}

void fsnodes_update_checksum(FSNode *node) {
	if (!node) {
		return;
	}
	if (gChecksumBackgroundUpdater.isNodeIncluded(node)) {
		removeFromChecksum(gChecksumBackgroundUpdater.fsNodesChecksum, node->checksum);
	}
	removeFromChecksum(gMetadata->fsNodesChecksum, node->checksum);
	node->checksum = fsnodes_checksum(node);
	addToChecksum(gMetadata->fsNodesChecksum, node->checksum);
	if (gChecksumBackgroundUpdater.isNodeIncluded(node)) {
		addToChecksum(gChecksumBackgroundUpdater.fsNodesChecksum, node->checksum);
	}
}

static void fsnodes_recalculate_checksum() {
	gMetadata->fsNodesChecksum = NODECHECKSUMSEED;  // arbitrary number
	// nodes
	for (uint32_t i = 0; i < NODEHASHSIZE; i++) {
		for (FSNode *node = gMetadata->nodehash[i]; node; node = node->next) {
			node->checksum = fsnodes_checksum(node, true);
			addToChecksum(gMetadata->fsNodesChecksum, node->checksum);
		}
	}
}

uint64_t fs_checksum(ChecksumMode mode) {
	uint64_t checksum = 0x1251;
	hashCombine(checksum, gMetadata->maxnodeid);
	hashCombine(checksum, gMetadata->metaversion);
	hashCombine(checksum, gMetadata->nextsessionid);
	if (mode == ChecksumMode::kForceRecalculate) {
		fsnodes_recalculate_checksum();
		xattr_recalculate_checksum();
		gMetadata->quota_checksum = gMetadata->quota_database.checksum();
	}
	hashCombine(checksum, gMetadata->fsNodesChecksum);
	hashCombine(checksum, gMetadata->xattrChecksum);
	hashCombine(checksum, gMetadata->quota_checksum);
	hashCombine(checksum, chunk_checksum(mode));
	return checksum;
}

#ifndef METARESTORE
uint8_t fs_start_checksum_recalculation() {
	if (gChecksumBackgroundUpdater.start()) {
		eventloop_make_next_poll_nonblocking();
		return LIZARDFS_STATUS_OK;
	} else {
		return LIZARDFS_ERROR_TEMP_NOTPOSSIBLE;
	}
}
#endif
