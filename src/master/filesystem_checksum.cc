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

#include "common/hashfn.h"
#include "common/main.h"
#include "common/platform.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_xattr.h"

static uint64_t fsnodes_checksum(const fsnode *node) {
	if (!node) {
		return 0;
	}
	uint64_t seed = 0x4660fe60565ba616;  // random number
	hashCombine(seed, node->type, node->id, node->goal, node->mode, node->uid, node->gid,
	            node->atime, node->mtime, node->ctime, node->trashtime);
	switch (node->type) {
	case TYPE_DIRECTORY:
	case TYPE_SOCKET:
	case TYPE_FIFO:
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		hashCombine(seed, node->data.devdata.rdev);
		break;
	case TYPE_SYMLINK:
		hashCombine(seed, node->data.sdata.pleng,
		            ByteArray(node->data.sdata.path, node->data.sdata.pleng));
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		hashCombine(seed, node->data.fdata.length);
		// first chunk's id
		if (node->data.fdata.length == 0 || node->data.fdata.chunks == 0) {
			hashCombine(seed, static_cast<uint64_t>(0));
		} else {
			hashCombine(seed, node->data.fdata.chunktab[0]);
		}
		// last chunk's id
		uint32_t lastchunk = (node->data.fdata.length - 1) / MFSCHUNKSIZE;
		if (node->data.fdata.length == 0 || lastchunk >= node->data.fdata.chunks) {
			hashCombine(seed, static_cast<uint64_t>(0));
		} else {
			hashCombine(seed, node->data.fdata.chunktab[lastchunk]);
		}
	}
	return seed;
}

void fsnodes_checksum_add_to_background(fsnode *node) {
	if (!node) {
		return;
	}
	removeFromChecksum(gMetadata->fsNodesChecksum, node->checksum);
	node->checksum = fsnodes_checksum(node);
	addToChecksum(gMetadata->fsNodesChecksum, node->checksum);
	addToChecksum(gChecksumBackgroundUpdater.fsNodesChecksum, node->checksum);
}

void fsnodes_update_checksum(fsnode *node) {
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
		for (fsnode *node = gMetadata->nodehash[i]; node; node = node->next) {
			node->checksum = fsnodes_checksum(node);
			addToChecksum(gMetadata->fsNodesChecksum, node->checksum);
		}
	}
}

static uint64_t fsedges_checksum(const fsedge *edge) {
	if (!edge) {
		return 0;
	}
	uint64_t seed = 0xb14f9f1819ff266c;  // random number
	if (edge->parent) {
		hashCombine(seed, edge->parent->id);
	}
	hashCombine(seed, edge->child->id, edge->nleng, ByteArray(edge->name, edge->nleng));
	return seed;
}

static void fsedges_checksum_edges_list(uint64_t &checksum, fsedge *edge) {
	while (edge) {
		edge->checksum = fsedges_checksum(edge);
		addToChecksum(checksum, edge->checksum);
		edge = edge->nextchild;
	}
}

static void fsedges_checksum_edges_rec(uint64_t &checksum, fsnode *node) {
	if (!node) {
		return;
	}
	fsedges_checksum_edges_list(checksum, node->data.ddata.children);
	for (const fsedge *edge = node->data.ddata.children; edge; edge = edge->nextchild) {
		if (edge->child->type == TYPE_DIRECTORY) {
			fsedges_checksum_edges_rec(checksum, edge->child);
		}
	}
}

void fsedges_checksum_add_to_background(fsedge *edge) {
	if (!edge) {
		return;
	}
	removeFromChecksum(gMetadata->fsEdgesChecksum, edge->checksum);
	edge->checksum = fsedges_checksum(edge);
	addToChecksum(gMetadata->fsEdgesChecksum, edge->checksum);
	addToChecksum(gChecksumBackgroundUpdater.fsEdgesChecksum, edge->checksum);
}

void fsedges_update_checksum(fsedge *edge) {
	if (!edge) {
		return;
	}
	if (gChecksumBackgroundUpdater.isEdgeIncluded(edge)) {
		removeFromChecksum(gChecksumBackgroundUpdater.fsEdgesChecksum, edge->checksum);
	}
	removeFromChecksum(gMetadata->fsEdgesChecksum, edge->checksum);
	edge->checksum = fsedges_checksum(edge);
	addToChecksum(gMetadata->fsEdgesChecksum, edge->checksum);
	if (gChecksumBackgroundUpdater.isEdgeIncluded(edge)) {
		addToChecksum(gChecksumBackgroundUpdater.fsEdgesChecksum, edge->checksum);
	}
}

static void fsedges_recalculate_checksum() {
	gMetadata->fsEdgesChecksum = EDGECHECKSUMSEED;
	// edges
	if (gMetadata->root) {
		fsedges_checksum_edges_rec(gMetadata->fsEdgesChecksum, gMetadata->root);
	}
	if (gMetadata->trash) {
		fsedges_checksum_edges_list(gMetadata->fsEdgesChecksum, gMetadata->trash);
	}
	if (gMetadata->reserved) {
		fsedges_checksum_edges_list(gMetadata->fsEdgesChecksum, gMetadata->reserved);
	}
}

uint64_t fs_checksum(ChecksumMode mode) {
	uint64_t checksum = 0x1251;
	hashCombine(checksum, gMetadata->maxnodeid);
	hashCombine(checksum, gMetadata->metaversion);
	hashCombine(checksum, gMetadata->nextsessionid);
	if (mode == ChecksumMode::kForceRecalculate) {
		fsnodes_recalculate_checksum();
		fsedges_recalculate_checksum();
		xattr_recalculate_checksum();
	}
	hashCombine(checksum, gMetadata->fsNodesChecksum);
	hashCombine(checksum, gMetadata->fsEdgesChecksum);
	hashCombine(checksum, gMetadata->xattrChecksum);
	hashCombine(checksum, gMetadata->gQuotaDatabase.checksum());
	hashCombine(checksum, chunk_checksum(mode));
	return checksum;
}

#ifndef METARESTORE
uint8_t fs_start_checksum_recalculation() {
	if (gChecksumBackgroundUpdater.start()) {
		main_make_next_poll_nonblocking();
		return LIZARDFS_STATUS_OK;
	} else {
		return LIZARDFS_ERROR_TEMP_NOTPOSSIBLE;
	}
}
#endif
