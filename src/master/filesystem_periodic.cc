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
#include "master/filesystem_periodic.h"

#include <cstdint>
#include <type_traits>

#include "common/main.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"
#include "master/filesystem_operations.h"
#include "master/matoclserv.h"

#define MSGBUFFSIZE 1000000
#define ERRORS_LOG_MAX 500

#ifndef METARESTORE

static char     *fsinfo_msgbuff = nullptr;
static uint32_t fsinfo_files = 0;
static uint32_t fsinfo_ugfiles = 0;
static uint32_t fsinfo_mfiles = 0;
static uint32_t fsinfo_chunks = 0;
static uint32_t fsinfo_ugchunks = 0;
static uint32_t fsinfo_mchunks = 0;
static uint32_t fsinfo_msgbuffleng = 0;
static uint32_t fsinfo_loopstart = 0;
static uint32_t fsinfo_loopend = 0;

void fs_test_getdata(uint32_t *loopstart, uint32_t *loopend, uint32_t *files, uint32_t *ugfiles,
			uint32_t *mfiles, uint32_t *chunks, uint32_t *ugchunks, uint32_t *mchunks,
			char **msgbuff, uint32_t *msgbuffleng) {
	*loopstart = fsinfo_loopstart;
	*loopend = fsinfo_loopend;
	*files = fsinfo_files;
	*ugfiles = fsinfo_ugfiles;
	*mfiles = fsinfo_mfiles;
	*chunks = fsinfo_chunks;
	*ugchunks = fsinfo_ugchunks;
	*mchunks = fsinfo_mchunks;
	*msgbuff = fsinfo_msgbuff;
	*msgbuffleng = fsinfo_msgbuffleng;
}

void fs_background_checksum_recalculation_a_bit() {
	uint32_t recalculated = 0;

	switch (gChecksumBackgroundUpdater.getStep()) {
	case ChecksumRecalculatingStep::kNone:  // Recalculation not in progress.
		return;
	case ChecksumRecalculatingStep::kNodes:
		// Nodes are in a hashtable, therefore they can be recalculated in multiple steps.
		while (gChecksumBackgroundUpdater.getPosition() < NODEHASHSIZE) {
			for (FSNode *node =
			             gMetadata->nodehash[gChecksumBackgroundUpdater.getPosition()];
			     node; node = node->next) {
				fsnodes_checksum_add_to_background(node);
				++recalculated;
			}
			gChecksumBackgroundUpdater.incPosition();
			if (recalculated >= gChecksumBackgroundUpdater.getSpeedLimit()) {
				break;
			}
		}
		if (gChecksumBackgroundUpdater.getPosition() == NODEHASHSIZE) {
			gChecksumBackgroundUpdater.incStep();
		}
		break;
	case ChecksumRecalculatingStep::kXattrs:
		// Xattrs are in a hashtable, therefore they can be recalculated in multiple steps.
		while (gChecksumBackgroundUpdater.getPosition() < XATTR_DATA_HASH_SIZE) {
			for (xattr_data_entry *xde =
			             gMetadata->xattr_data_hash[gChecksumBackgroundUpdater
			                                                .getPosition()];
			     xde; xde = xde->next) {
				xattr_checksum_add_to_background(xde);
				++recalculated;
			}
			gChecksumBackgroundUpdater.incPosition();
			if (recalculated >= gChecksumBackgroundUpdater.getSpeedLimit()) {
				break;
			}
		}
		if (gChecksumBackgroundUpdater.getPosition() == XATTR_DATA_HASH_SIZE) {
			gChecksumBackgroundUpdater.incStep();
		}
		break;
	case ChecksumRecalculatingStep::kChunks:
		// Chunks can be processed in multiple steps.
		if (chunks_update_checksum_a_bit(gChecksumBackgroundUpdater.getSpeedLimit()) ==
		    ChecksumRecalculationStatus::kDone) {
			gChecksumBackgroundUpdater.incStep();
		}
		break;
	case ChecksumRecalculatingStep::kDone:
		gChecksumBackgroundUpdater.end();
		matoclserv_broadcast_metadata_checksum_recalculated(LIZARDFS_STATUS_OK);
		return;
	}
	main_make_next_poll_nonblocking();
}

void fs_periodic_test_files() {
	static uint32_t i = 0;
	uint32_t k;
	uint64_t chunkid;
	uint8_t vc, valid, ugflag;
	static uint32_t files = 0;
	static uint32_t ugfiles = 0;
	static uint32_t mfiles = 0;
	static uint32_t chunks = 0;
	static uint32_t ugchunks = 0;
	static uint32_t mchunks = 0;
	static uint32_t errors = 0;
	static uint32_t notfoundchunks = 0;
	static uint32_t unavailchunks = 0;
	static uint32_t unavailfiles = 0;
	static uint32_t unavailtrashfiles = 0;
	static uint32_t unavailreservedfiles = 0;
	static char *msgbuff = NULL, *tmp;
	static uint32_t leng = 0;
	FSNode *f;

	if ((uint32_t)(main_time()) <= test_start_time) {
		return;
	}
	if (i >= NODEHASHSIZE) {
		syslog(LOG_NOTICE, "structure check loop");
		i = 0;
		errors = 0;
	}
	if (i == 0) {
		if (errors == ERRORS_LOG_MAX) {
			syslog(LOG_ERR, "only first %u errors (unavailable chunks/files) were logged",
			       ERRORS_LOG_MAX);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "only first %u errors (unavailable chunks/files) "
				                 "were logged\n",
				                 ERRORS_LOG_MAX);
			}
		}
		if (notfoundchunks > 0) {
			syslog(LOG_ERR, "unknown chunks: %" PRIu32, notfoundchunks);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "unknown chunks: %" PRIu32 "\n", notfoundchunks);
			}
			notfoundchunks = 0;
		}
		if (unavailchunks > 0) {
			syslog(LOG_ERR, "unavailable chunks: %" PRIu32, unavailchunks);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "unavailable chunks: %" PRIu32 "\n", unavailchunks);
			}
			unavailchunks = 0;
		}
		if (unavailtrashfiles > 0) {
			syslog(LOG_ERR, "unavailable trash files: %" PRIu32, unavailtrashfiles);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "unavailable trash files: %" PRIu32 "\n", unavailtrashfiles);
			}
			unavailtrashfiles = 0;
		}
		if (unavailreservedfiles > 0) {
			syslog(LOG_ERR, "unavailable reserved files: %" PRIu32, unavailreservedfiles);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "unavailable reserved files: %" PRIu32 "\n", unavailreservedfiles);
			}
			unavailreservedfiles = 0;
		}
		if (unavailfiles > 0) {
			syslog(LOG_ERR, "unavailable files: %" PRIu32, unavailfiles);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "unavailable files: %" PRIu32 "\n", unavailfiles);
			}
			unavailfiles = 0;
		}
		fsinfo_files = files;
		fsinfo_ugfiles = ugfiles;
		fsinfo_mfiles = mfiles;
		fsinfo_chunks = chunks;
		fsinfo_ugchunks = ugchunks;
		fsinfo_mchunks = mchunks;
		files = 0;
		ugfiles = 0;
		mfiles = 0;
		chunks = 0;
		ugchunks = 0;
		mchunks = 0;

		if (fsinfo_msgbuff == NULL) {
			fsinfo_msgbuff = (char *)malloc(MSGBUFFSIZE);
			passert(fsinfo_msgbuff);
		}
		tmp = fsinfo_msgbuff;
		fsinfo_msgbuff = msgbuff;
		msgbuff = tmp;
		if (leng > MSGBUFFSIZE) {
			fsinfo_msgbuffleng = MSGBUFFSIZE;
		} else {
			fsinfo_msgbuffleng = leng;
		}
		leng = 0;

		fsinfo_loopstart = fsinfo_loopend;
		fsinfo_loopend = main_time();
	}
	for (k = 0; k < (NODEHASHSIZE / 14400) && i < NODEHASHSIZE; k++, i++) {
		for (f = gMetadata->nodehash[i]; f; f = f->next) {
			if (f->type == FSNode::kFile || f->type == FSNode::kTrash || f->type == FSNode::kReserved) {
				valid = 1;
				ugflag = 0;
				for (uint32_t j = 0; j < static_cast<FSNodeFile *>(f)->chunks.size(); ++j) {
					chunkid = static_cast<FSNodeFile *>(f)->chunks[j];
					if (chunkid == 0) {
						continue;
					}

					if (chunk_get_fullcopies(chunkid, &vc) != LIZARDFS_STATUS_OK) {
						if (errors < ERRORS_LOG_MAX) {
							syslog(LOG_ERR,
							       "structure error - chunk "
							       "%016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32
							       ")",
							       chunkid, f->id, j);
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
								                 "structure error - "
								                 "chunk %016" PRIX64
								                 " not found "
								                 "(inode: %" PRIu32 " ; index: %" PRIu32 ")\n",
								                 chunkid, f->id, j);
							}
							errors++;
						}
						notfoundchunks++;
						if ((notfoundchunks % 1000) == 0) {
							syslog(LOG_ERR, "unknown chunks: %" PRIu32 " ...", notfoundchunks);
						}
						valid = 0;
						mchunks++;
					} else if (vc == 0) {
						if (errors < ERRORS_LOG_MAX) {
							syslog(LOG_ERR,
							       "currently unavailable "
							       "chunk %016" PRIX64 " (inode: %" PRIu32 " ; index: %" PRIu32 ")",
							       chunkid, f->id, j);
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
								                 "currently "
								                 "unavailable chunk "
								                 "%016" PRIX64 " (inode: %" PRIu32
								                 " ; index: %" PRIu32 ")\n",
								                 chunkid, f->id, j);
							}
							errors++;
						}
						unavailchunks++;
						if ((unavailchunks % 1000) == 0) {
							syslog(LOG_ERR,
							       "unavailable chunks: "
							       "%" PRIu32 " ...",
							       unavailchunks);
						}
						valid = 0;
						mchunks++;
					} else {
						int recover, remove;
						chunk_get_partstomodify(chunkid, recover, remove);
						if (recover > 0) {
							ugflag = 1;
							ugchunks++;
						}
					}
					chunks++;
				}
				if (valid == 0) {
					mfiles++;
					if (f->type == FSNode::kTrash) {
						if (errors < ERRORS_LOG_MAX) {
							std::string name = (std::string)gMetadata->trash.at(f->id);

							syslog(LOG_ERR,
							       "- currently unavailable file in "
							       "trash %" PRIu32 ": %s",
							       f->id, fsnodes_escape_name(name).c_str());
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
								                 "- currently unavailable "
								                 "file in trash %" PRIu32 ": %s\n",
								                 f->id, fsnodes_escape_name(name).c_str());
							}
							errors++;
							unavailtrashfiles++;
							if ((unavailtrashfiles % 1000) == 0) {
								syslog(LOG_ERR,
								       "unavailable trash files: "
								       "%" PRIu32 " ...",
								       unavailtrashfiles);
							}
						}
					} else if (f->type == FSNode::kReserved) {
						if (errors < ERRORS_LOG_MAX) {
							std::string name = (std::string)gMetadata->reserved.at(f->id);

							syslog(LOG_ERR,
							       "+ currently unavailable reserved "
							       "file %" PRIu32 ": %s",
							       f->id, fsnodes_escape_name(name).c_str());
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
								                 "+ currently unavailable "
								                 "reserved file %" PRIu32 ": %s\n",
								                 f->id, fsnodes_escape_name(name).c_str());
							}
							errors++;
							unavailreservedfiles++;
							if ((unavailreservedfiles % 1000) == 0) {
								syslog(LOG_ERR,
								       "unavailable reserved "
								       "files: %" PRIu32 " ...",
								       unavailreservedfiles);
							}
						}
					} else {
						std::string path;
						for (const auto parent_inode : f->parent) {
							if (errors < ERRORS_LOG_MAX) {
								FSNodeDirectory *parent = fsnodes_id_to_node_verify<FSNodeDirectory>(parent_inode);
								fsnodes_getpath(parent, f, path);
								syslog(LOG_ERR,
								       "* currently unavailable "
								       "file %" PRIu32 ": %s",
								       f->id, fsnodes_escape_name(path).c_str());
								if (leng < MSGBUFFSIZE) {
									leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
									                 "* currently "
									                 "unavailable file "
									                 "%" PRIu32 ": %s\n",
									                 f->id, fsnodes_escape_name(path).c_str());
								}
								errors++;
							}
							unavailfiles++;
							if ((unavailfiles % 1000) == 0) {
								syslog(LOG_ERR, "unavailable files: %" PRIu32 " ...", unavailfiles);
							}
						}
					}
				} else if (ugflag) {
					ugfiles++;
				}
				files++;
			}
			for (const auto &parent_inode : f->parent) {
				FSNodeDirectory *parent = fsnodes_id_to_node<FSNodeDirectory>(parent_inode);
				if (!parent || parent->type != FSNode::kDirectory) {
					if (errors < ERRORS_LOG_MAX) {
						syslog(LOG_ERR, "structure error - invalid node's parent (inode: %" PRIu32
						                " ; parent's inode: %" PRIu32 ")",
						       f->id, parent_inode);
						if (leng < MSGBUFFSIZE) {
							leng +=
							    snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
							             "structure error - invalid node's parent (inode: %" PRIu32
							             " ; parent's inode: %" PRIu32 ")",
							             f->id, parent_inode);
						}
						errors++;
					}
				}
			}
			if (f->type == FSNode::kDirectory) {
				for (const auto &entry : static_cast<FSNodeDirectory *>(f)->entries) {
					FSNode *node = entry.second;

					if (!node ||
					    std::find(node->parent.begin(), node->parent.end(), f->id) ==
					        node->parent.end()) {
						syslog(LOG_ERR,
						       "structure error - "
						       "child doesn't point to parent (node: "
						       "%" PRIu32 " ; parent: %" PRIu32 ")",
						       node->id, f->id);
						if (leng < MSGBUFFSIZE) {
							leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
							                 "structure error - "
							                 "child doesn't point to parent (node: "
							                 "%" PRIu32 " ; parent: %" PRIu32 ")",
							                 node->id, f->id);
						}
					}
				}
			}
		}
	}
}
#endif

struct InodeInfo {
	uint32_t free;
	uint32_t reserved;
};

static InodeInfo fs_do_emptytrash(uint32_t ts) {
	InodeInfo ii{0, 0};

	auto it = gMetadata->trash.cbegin();
	while (it != gMetadata->trash.cend()) {
		FSNodeFile *node = fsnodes_id_to_node_verify<FSNodeFile>((*it).first);

		assert(node->type == FSNode::kTrash);

		++it;

		if (((uint64_t)(node->atime) + node->trashtime < (uint64_t)ts)) {
			if (fsnodes_purge(ts, node)) {
				ii.free++;
			} else {
				ii.reserved++;
			}
#ifdef LIZARDFS_HAVE_64BIT_JUDY
			it.reload();
#else
			static_assert(std::is_same<NodePathContainer, flat_map<uint32_t, hstorage::Handle>>::value,
			              "Iterator must internally be a pointer to contiguous structure");
			--it;
#endif
		}
	}

	return ii;
}

#ifndef METARESTORE
void fs_periodic_emptytrash(void) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	InodeInfo ii = fs_do_emptytrash(ts);
	if (ii.free > 0 || ii.reserved > 0) {
		fs_changelog(ts, "EMPTYTRASH():%" PRIu32 ",%" PRIu32, ii.free, ii.reserved);
	}
}
#endif

uint8_t fs_apply_emptytrash(uint32_t ts, uint32_t freeinodes, uint32_t reservedinodes) {
	InodeInfo ii = fs_do_emptytrash(ts);
	gMetadata->metaversion++;
	if ((freeinodes != ii.free) || (reservedinodes != ii.reserved)) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	return LIZARDFS_STATUS_OK;
}

uint32_t fs_do_emptyreserved(uint32_t ts) {
	uint32_t fi = 0;
	auto it = gMetadata->reserved.cbegin();
	while (it != gMetadata->reserved.cend()) {
		FSNodeFile *node = fsnodes_id_to_node_verify<FSNodeFile>((*it).first);

		assert(node->type == FSNode::kReserved);

		++it;

		if (node && node->sessionid.empty()) {
			fsnodes_purge(ts,node);
			fi++;
#ifdef LIZARDFS_HAVE_64BIT_JUDY
			it.reload();
#else
			static_assert(std::is_same<NodePathContainer, flat_map<uint32_t, hstorage::Handle>>::value,
			              "Iterator must internally be a pointer to contiguous structure");
			--it;
#endif
		}
	}
	return fi;
}

#ifndef METARESTORE
void fs_periodic_emptyreserved(void) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	uint32_t fi = fs_do_emptyreserved(ts);
	if (fi>0) {
		fs_changelog(ts, "EMPTYRESERVED():%" PRIu32,fi);
	}
}
#endif

uint8_t fs_apply_emptyreserved(uint32_t ts,uint32_t freeinodes) {
	uint32_t fi = fs_do_emptyreserved(ts);
	gMetadata->metaversion++;
	if (freeinodes!=fi) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	return LIZARDFS_STATUS_OK;
}
