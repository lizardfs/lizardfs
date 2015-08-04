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

uint32_t fs_test_log_inconsistency(fsedge *e, const char *iname, char *buff, uint32_t size) {
	uint32_t leng;
	leng = 0;
	if (e->parent) {
		syslog(LOG_ERR,
		       "structure error - %s inconsistency (edge: %" PRIu32 ",%s -> %" PRIu32 ")",
		       iname, e->parent->id, fsnodes_escape_name(e->nleng, e->name), e->child->id);
		if (leng < size) {
			leng += snprintf(buff + leng, size - leng,
			                 "structure error - %s inconsistency (edge: %" PRIu32
			                 ",%s -> %" PRIu32 ")\n",
			                 iname, e->parent->id,
			                 fsnodes_escape_name(e->nleng, e->name), e->child->id);
		}
	} else {
		if (e->child->type == TYPE_TRASH) {
			syslog(LOG_ERR,
			       "structure error - %s inconsistency (edge: TRASH,%s -> %" PRIu32 ")",
			       iname, fsnodes_escape_name(e->nleng, e->name), e->child->id);
			if (leng < size) {
				leng += snprintf(buff + leng, size - leng,
				                 "structure error - %s inconsistency (edge: "
				                 "TRASH,%s -> %" PRIu32 ")\n",
				                 iname, fsnodes_escape_name(e->nleng, e->name),
				                 e->child->id);
			}
		} else if (e->child->type == TYPE_RESERVED) {
			syslog(LOG_ERR,
			       "structure error - %s inconsistency (edge: RESERVED,%s -> %" PRIu32
			       ")",
			       iname, fsnodes_escape_name(e->nleng, e->name), e->child->id);
			if (leng < size) {
				leng += snprintf(buff + leng, size - leng,
				                 "structure error - %s inconsistency (edge: "
				                 "RESERVED,%s -> %" PRIu32 ")\n",
				                 iname, fsnodes_escape_name(e->nleng, e->name),
				                 e->child->id);
			}
		} else {
			syslog(LOG_ERR,
			       "structure error - %s inconsistency (edge: NULL,%s -> %" PRIu32 ")",
			       iname, fsnodes_escape_name(e->nleng, e->name), e->child->id);
			if (leng < size) {
				leng += snprintf(buff + leng, size - leng,
				                 "structure error - %s inconsistency (edge: "
				                 "NULL,%s -> %" PRIu32 ")\n",
				                 iname, fsnodes_escape_name(e->nleng, e->name),
				                 e->child->id);
			}
		}
	}
	return leng;
}

void fs_background_checksum_recalculation_a_bit() {
	uint32_t recalculated = 0;

	switch (gChecksumBackgroundUpdater.getStep()) {
	case ChecksumRecalculatingStep::kNone:  // Recalculation not in progress.
		return;
	case ChecksumRecalculatingStep::kTrash:
		// Trash has to be recalculated in one step, as it is on a list.
		for (fsedge *edge = gMetadata->trash; edge; edge = edge->nextchild) {
			fsedges_checksum_add_to_background(edge);
		}
		gChecksumBackgroundUpdater.incStep();
		break;
	case ChecksumRecalculatingStep::kReserved:
		// Reserved has to be recalculated in one step, as it is on a list.
		for (fsedge *edge = gMetadata->reserved; edge; edge = edge->nextchild) {
			fsedges_checksum_add_to_background(edge);
		}
		gChecksumBackgroundUpdater.incStep();
		break;
	case ChecksumRecalculatingStep::kNodes:
		// Nodes are in a hashtable, therefore they can be recalculated in multiple steps.
		while (gChecksumBackgroundUpdater.getPosition() < NODEHASHSIZE) {
			for (fsnode *node =
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
	case ChecksumRecalculatingStep::kEdges:
		// Edges (not ones in trash or reserved) are in a hashtable,
		// therefore they can be recalculated in multiple steps.
		while (gChecksumBackgroundUpdater.getPosition() < EDGEHASHSIZE) {
			for (fsedge *edge =
			             gMetadata->edgehash[gChecksumBackgroundUpdater.getPosition()];
			     edge; edge = edge->next) {
				fsedges_checksum_add_to_background(edge);
				++recalculated;
			}
			gChecksumBackgroundUpdater.incPosition();
			if (recalculated >= gChecksumBackgroundUpdater.getSpeedLimit()) {
				break;
			}
		}
		if (gChecksumBackgroundUpdater.getPosition() == EDGEHASHSIZE) {
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
	uint32_t j;
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
	fsnode *f;
	fsedge *e;

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
			syslog(LOG_ERR,
			       "only first %u errors (unavailable chunks/files) were logged",
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
				                 "unavailable chunks: %" PRIu32 "\n",
				                 unavailchunks);
			}
			unavailchunks = 0;
		}
		if (unavailtrashfiles > 0) {
			syslog(LOG_ERR, "unavailable trash files: %" PRIu32, unavailtrashfiles);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "unavailable trash files: %" PRIu32 "\n",
				                 unavailtrashfiles);
			}
			unavailtrashfiles = 0;
		}
		if (unavailreservedfiles > 0) {
			syslog(LOG_ERR, "unavailable reserved files: %" PRIu32,
			       unavailreservedfiles);
			if (leng < MSGBUFFSIZE) {
				leng += snprintf(msgbuff + leng, MSGBUFFSIZE - leng,
				                 "unavailable reserved files: %" PRIu32 "\n",
				                 unavailreservedfiles);
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
			if (f->type == TYPE_FILE || f->type == TYPE_TRASH ||
			    f->type == TYPE_RESERVED) {
				valid = 1;
				ugflag = 0;
				for (j = 0; j < f->data.fdata.chunks; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid > 0) {
						if (chunk_get_validcopies(chunkid, &vc) !=
						    LIZARDFS_STATUS_OK) {
							if (errors < ERRORS_LOG_MAX) {
								syslog(LOG_ERR,
								       "structure error - chunk "
								       "%016" PRIX64
								       " not found (inode: %" PRIu32
								       " ; index: %" PRIu32 ")",
								       chunkid, f->id, j);
								if (leng < MSGBUFFSIZE) {
									leng += snprintf(
									        msgbuff + leng,
									        MSGBUFFSIZE - leng,
									        "structure error - "
									        "chunk %016" PRIX64
									        " not found "
									        "(inode: %" PRIu32
									        " ; index: %" PRIu32
									        ")\n",
									        chunkid, f->id, j);
								}
								errors++;
							}
							notfoundchunks++;
							if ((notfoundchunks % 1000) == 0) {
								syslog(LOG_ERR,
								       "unknown chunks: %" PRIu32
								       " ...",
								       notfoundchunks);
							}
							valid = 0;
							mchunks++;
						} else if (vc == 0) {
							if (errors < ERRORS_LOG_MAX) {
								syslog(LOG_ERR,
								       "currently unavailable "
								       "chunk %016" PRIX64
								       " (inode: %" PRIu32
								       " ; index: %" PRIu32 ")",
								       chunkid, f->id, j);
								if (leng < MSGBUFFSIZE) {
									leng += snprintf(
									        msgbuff + leng,
									        MSGBUFFSIZE - leng,
									        "currently "
									        "unavailable chunk "
									        "%016" PRIX64
									        " (inode: %" PRIu32
									        " ; index: %" PRIu32
									        ")\n",
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
							// FIXME: chunk_get_validcopies is no longer
							// sufficient to test chunk status
							// Now chunk has many types of ChunkPart
							// and each one have different validity
							// requirements
							/*
							if ((goal::isXorGoal(f->goal) && vc == 1) ||
						           (goal::isOrdinaryGoal(f->goal) &&
						            vc < gGoalDefinitions[f->goal]
						                            .getExpectedCopies())) {
							ugflag = 1;
							ugchunks++;
							*/
						}
						chunks++;
					}
				}
				if (valid == 0) {
					mfiles++;
					if (f->type == TYPE_TRASH) {
						if (errors < ERRORS_LOG_MAX) {
							syslog(LOG_ERR,
							       "- currently unavailable file in "
							       "trash %" PRIu32 ": %s",
							       f->id, fsnodes_escape_name(
							                      f->parents->nleng,
							                      f->parents->name));
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(
								        msgbuff + leng,
								        MSGBUFFSIZE - leng,
								        "- currently unavailable "
								        "file in trash %" PRIu32
								        ": %s\n",
								        f->id,
								        fsnodes_escape_name(
								                f->parents->nleng,
								                f->parents->name));
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
					} else if (f->type == TYPE_RESERVED) {
						if (errors < ERRORS_LOG_MAX) {
							syslog(LOG_ERR,
							       "+ currently unavailable reserved "
							       "file %" PRIu32 ": %s",
							       f->id, fsnodes_escape_name(
							                      f->parents->nleng,
							                      f->parents->name));
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(
								        msgbuff + leng,
								        MSGBUFFSIZE - leng,
								        "+ currently unavailable "
								        "reserved file %" PRIu32
								        ": %s\n",
								        f->id,
								        fsnodes_escape_name(
								                f->parents->nleng,
								                f->parents->name));
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
						uint8_t *path;
						uint16_t pleng;
						for (e = f->parents; e; e = e->nextparent) {
							if (errors < ERRORS_LOG_MAX) {
								fsnodes_getpath(e, &pleng, &path);
								syslog(LOG_ERR,
								       "* currently unavailable "
								       "file %" PRIu32 ": %s",
								       f->id, fsnodes_escape_name(
								                      pleng, path));
								if (leng < MSGBUFFSIZE) {
									leng += snprintf(
									        msgbuff + leng,
									        MSGBUFFSIZE - leng,
									        "* currently "
									        "unavailable file "
									        "%" PRIu32 ": %s\n",
									        f->id,
									        fsnodes_escape_name(
									                pleng,
									                path));
								}
								free(path);
								errors++;
							}
							unavailfiles++;
							if ((unavailfiles % 1000) == 0) {
								syslog(LOG_ERR,
								       "unavailable files: %" PRIu32
								       " ...",
								       unavailfiles);
							}
						}
					}
				} else if (ugflag) {
					ugfiles++;
				}
				files++;
			}
			for (e = f->parents; e; e = e->nextparent) {
				if (e->child != f) {
					if (e->parent) {
						syslog(LOG_ERR,
						       "structure error - edge->child/child->edges "
						       "(node: %" PRIu32 " ; edge: %" PRIu32
						       ",%s -> %" PRIu32 ")",
						       f->id, e->parent->id,
						       fsnodes_escape_name(e->nleng, e->name),
						       e->child->id);
						if (leng < MSGBUFFSIZE) {
							leng += snprintf(
							        msgbuff + leng, MSGBUFFSIZE - leng,
							        "structure error - "
							        "edge->child/child->edges (node: "
							        "%" PRIu32 " ; edge: %" PRIu32
							        ",%s -> %" PRIu32 ")\n",
							        f->id, e->parent->id,
							        fsnodes_escape_name(e->nleng,
							                            e->name),
							        e->child->id);
						}
					} else {
						syslog(LOG_ERR,
						       "structure error - edge->child/child->edges "
						       "(node: %" PRIu32
						       " ; edge: NULL,%s -> %" PRIu32 ")",
						       f->id,
						       fsnodes_escape_name(e->nleng, e->name),
						       e->child->id);
						if (leng < MSGBUFFSIZE) {
							leng += snprintf(
							        msgbuff + leng, MSGBUFFSIZE - leng,
							        "structure error - "
							        "edge->child/child->edges (node: "
							        "%" PRIu32
							        " ; edge: NULL,%s -> %" PRIu32
							        ")\n",
							        f->id, fsnodes_escape_name(e->nleng,
							                                   e->name),
							        e->child->id);
						}
					}
				} else if (e->nextchild) {
					if (e->nextchild->prevchild != &(e->nextchild)) {
						if (leng < MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(
							        e, "nextchild/prevchild",
							        msgbuff + leng, MSGBUFFSIZE - leng);
						} else {
							fs_test_log_inconsistency(
							        e, "nextchild/prevchild", NULL, 0);
						}
					}
				} else if (e->nextparent) {
					if (e->nextparent->prevparent != &(e->nextparent)) {
						if (leng < MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(
							        e, "nextparent/prevparent",
							        msgbuff + leng, MSGBUFFSIZE - leng);
						} else {
							fs_test_log_inconsistency(
							        e, "nextparent/prevparent", NULL,
							        0);
						}
					}
				} else if (e->next) {
					if (e->next->prev != &(e->next)) {
						if (leng < MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(
							        e, "nexthash/prevhash",
							        msgbuff + leng, MSGBUFFSIZE - leng);
						} else {
							fs_test_log_inconsistency(
							        e, "nexthash/prevhash", NULL, 0);
						}
					}
				}
			}
			if (f->type == TYPE_DIRECTORY) {
				for (e = f->data.ddata.children; e; e = e->nextchild) {
					if (e->parent != f) {
						if (e->parent) {
							syslog(LOG_ERR,
							       "structure error - "
							       "edge->parent/parent->edges (node: "
							       "%" PRIu32 " ; edge: %" PRIu32
							       ",%s -> %" PRIu32 ")",
							       f->id, e->parent->id,
							       fsnodes_escape_name(e->nleng,
							                           e->name),
							       e->child->id);
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(
								        msgbuff + leng,
								        MSGBUFFSIZE - leng,
								        "structure error - "
								        "edge->parent/"
								        "parent->edges (node: "
								        "%" PRIu32
								        " ; edge: %" PRIu32
								        ",%s -> %" PRIu32 ")\n",
								        f->id, e->parent->id,
								        fsnodes_escape_name(
								                e->nleng, e->name),
								        e->child->id);
							}
						} else {
							syslog(LOG_ERR,
							       "structure error - "
							       "edge->parent/parent->edges (node: "
							       "%" PRIu32
							       " ; edge: NULL,%s -> %" PRIu32 ")",
							       f->id, fsnodes_escape_name(e->nleng,
							                                  e->name),
							       e->child->id);
							if (leng < MSGBUFFSIZE) {
								leng += snprintf(
								        msgbuff + leng,
								        MSGBUFFSIZE - leng,
								        "structure error - "
								        "edge->parent/"
								        "parent->edges (node: "
								        "%" PRIu32
								        " ; edge: NULL,%s -> "
								        "%" PRIu32 ")\n",
								        f->id,
								        fsnodes_escape_name(
								                e->nleng, e->name),
								        e->child->id);
							}
						}
					} else if (e->nextchild) {
						if (e->nextchild->prevchild != &(e->nextchild)) {
							if (leng < MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(
								        e, "nextchild/prevchild",
								        msgbuff + leng,
								        MSGBUFFSIZE - leng);
							} else {
								fs_test_log_inconsistency(
								        e, "nextchild/prevchild",
								        NULL, 0);
							}
						}
					} else if (e->nextparent) {
						if (e->nextparent->prevparent != &(e->nextparent)) {
							if (leng < MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(
								        e, "nextparent/prevparent",
								        msgbuff + leng,
								        MSGBUFFSIZE - leng);
							} else {
								fs_test_log_inconsistency(
								        e, "nextparent/prevparent",
								        NULL, 0);
							}
						}
					} else if (e->next) {
						if (e->next->prev != &(e->next)) {
							if (leng < MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(
								        e, "nexthash/prevhash",
								        msgbuff + leng,
								        MSGBUFFSIZE - leng);
							} else {
								fs_test_log_inconsistency(
								        e, "nexthash/prevhash",
								        NULL, 0);
							}
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
	fsedge *e;
	fsnode *p;
	InodeInfo ii{0, 0};
	e = gMetadata->trash;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (((uint64_t)(p->atime) + (uint64_t)(p->trashtime) < (uint64_t)ts) &&
		    ((uint64_t)(p->mtime) + (uint64_t)(p->trashtime) < (uint64_t)ts) &&
		    ((uint64_t)(p->ctime) + (uint64_t)(p->trashtime) < (uint64_t)ts)) {
			if (fsnodes_purge(ts, p)) {
				ii.free++;
			} else {
				ii.reserved++;
			}
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
	fsedge *e;
	fsnode *p;
	uint32_t fi = 0;
	e = gMetadata->reserved;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (p->data.fdata.sessionids==NULL) {
			fsnodes_purge(ts,p);
			fi++;
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
