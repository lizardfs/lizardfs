/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2017 Skytechnology sp. z o.o..

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

#include "common/cfg.h"
#include "common/event_loop.h"
#ifdef LIZARDFS_HAVE_64BIT_JUDY
#  include "common/judy_map.h"
#else
#  include "common/flat_map.h"
#endif
#include "common/loop_watchdog.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"
#include "master/filesystem_operations.h"
#include "master/matoclserv.h"

#define MSGBUFFSIZE 1000000
#define ERRORS_LOG_MAX 500
#define FILETESTSMINLOOPTIME 1
#define FILETESTSMAXLOOPTIME 7200

#ifndef METARESTORE

static uint32_t fsinfo_files = 0;
static uint32_t fsinfo_ugfiles = 0;
static uint32_t fsinfo_mfiles = 0;
static uint32_t fsinfo_chunks = 0;
static uint32_t fsinfo_ugchunks = 0;
static uint32_t fsinfo_mchunks = 0;
static uint32_t fsinfo_loopstart = 0;
static uint32_t fsinfo_loopend = 0;
static uint32_t fsinfo_notfoundchunks = 0;
static uint32_t fsinfo_unavailchunks = 0;
static uint32_t fsinfo_unavailfiles = 0;
static uint32_t fsinfo_unavailtrashfiles = 0;
static uint32_t fsinfo_unavailreservedfiles = 0;

static int gTasksBatchSize = 1000;

static int gFileTestLoopTime = 300;
static int gFileTestLoopIndex = 0;
static unsigned gFileTestLoopBucketLimit = 0;

enum NodeErrorFlag {
	kChunkUnavailable = 1,
	kChunkUnderGoal   = 2,
	kStructureError   = 4,
	kAllNodeErrors    = 7
};

#ifdef LIZARDFS_HAVE_64BIT_JUDY
	typedef judy_map<uint32_t, uint8_t> DefectiveNodesMap;
#else
	typedef flat_map<uint32_t, uint8_t> DefectiveNodesMap;
#endif

static const size_t kMaxNodeEntries = 1000000;
static DefectiveNodesMap gDefectiveNodes;

void fs_background_task_manager_work() {
	if (gMetadata->task_manager.workAvailable()) {
		uint32_t ts = eventloop_time();
		ChecksumUpdater cu(ts);
		gMetadata->task_manager.processJobs(ts, gTasksBatchSize);
		if (gMetadata->task_manager.workAvailable()) {
			eventloop_make_next_poll_nonblocking();
		}
	}
}

static std::string get_node_info(FSNode *node) {
	std::string name;
	if (node == nullptr) {
		return name;
	}
	if (node->type == FSNode::kTrash) {
		name = "file in trash " + std::to_string(node->id) + ": " +
		       (std::string)gMetadata->trash.at(TrashPathKey(node));
	} else if (node->type == FSNode::kReserved) {
		name = "reserved file " + std::to_string(node->id) + ": " +
		       (std::string)gMetadata->reserved.at(node->id);
	} else if (node->type == FSNode::kFile) {
		name = "file " + std::to_string(node->id) + ": ";
		bool first = true;
		for (const auto parent_inode : node->parent) {
			std::string path;
			FSNodeDirectory *parent =
			        fsnodes_id_to_node_verify<FSNodeDirectory>(parent_inode);
			fsnodes_getpath(parent, node, path);
			if (!first) {
				name += "|" + path;
			} else {
				name += path;
			}
			first = false;
		}
	} else if (node->type == FSNode::kDirectory) {
		name = "directory " + std::to_string(node->id) + ": ";
		std::string path;
		FSNodeDirectory *parent = nullptr;
		if (!node->parent.empty()) {
			parent = fsnodes_id_to_node_verify<FSNodeDirectory>(node->parent.front());
		}
		fsnodes_getpath(parent, node, path);
		name += path;
	}

	return fsnodes_escape_name(name);
}

std::vector<DefectiveFileInfo> fs_get_defective_nodes_info(uint8_t requested_flags, uint64_t max_entries,
	                                                   uint64_t &entry_index) {
	FSNode *node;
	std::vector<DefectiveFileInfo> defective_nodes_info;
	ActiveLoopWatchdog watchdog;
	defective_nodes_info.reserve(max_entries);
	auto it = gDefectiveNodes.find_nth(entry_index);
	watchdog.start();
	for (uint64_t i = 0; i < max_entries && it != gDefectiveNodes.end(); ++it) {
		if (((*it).second & requested_flags) != 0) {
			node = fsnodes_id_to_node<FSNode>((*it).first);
			std::string info = get_node_info(node);
			defective_nodes_info.emplace_back(std::move(info), (*it).second);
			++i;
		}
		++entry_index;
		if (watchdog.expired()) {
			return defective_nodes_info;
		}
	}
	entry_index = 0;
	return defective_nodes_info;
}

void fs_test_getdata(uint32_t &loopstart, uint32_t &loopend, uint32_t &files, uint32_t &ugfiles,
		uint32_t &mfiles, uint32_t &chunks, uint32_t &ugchunks, uint32_t &mchunks,
		std::string &result) {
	std::stringstream report;
	int errors = 0;

	for (const auto &entry : gDefectiveNodes) {
		if (errors >= ERRORS_LOG_MAX) {
			break;
		}

		FSNode *node = fsnodes_id_to_node<FSNode>(entry.first);
		if (!node) {
			report << "Structure error in defective list, entry " << std::to_string(entry.first) << "\n";
			errors++;
			continue;
		}

		if (node->type == FSNode::kFile || node->type == FSNode::kTrash ||
		    node->type == FSNode::kReserved) {
			FSNodeFile *file_node = static_cast<FSNodeFile *>(node);
			for (std::size_t j = 0; j < file_node->chunks.size(); ++j) {
				auto chunkid = file_node->chunks[j];
				if (chunkid == 0) {
					continue;
				}

				uint8_t vc;
				if (chunk_get_fullcopies(chunkid, &vc) != LIZARDFS_STATUS_OK) {
					report << "structure error - chunk " << chunkid
					       << " not found (inode: " << file_node->id
					       << " ; index: " << j << ")\n";
					errors++;
				} else if (vc == 0) {
					report << "currently unavailable chunk " << chunkid
					       << " (inode: " << file_node->id << " ; index: " << j
					       << ")\n";
					errors++;
				}
			}
		}

		if (errors >= ERRORS_LOG_MAX) {
			break;
		}

		if (entry.second & kChunkUnavailable) {
			assert(node->type == FSNode::kFile || node->type == FSNode::kTrash ||
			       node->type == FSNode::kReserved);
			std::string name = get_node_info(node);
			if (node->type == FSNode::kTrash) {
				report << "-";
			} else if (node->type == FSNode::kReserved) {
				report << "+";
			} else {
				report << "*";
			}
			report << " currently unavailable " << name << "\n";
			errors++;
		}

		if (errors >= ERRORS_LOG_MAX) {
			break;
		}

		if (entry.second & kStructureError) {
			std::string name = get_node_info(node);
			report << "Structure error in " << name << "\n";
			errors++;
		}

		if (errors >= ERRORS_LOG_MAX) {
			break;
		}
	}

	if (errors >= ERRORS_LOG_MAX) {
		report << "only first " << errors
		       << " errors (unavailable chunks/files) were logged\n";
	}
	if (fsinfo_notfoundchunks > 0) {
		report << "unknown chunks: " << fsinfo_notfoundchunks << "\n";
	}
	if (fsinfo_unavailchunks > 0) {
		report << "unavailable chunks: " << fsinfo_unavailchunks << "\n";
	}
	if (fsinfo_unavailtrashfiles > 0) {
		report << "unavailable trash files: " << fsinfo_unavailtrashfiles << "\n";
	}
	if (fsinfo_unavailreservedfiles > 0) {
		report << "unavailable reserved files: " << fsinfo_unavailreservedfiles << "\n";
	}
	if (fsinfo_unavailfiles > 0) {
		report << "unavailable files: " << fsinfo_unavailfiles << "\n";
	}
	result = report.str();

	files = fsinfo_files;
	ugfiles = fsinfo_ugfiles;
	mfiles = fsinfo_mfiles;
	chunks = fsinfo_chunks;
	ugchunks = fsinfo_ugchunks;
	mchunks = fsinfo_mchunks;
	loopstart = fsinfo_loopstart;
	loopend = fsinfo_loopend;
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
	eventloop_make_next_poll_nonblocking();
}

void fs_process_file_test() {
	uint32_t k;
	uint8_t vc, node_error_flag;
	ActiveLoopWatchdog watchdog;

	static uint32_t files = 0;
	static uint32_t ugfiles = 0;
	static uint32_t mfiles = 0;
	static uint32_t chunks = 0;
	static uint32_t ugchunks = 0;
	static uint32_t mchunks = 0;
	static uint32_t notfoundchunks = 0;
	static uint32_t unavailchunks = 0;
	static uint32_t unavailfiles = 0;
	static uint32_t unavailtrashfiles = 0;
	static uint32_t unavailreservedfiles = 0;

	FSNode *f;

	if (gFileTestLoopIndex == 0) {
		fsinfo_files = files;
		fsinfo_ugfiles = ugfiles;
		fsinfo_mfiles = mfiles;
		fsinfo_chunks = chunks;
		fsinfo_ugchunks = ugchunks;
		fsinfo_mchunks = mchunks;
		fsinfo_loopstart = fsinfo_loopend;
		fsinfo_loopend = eventloop_time();
		fsinfo_notfoundchunks = notfoundchunks;
		fsinfo_unavailchunks = unavailchunks;
		fsinfo_unavailfiles = unavailfiles;
		fsinfo_unavailtrashfiles = unavailtrashfiles;
		fsinfo_unavailreservedfiles = unavailreservedfiles;

		files = 0;
		ugfiles = 0;
		mfiles = 0;
		chunks = 0;
		ugchunks = 0;
		mchunks = 0;
		notfoundchunks = 0;
		unavailchunks = 0;
		unavailfiles = 0;
		unavailtrashfiles = 0;
		unavailreservedfiles = 0;
	}

	watchdog.start();
	for (k = 0; k < gFileTestLoopBucketLimit && gFileTestLoopIndex < NODEHASHSIZE;
	     k++, gFileTestLoopIndex++) {
		if (k > 0 && watchdog.expired()) {
			gFileTestLoopBucketLimit -= k;
			return;
		}

		for (f = gMetadata->nodehash[gFileTestLoopIndex]; f; f = f->next) {
			node_error_flag = 0;

			if (f->type == FSNode::kFile || f->type == FSNode::kTrash ||
			    f->type == FSNode::kReserved) {
				for (const auto &chunkid : static_cast<FSNodeFile *>(f)->chunks) {
					if (chunkid == 0) {
						continue;
					}

					if (chunk_get_fullcopies(chunkid, &vc) !=
					    LIZARDFS_STATUS_OK) {
						node_error_flag |=
						        static_cast<int>(kChunkUnavailable);
						notfoundchunks++;
						mchunks++;
					} else if (vc == 0) {
						node_error_flag |=
						        static_cast<int>(kChunkUnavailable);
						unavailchunks++;
						mchunks++;
					} else {
						int recover, remove;
						chunk_get_partstomodify(chunkid, recover, remove);
						if (recover > 0) {
							node_error_flag |=
							        static_cast<int>(kChunkUnderGoal);
							ugchunks++;
						}
					}
					chunks++;
				}
			}

			if (f->type == FSNode::kDirectory) {
				for (const auto &entry :
				     static_cast<FSNodeDirectory *>(f)->entries) {
					FSNode *node = entry.second;

					if (!node ||
					    std::find(node->parent.begin(), node->parent.end(),
					              f->id) == node->parent.end()) {
						node_error_flag |=
						        static_cast<int>(kStructureError);
					}
				}
			}

			if (node_error_flag == 0) {
				auto it = gDefectiveNodes.find(f->id);
				if (it != gDefectiveNodes.end()) {
					gDefectiveNodes.erase(it);
				}
				continue;
			}

			if (node_error_flag & kChunkUnavailable) {
				if (f->type == FSNode::kTrash) {
					unavailtrashfiles++;
				} else if (f->type == FSNode::kReserved) {
					unavailreservedfiles++;
				} else {
					unavailfiles += f->parent.size();
				}

				auto it = gDefectiveNodes.find(f->id);
				if (it == gDefectiveNodes.end()) {
					std::string name = get_node_info(f);
					lzfs_pretty_syslog(LOG_ERR, "Chunks unavailable in %s",
					                   name.c_str());
				}
			}
			if (node_error_flag & kChunkUnderGoal) {
				ugfiles++;
			}
			if (node_error_flag & kStructureError) {
				auto it = gDefectiveNodes.find(f->id);
				if (it == gDefectiveNodes.end()) {
					std::string name = get_node_info(f);
					lzfs_pretty_syslog(LOG_ERR, "Structure error in %s",
					                   name.c_str());
				}
			}

			if (gDefectiveNodes.size() < kMaxNodeEntries) {
				gDefectiveNodes[f->id] = node_error_flag;
			} else {
				auto it = gDefectiveNodes.find(f->id);
				if (it != gDefectiveNodes.end()) {
					(*it).second = node_error_flag;
				}
			}
		}
	}

	gFileTestLoopBucketLimit -= k;
	if (gFileTestLoopIndex >= NODEHASHSIZE) {
		gFileTestLoopIndex = 0;
	}
}

void fs_periodic_file_test() {
	if (eventloop_time() <= gTestStartTime) {
		gFileTestLoopBucketLimit = 0;
		return;
	}

	if (gFileTestLoopBucketLimit == 0) {
		gFileTestLoopBucketLimit = NODEHASHSIZE / gFileTestLoopTime;
		fs_process_file_test();
	}
}

void fs_background_file_test(void) {
	if (gFileTestLoopBucketLimit > 0) {
		fs_process_file_test();
		if (gFileTestLoopBucketLimit > 0) {
			eventloop_make_next_poll_nonblocking();
		}
	}
}

void fsnodes_periodic_remove(uint32_t inode) {
	auto it = gDefectiveNodes.find(inode);
	if (it != gDefectiveNodes.end()) {
		gDefectiveNodes.erase(it);
	}
}
#endif

struct InodeInfo {
	uint32_t free;
	uint32_t reserved;
};

#ifndef METARESTORE
static void fs_do_emptytrash(uint32_t ts) {
	SignalLoopWatchdog watchdog;

	auto it = gMetadata->trash.begin();
	watchdog.start();
	while (it != gMetadata->trash.end() && ((*it).first.timestamp < ts)) {
		FSNodeFile *node = fsnodes_id_to_node_verify<FSNodeFile>((*it).first.id);

		if (!node) {
			gMetadata->trash.erase(it);
			it = gMetadata->trash.begin();
			continue;
		}

		assert(node->type == FSNode::kTrash);

		uint32_t node_id = node->id;
		fsnodes_purge(ts, node);

		// Purge operation should be performed anyway - if it fails, inode will be reserved
		fs_changelog(ts, "PURGE(%" PRIu32 ")", node_id);

		it = gMetadata->trash.begin();

		if (watchdog.expired()) {
			break;
		}
	}
}
#endif

static InodeInfo fs_do_emptytrash_deprecated(uint32_t ts) {
	InodeInfo ii{0, 0};

	auto it = gMetadata->trash.begin();
	while (it != gMetadata->trash.end() && ((*it).first.timestamp < ts)) {
		FSNodeFile *node = fsnodes_id_to_node_verify<FSNodeFile>((*it).first.id);

		if (!node) {
			gMetadata->trash.erase(it);
			it = gMetadata->trash.begin();
			continue;
		}

		assert(node->type == FSNode::kTrash);

		if (fsnodes_purge(ts, node)) {
			ii.free++;
		} else {
			ii.reserved++;
		}

		it = gMetadata->trash.begin();
	}
	return ii;
}

#ifndef METARESTORE
void fs_periodic_emptytrash(void) {
	uint32_t ts = eventloop_time();
	fs_do_emptytrash(ts);
}
#endif

uint8_t fs_apply_emptytrash_deprecated(uint32_t ts, uint32_t freeinodes, uint32_t reservedinodes) {
	InodeInfo ii = fs_do_emptytrash_deprecated(ts);
	gMetadata->metaversion++;
	if ((freeinodes != ii.free) || (reservedinodes != ii.reserved)) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_apply_emptyreserved_deprecated(uint32_t /*ts*/,uint32_t /*freeinodes*/) {
	return LIZARDFS_STATUS_OK;
}

#ifndef METARESTORE
void fs_read_periodic_config_file() {
	gFileTestLoopTime = cfg_get_minmaxvalue<uint32_t>("FILE_TEST_LOOP_MIN_TIME", 3600, FILETESTSMINLOOPTIME, FILETESTSMAXLOOPTIME);
}

void fs_periodic_master_init() {
	eventloop_timeregister(TIMEMODE_RUN_LATE, 1, 0, fs_periodic_file_test);
	eventloop_eachloopregister(fs_background_checksum_recalculation_a_bit);
	eventloop_eachloopregister(fs_background_task_manager_work);
	eventloop_eachloopregister(fs_background_file_test);
	eventloop_timeregister_ms(100, fs_periodic_emptytrash);
}
#endif
