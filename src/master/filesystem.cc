/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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
#include "master/filesystem.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/debug_log.h"
#include "common/exceptions.h"
#include "common/hashfn.h"
#include "common/lizardfs_version.h"
#include "common/massert.h"
#include "common/metadata.h"
#include "common/MFSCommunication.h"
#include "common/rotate_files.h"
#include "common/setup.h"
#include "common/slogger.h"
#include "common/tape_copies.h"
#include "common/tape_copy_state.h"
#include "master/checksum.h"
#include "master/chunks.h"
#include "master/filesystem_bst.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_checksum_background_updater.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_freenode.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_periodic.h"
#include "master/filesystem_store.h"
#include "master/filesystem_xattr.h"
#include "master/goal_config_loader.h"
#include "master/matomlserv.h"
#include "master/matotsserv.h"
#include "master/personality.h"
#include "master/restore.h"
#include "master/quota_database.h"

#ifdef LIZARDFS_HAVE_PWD_H
#  include <pwd.h>
#endif
#ifndef METARESTORE
#  include "common/cfg.h"
#  include "common/main.h"
#  include "master/changelog.h"
#  include "master/datacachemgr.h"
#  include "master/matoclserv.h"
#  include "master/matocsserv.h"
#  include "master/matotsserv.h"
#endif

#define CHIDS_NO 0
#define CHIDS_YES 1
#define CHIDS_AUTO 2

FilesystemMetadata* gMetadata = nullptr;

#ifndef METARESTORE

static void* gEmptyTrashHook;
static void* gEmptyReservedHook;
static void* gFreeInodesHook;
static bool gAutoRecovery = false;
bool gMagicAutoFileRepair = false;
bool gAtimeDisabled = false;
MetadataDumper metadataDumper(kMetadataFilename, kMetadataTmpFilename);


uint32_t test_start_time;

static bool gSaveMetadataAtExit = true;

// Configuration of goals
GoalMap<Goal> gGoalDefinitions;

#endif // ifndef METARESTORE

// Number of changelog file versions
uint32_t gStoredPreviousBackMetaCopies;

// Checksum validation
bool gDisableChecksumVerification = false;


ChecksumBackgroundUpdater gChecksumBackgroundUpdater;

enum {FLAG_TREE,FLAG_TRASH,FLAG_RESERVED};

#ifdef METARESTORE

void fs_disable_checksum_verification(bool value) {
	gDisableChecksumVerification = value;
}

/* DUMP */

void fs_dumpedge(fsedge *e) {
	if (e->parent==NULL) {
		if (e->child->type==TYPE_TRASH) {
			printf("E|p:     TRASH|c:%10" PRIu32 "|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		} else if (e->child->type==TYPE_RESERVED) {
			printf("E|p:  RESERVED|c:%10" PRIu32 "|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		} else {
			printf("E|p:      NULL|c:%10" PRIu32 "|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		}
	} else {
		printf("E|p:%10" PRIu32 "|c:%10" PRIu32 "|n:%s\n",e->parent->id,e->child->id,fsnodes_escape_name(e->nleng,e->name));
	}
}

void fs_dumpnode(fsnode *f) {
	char c;
	uint32_t i,ch;
	sessionidrec *sessionidptr;

	c='?';
	switch (f->type) {
	case TYPE_DIRECTORY:
		c='D';
		break;
	case TYPE_SOCKET:
		c='S';
		break;
	case TYPE_FIFO:
		c='F';
		break;
	case TYPE_BLOCKDEV:
		c='B';
		break;
	case TYPE_CHARDEV:
		c='C';
		break;
	case TYPE_SYMLINK:
		c='L';
		break;
	case TYPE_FILE:
		c='-';
		break;
	case TYPE_TRASH:
		c='T';
		break;
	case TYPE_RESERVED:
		c='R';
		break;
	}

	printf("%c|i:%10" PRIu32 "|#:%" PRIu8 "|e:%1" PRIX16 "|m:%04" PRIo16 "|u:%10" PRIu32 "|g:%10" PRIu32 "|a:%10" PRIu32 ",m:%10" PRIu32 ",c:%10" PRIu32 "|t:%10" PRIu32,c,f->id,f->goal,(uint16_t)(f->mode>>12),(uint16_t)(f->mode&0xFFF),f->uid,f->gid,f->atime,f->mtime,f->ctime,f->trashtime);

	if (f->type==TYPE_BLOCKDEV || f->type==TYPE_CHARDEV) {
		printf("|d:%5" PRIu32 ",%5" PRIu32 "\n",f->data.devdata.rdev>>16,f->data.devdata.rdev&0xFFFF);
	} else if (f->type==TYPE_SYMLINK) {
		printf("|p:%s\n",fsnodes_escape_name(f->data.sdata.pleng,f->data.sdata.path));
	} else if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_RESERVED) {
		printf("|l:%20" PRIu64 "|c:(",f->data.fdata.length);
		ch = 0;
		for (i=0 ; i<f->data.fdata.chunks ; i++) {
			if (f->data.fdata.chunktab[i]!=0) {
				ch=i+1;
			}
		}
		for (i=0 ; i<ch ; i++) {
			if (f->data.fdata.chunktab[i]!=0) {
				printf("%016" PRIX64,f->data.fdata.chunktab[i]);
			} else {
				printf("N");
			}
			if (i+1<ch) {
				printf(",");
			}
		}
		printf(")|r:(");
		for (sessionidptr=f->data.fdata.sessionids ; sessionidptr ; sessionidptr=sessionidptr->next) {
			printf("%" PRIu32,sessionidptr->sessionid);
			if (sessionidptr->next) {
				printf(",");
			}
		}
		printf(")\n");
	} else {
		printf("\n");
	}
}

void fs_dumpnodes() {
	uint32_t i;
	fsnode *p;
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (p=gMetadata->nodehash[i] ; p ; p=p->next) {
			fs_dumpnode(p);
		}
	}
}

void fs_dumpedgelist(fsedge *e) {
	while (e) {
		fs_dumpedge(e);
		e=e->nextchild;
	}
}

void fs_dumpedges(fsnode *f) {
	fsedge *e;
	fs_dumpedgelist(f->data.ddata.children);
	for (e=f->data.ddata.children ; e ; e=e->nextchild) {
		if (e->child->type==TYPE_DIRECTORY) {
			fs_dumpedges(e->child);
		}
	}
}

void fs_dumpfree() {
	freenode *n;
	for (n=gMetadata->freelist ; n ; n=n->next) {
		printf("I|i:%10" PRIu32 "|f:%10" PRIu32 "\n",n->id,n->ftime);
	}
}

void xattr_dump() {
	uint32_t i;
	xattr_data_entry *xa;

	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		for (xa=gMetadata->xattr_data_hash[i] ; xa ; xa=xa->next) {
			printf("X|i:%10" PRIu32 "|n:%s|v:%s\n",xa->inode,fsnodes_escape_name(xa->anleng,xa->attrname),fsnodes_escape_name(xa->avleng,xa->attrvalue));
		}
	}
}


void fs_dump(void) {
	fs_dumpnodes();
	fs_dumpedges(gMetadata->root);
	fs_dumpedgelist(gMetadata->trash);
	fs_dumpedgelist(gMetadata->reserved);
	fs_dumpfree();
	xattr_dump();
}

#endif


void fs_unlock() {
	gMetadataLockfile->unlock();
}

#ifndef METARESTORE

static void metadataPollDesc(std::vector<pollfd> &pdesc) {
	metadataDumper.pollDesc(pdesc);
}

static void metadataPollServe(const std::vector<pollfd> &pdesc) {
	bool metadataDumpInProgress = metadataDumper.inProgress();
	metadataDumper.pollServe(pdesc);
	if (metadataDumpInProgress && !metadataDumper.inProgress()) {
		if (metadataDumper.dumpSucceeded()) {
			if (fs_commit_metadata_dump()) {
				fs_broadcast_metadata_saved(LIZARDFS_STATUS_OK);
			} else {
				fs_broadcast_metadata_saved(LIZARDFS_ERROR_IO);
			}
		} else {
			fs_broadcast_metadata_saved(LIZARDFS_ERROR_IO);
			if (metadataDumper.useMetarestore()) {
				// master should recalculate its checksum
				syslog(LOG_WARNING, "dumping metadata failed, recalculating checksum");
				fs_start_checksum_recalculation();
			}
			unlink(kMetadataTmpFilename);
		}
	}
}

void fs_periodic_storeall() {
	fs_storeall(MetadataDumper::kBackgroundDump); // ignore error
}

void fs_term(void) {
	if (metadataDumper.inProgress()) {
		metadataDumper.waitUntilFinished();
	}
	bool metadataStored = false;
	if (gMetadata != nullptr && gSaveMetadataAtExit) {
		for (;;) {
			metadataStored = (fs_storeall(MetadataDumper::kForegroundDump) == LIZARDFS_STATUS_OK);
			if (metadataStored) {
				break;
			}
			syslog(LOG_ERR,"can't store metadata - try to make more space on your hdd or change privieleges - retrying after 10 seconds");
			sleep(10);
		}
	}
	if (metadataStored) {
		// Remove the lock to say that the server has gently stopped and saved its metadata.
		fs_unlock();
	} else if (gMetadata != nullptr && !gSaveMetadataAtExit) {
		// We will leave the lockfile present to indicate, that our metadata.mfs file should not be
		// loaded (it is not up to date -- some changelogs need to be applied). Write a message
		// which tells that the lockfile is not left because of a crash, but because we have been
		// asked to stop without saving metadata. Include information about version of metadata
		// which can be recovered using our changelogs.
		auto message = "quick_stop: " + std::to_string(gMetadata->metaversion) + "\n";
		gMetadataLockfile->writeMessage(message);
	} else {
		// We will leave the lockfile present to indicate, that our metadata.mfs file should not be
		// loaded (it is not up to date, because we didn't manage to download the most recent).
		// Write a message which tells that the lockfile is not left because of a crash, but because
		// we have been asked to stop before loading metadata. Don't overwrite 'quick_stop' though!
		if (!gMetadataLockfile->hasMessage()) {
			gMetadataLockfile->writeMessage("no_metadata: 0\n");
		}
	}
}

void fs_disable_metadata_dump_on_exit() {
	gSaveMetadataAtExit = false;
}

#else
void fs_storeall(const char *fname) {
	FILE *fd;
	fd = fopen(fname,"w");
	if (fd==NULL) {
		lzfs_pretty_syslog(LOG_ERR, "can't open metadata file");
		return;
	}
	fs_store_fd(fd);

	if (ferror(fd)!=0) {
		lzfs_pretty_syslog(LOG_ERR, "can't write metadata");
	} else if (fflush(fd) == EOF) {
		lzfs_pretty_syslog(LOG_ERR, "can't fflush metadata");
	} else if (fsync(fileno(fd)) == -1) {
		lzfs_pretty_syslog(LOG_ERR, "can't fsync metadata");
	}
	fclose(fd);
}

void fs_term(const char *fname, bool noLock) {
	if (!noLock) {
		gMetadataLockfile->eraseMessage();
	}
	fs_storeall(fname);
	if (!noLock) {
		fs_unlock();
	}
}
#endif

void fs_strinit(void) {
	gMetadata = new FilesystemMetadata;
}

/* executed in master mode */
#ifndef METARESTORE

/// Returns true iff we are allowed to swallow a stale lockfile and apply changelogs.
static bool fs_can_do_auto_recovery() {
	return gAutoRecovery || main_has_extra_argument("auto-recovery", CaseSensitivity::kIgnore);
}

void fs_erase_message_from_lockfile() {
	if (gMetadataLockfile != nullptr) {
		gMetadataLockfile->eraseMessage();
	}
}

int fs_loadall(void) {
	fs_strinit();
	chunk_strinit();
	changelogsMigrateFrom_1_6_29("changelog");
	if (fs::exists(kMetadataTmpFilename)) {
		throw MetadataFsConsistencyException(
				"temporary metadata file exists, metadata directory is in dirty state");
	}
	if ((metadataserver::isMaster()) && !fs::exists(kMetadataFilename)) {
		fs_unlock();
		std::string currentPath = fs::getCurrentWorkingDirectoryNoThrow();
		throw FilesystemException("can't open metadata file "+ currentPath + "/" + kMetadataFilename
					+ ": if this is a new installation create empty metadata by copying "
					+ currentPath + "/" + kMetadataFilename + ".empty to " + currentPath
					+ "/" + kMetadataFilename);
	}
	fs_loadall(kMetadataFilename, 0);

	bool autoRecovery = fs_can_do_auto_recovery();
	if (autoRecovery || (metadataserver::getPersonality() == metadataserver::Personality::kShadow)) {
		lzfs_pretty_syslog_attempt(LOG_INFO, "%s - applying changelogs from %s",
				(autoRecovery ? "AUTO_RECOVERY enabled" : "running in shadow mode"),
				fs::getCurrentWorkingDirectoryNoThrow().c_str());
		fs_load_changelogs();
		lzfs_pretty_syslog(LOG_INFO, "all needed changelogs applied successfully");
	}
	return 0;
}

void fs_cs_disconnected(void) {
	test_start_time = main_time()+600;
}

/*
 * Initialize subsystems required by Master personality of metadataserver.
 */
void fs_become_master() {
	if (!gMetadata) {
		syslog(LOG_ERR, "Attempted shadow->master transition without metadata - aborting");
		exit(1);
	}
	dcm_clear();
	test_start_time = main_time() + 900;
	main_timeregister(TIMEMODE_RUN_LATE, 1, 0, fs_periodic_test_files);
	main_eachloopregister(fs_background_checksum_recalculation_a_bit);
	gEmptyTrashHook = main_timeregister(TIMEMODE_RUN_LATE,
			cfg_get_minvalue<uint32_t>("EMPTY_TRASH_PERIOD", 300, 1),
			0, fs_periodic_emptytrash);
	gEmptyReservedHook = main_timeregister(TIMEMODE_RUN_LATE,
			cfg_get_minvalue<uint32_t>("EMPTY_RESERVED_INODES_PERIOD", 60, 1),
			0, fs_periodic_emptyreserved);
	gFreeInodesHook = main_timeregister(TIMEMODE_RUN_LATE,
			cfg_get_minvalue<uint32_t>("FREE_INODES_PERIOD", 60, 1),
			0, fs_periodic_freeinodes);
	return;
}

static void fs_read_goals_from_stream(std::istream&& stream) {
	GoalConfigLoader loader;
	loader.load(std::move(stream));
	gGoalDefinitions = loader.goals();
	for (unsigned i = goal::kMinXorLevel; i <= goal::kMaxXorLevel; ++i) {
		gGoalDefinitions[goal::xorLevelToGoal(i)] = Goal::getXorGoal(i);
	}
}

static void fs_read_goal_config_file() {
	std::string goalConfigFile =
			cfg_getstring("CUSTOM_GOALS_FILENAME", "");
	if (goalConfigFile.empty()) {
		// file is not specified
		const char *defaultGoalConfigFile = ETC_PATH "/mfsgoals.cfg";
		if (access(defaultGoalConfigFile, F_OK) == 0) {
			// the default file exists - use it
			goalConfigFile = defaultGoalConfigFile;
		} else {
			lzfs_pretty_syslog(LOG_WARNING,
					"goal configuration file %s not found - using default goals; if you don't "
					"want to define custom goals create an empty file %s to disable this warning",
					defaultGoalConfigFile, defaultGoalConfigFile);
			fs_read_goals_from_stream(std::stringstream()); // empty means defaults
			return;
		}
	}
	std::ifstream goalConfigStream(goalConfigFile);
	if (!goalConfigStream.good()) {
		throw ConfigurationException("failed to open goal definitions file " + goalConfigFile);
	}
	try {
		fs_read_goals_from_stream(std::move(goalConfigStream));
		lzfs_pretty_syslog(LOG_INFO,
				"initialized goal definitions from file %s",
				goalConfigFile.c_str());
	} catch (Exception& ex) {
		throw ConfigurationException(
				"malformed goal definitions in " + goalConfigFile + ": " + ex.message());
	}
}

static void fs_read_config_file() {
	gAutoRecovery = cfg_getint32("AUTO_RECOVERY", 0) == 1;
	gDisableChecksumVerification = cfg_getint32("DISABLE_METADATA_CHECKSUM_VERIFICATION", 0) != 0;
	gMagicAutoFileRepair = cfg_getint32("MAGIC_AUTO_FILE_REPAIR", 0) == 1;
	gAtimeDisabled = cfg_getint32("NO_ATIME", 0) == 1;
	gStoredPreviousBackMetaCopies = cfg_get_maxvalue(
			"BACK_META_KEEP_PREVIOUS",
			kDefaultStoredPreviousBackMetaCopies,
			kMaxStoredPreviousBackMetaCopies);

	ChecksumUpdater::setPeriod(cfg_getint32("METADATA_CHECKSUM_INTERVAL", 50));
	gChecksumBackgroundUpdater.setSpeedLimit(
			cfg_getint32("METADATA_CHECKSUM_RECALCULATION_SPEED", 100));
	metadataDumper.setMetarestorePath(
			cfg_get("MFSMETARESTORE_PATH", std::string(SBIN_PATH "/mfsmetarestore")));
	metadataDumper.setUseMetarestore(cfg_getint32("MAGIC_PREFER_BACKGROUND_DUMP", 0));

	fs_read_goal_config_file(); // may throw
}

void fs_reload(void) {
	try {
		fs_read_config_file();
	} catch (Exception& ex) {
		syslog(LOG_WARNING, "Error in configuration: %s", ex.what());
	}
	if (metadataserver::isMaster()) {
		main_timechange(gEmptyTrashHook, TIMEMODE_RUN_LATE,
				cfg_get_minvalue<uint32_t>("EMPTY_TRASH_PERIOD", 300, 1), 0);
		main_timechange(gEmptyReservedHook, TIMEMODE_RUN_LATE,
				cfg_get_minvalue<uint32_t>("EMPTY_RESERVED_INODES_PERIOD", 60, 1), 0);
		main_timechange(gFreeInodesHook, TIMEMODE_RUN_LATE,
				cfg_get_minvalue<uint32_t>("FREE_INODES_PERIOD", 60, 1), 0);
	}
}

void fs_unload() {
	lzfs_pretty_syslog(LOG_WARNING, "unloading filesystem at %" PRIu64, fs_getversion());
	restore_reset();
	matoclserv_session_unload();
	chunk_unload();
	dcm_clear();
	delete gMetadata;
	gMetadata = nullptr;
}

int fs_init(bool doLoad) {
	fs_read_config_file();
	if (!gMetadataLockfile) {
		gMetadataLockfile.reset(new Lockfile(kMetadataFilename + std::string(".lock")));
	}
	if (!gMetadataLockfile->isLocked()) {
		try {
			gMetadataLockfile->lock((fs_can_do_auto_recovery() || !metadataserver::isMaster()) ?
					Lockfile::StaleLock::kSwallow : Lockfile::StaleLock::kReject);
		} catch (const LockfileException& e) {
			if (e.reason() == LockfileException::Reason::kStaleLock) {
				throw LockfileException(
						std::string(e.what()) + ", consider running `mfsmetarestore -a' to fix problems with your datadir.",
						LockfileException::Reason::kStaleLock);
			}
			throw;
		}
	}
	changelog_init(kChangelogFilename, 0, 50);

	if (doLoad || (metadataserver::isMaster())) {
		fs_loadall();
	}
	main_reloadregister(fs_reload);
	metadataserver::registerFunctionCalledOnPromotion(fs_become_master);
	if (!cfg_isdefined("MAGIC_DISABLE_METADATA_DUMPS")) {
		// Secret option disabling periodic metadata dumps
		main_timeregister(TIMEMODE_RUN_LATE,3600,0,fs_periodic_storeall);
	}
	if (metadataserver::isMaster()) {
		fs_become_master();
	}
	main_pollregister(metadataPollDesc, metadataPollServe);
	main_destructregister(fs_term);
	return 0;
}

/*
 * Initialize filesystem subsystem if currently metadataserver have Master personality.
 */
int fs_init() {
	return fs_init(false);
}

#else
int fs_init(const char *fname,int ignoreflag, bool noLock) {
	if (!noLock) {
		gMetadataLockfile.reset(new Lockfile(fs::dirname(fname) + "/" + kMetadataFilename + ".lock"));
		gMetadataLockfile->lock(Lockfile::StaleLock::kSwallow);
	}
	fs_strinit();
	chunk_strinit();
	fs_loadall(fname,ignoreflag);
	return 0;
}
#endif
