/*
   Copyright 2016 Skytechnology sp. z o.o..

   This file is part of LizardFS.

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

#include "common/cfg.h"
#include "common/event_loop.h"
#include "master/hstring_memstorage.h"
#ifdef LIZARDFS_HAVE_DB
  #include "master/hstring_bdbstorage.h"
#endif

static int gUseBDBStorage;
static std::string gBDBStoragePath;
static uint64_t gBDBStorageCacheSize;

void hstorage_reload() {
	int use_bdb = cfg_getuint8("USE_BDB_NAME_STORAGE", 0);
	std::string bdb_path = cfg_getstring("DATA_PATH", DATA_PATH);
	uint64_t cache_size = cfg_getuint32("BDB_NAME_STORAGE_CACHE_SIZE", 10);

	if (use_bdb != gUseBDBStorage) {
		lzfs_pretty_syslog(LOG_ERR, "Changing USE_BDB_NAME_STORAGE requires restart.");
	}

	if (gUseBDBStorage && bdb_path != gBDBStoragePath) {
		lzfs_pretty_syslog(LOG_ERR,
		                   "Changing DATA_PATH with enabled BDB name storage requires restart.");
	}

	if (cache_size != gBDBStorageCacheSize) {
		lzfs_pretty_syslog(LOG_ERR, "Changing BDB_NAME_STORAGE_CACHE_SIZE requires restart.");
	}
}

void hstorage_term(void) {
	hstorage::Storage::reset();
}

int hstorage_init() {
	gUseBDBStorage = cfg_getuint8("USE_BDB_FOR_NAME_STORAGE", 0);
	gBDBStoragePath = cfg_getstring("DATA_PATH", DATA_PATH);
	gBDBStorageCacheSize = cfg_getuint32("BDB_NAME_STORAGE_CACHE_SIZE", 10);

	if (gUseBDBStorage) {
#ifdef LIZARDFS_HAVE_DB
		hstorage::Storage::reset(new hstorage::BDBStorage(gBDBStoragePath + "/name_storage.db",
		                                                  gBDBStorageCacheSize * 1024 * 1024, 1));
#else
		lzfs_pretty_syslog(LOG_ERR, "Berkeley DB was not enabled during compilation. Falling back to default name storage.");
		hstorage::Storage::reset(new hstorage::MemStorage());
#endif
	} else {
		hstorage::Storage::reset(new hstorage::MemStorage());
	}

	eventloop_reloadregister(hstorage_reload);
	eventloop_destructregister(hstorage_term);

	return 0;
}
