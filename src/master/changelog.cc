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
#include "master/changelog.h"

#include <stdarg.h>
#include <stdio.h>
#include <syslog.h>
#include <unistd.h>

#include "common/cfg.h"
#include "common/event_loop.h"
#include "common/main.h"
#include "common/metadata.h"
#include "common/rotate_files.h"
#include "common/slogger.h"

/// Base name of a changelog file.
/// Sometthing like "changelog.mfs" or "changelog_ml.mfs"
static std::string gChangelogFilename;

/// Minimal acceptable value of BACK_LOGS config entry.
static uint32_t gMinBackLogsNumber = 0;

/// Maximal acceptable value of BACK_LOGS config entry.
static uint32_t gMaxBackLogsNumber = 50;

static uint32_t BackLogsNumber;
static FILE *fd = nullptr;
static bool gFlush = true;

void changelog_rotate() {
	if (fd) {
		fclose(fd);
		fd=NULL;
	}
	if (BackLogsNumber>0) {
		rotateFiles(gChangelogFilename, BackLogsNumber);
	} else {
		unlink(gChangelogFilename.c_str());
	}
}

void changelog(uint64_t version, const char* entry) {
	if (fd==NULL) {
		fd = fopen(gChangelogFilename.c_str(), "a");
		if (!fd) {
			syslog(LOG_NOTICE, "lost metadata change %" PRIu64 ": %s", version, entry);
		}
	}

	if (fd) {
		fprintf(fd,"%" PRIu64 ": %s\n", version, entry);
		if (gFlush) {
			fflush(fd);
		}
	}
}

static void changelog_reload(void) {
	BackLogsNumber = cfg_get_minmaxvalue<uint32_t>("BACK_LOGS", 50,
			gMinBackLogsNumber, gMaxBackLogsNumber);
}

void changelog_init(std::string changelogFilename,
		uint32_t minBackLogsNumber, uint32_t maxBackLogsNumber) {
	gChangelogFilename = std::move(changelogFilename);
	gMinBackLogsNumber = minBackLogsNumber;
	gMaxBackLogsNumber = maxBackLogsNumber;
	BackLogsNumber = cfg_getuint32("BACK_LOGS", 50);
	if (BackLogsNumber > gMaxBackLogsNumber) {
		throw InitializeException(cfg_filename() + ": BACK_LOGS value too big, "
				"maximum allowed is " + std::to_string(gMaxBackLogsNumber));
	}
	if (BackLogsNumber < gMinBackLogsNumber) {
		throw InitializeException(cfg_filename() + ": BACK_LOGS value too low, "
				"minimum allowed is " + std::to_string(gMinBackLogsNumber));
	}
	eventloop_reloadregister(changelog_reload);
}

uint32_t changelog_get_back_logs_config_value() {
	return BackLogsNumber;
}

void changelog_flush(void) {
	if (fd) {
		fflush(fd);
	}
}

void changelog_disable_flush(void) {
	gFlush = false;
}

void changelog_enable_flush(void) {
	gFlush = true;
	changelog_flush();
}
