/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of LizardFS and is part of LizardFS.

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
#include "common/main.h"
#include "common/metadata.h"
#include "common/rotate_files.h"
#include "master/matomlserv.h"

#define MAXLOGLINESIZE 200000U
#define MAXLOGNUMBER 1000U
static uint32_t BackLogsNumber;
static FILE *fd;
static bool gFlush = true;

void changelog_rotate() {
	if (fd) {
		fclose(fd);
		fd=NULL;
	}
	if (BackLogsNumber>0) {
		rotateFiles(kChangelogFilename, BackLogsNumber);
	} else {
		unlink(kChangelogFilename);
	}
	matomlserv_broadcast_logrotate();
}

void changelog(uint64_t version,const char *format,...) {
	static char printbuff[MAXLOGLINESIZE];
	va_list ap;
	uint32_t leng;

	va_start(ap,format);
	leng = vsnprintf(printbuff,MAXLOGLINESIZE,format,ap);
	va_end(ap);
	if (leng>=MAXLOGLINESIZE) {
		printbuff[MAXLOGLINESIZE-1]='\0';
		leng=MAXLOGLINESIZE;
	} else {
		leng++;
	}

	if (fd==NULL) {
		fd = fopen(kChangelogFilename, "a");
		if (!fd) {
			syslog(LOG_NOTICE,"lost LFS change %" PRIu64 ": %s",version,printbuff);
		}
	}

	if (fd) {
		fprintf(fd,"%" PRIu64 ": %s\n",version,printbuff);
		if (gFlush) {
			fflush(fd);
		}
	}
	matomlserv_broadcast_logstring(version,(uint8_t*)printbuff,leng);
}

void changelog_reload(void) {
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	if (BackLogsNumber>MAXLOGNUMBER) {
		syslog(LOG_WARNING,"BACK_LOGS value too big !!!");
		BackLogsNumber = MAXLOGLINESIZE;
	}
}

int changelog_init(void) {
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	if (BackLogsNumber>MAXLOGNUMBER) {
		fprintf(stderr,"BACK_LOGS value too big !!!");
		return -1;
	}
	main_reloadregister(changelog_reload);
	fd = NULL;
	return 0;
}

void changelog_disable_flush(void) {
	gFlush = false;
}

void changelog_enable_flush(void) {
	gFlush = true;
	if (fd) {
		fflush(fd);
	}
}

