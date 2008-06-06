/*
   Copyright 2008 Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdarg.h>
#include <syslog.h>
#include <unistd.h>

#include "main.h"
#include "changelog.h"
#include "matocsserv.h"
#include "config.h"

#define MAXLOGLINESIZE 10000
static uint32_t BackLogsNumber;
static FILE *fd;

void rotatelog() {
	char logname1[100],logname2[100];
	uint32_t i;
	if (fd) {
		fclose(fd);
		fd=NULL;
	}
	if (BackLogsNumber>0) {
		for (i=BackLogsNumber ; i>0 ; i--) {
			snprintf(logname1,100,"changelog.%d.mfs",i);
			snprintf(logname2,100,"changelog.%d.mfs",i-1);
			rename(logname2,logname1);
		}
	} else {
		unlink("changelog.0.mfs");
	}
	matocsserv_broadcast_logrotate();
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
		fd = fopen("changelog.0.mfs","a");
		if (!fd) {
			syslog(LOG_NOTICE,"lost MFS change %llu: %s",version,printbuff);
		}
	}

	if (fd) {
		fprintf(fd,"%llu: %s\n",version,printbuff);
		fflush(fd);
	}
	matocsserv_broadcast_logstring(version,(uint8_t*)printbuff,leng);
}

int changelog_init(void) {
	config_getuint32("BACK_LOGS",50,&BackLogsNumber);
	fd = NULL;
	return 0;
}
