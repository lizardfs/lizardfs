/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

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

#include "config.h"

#include <stdio.h>
#include <stdarg.h>
#include <syslog.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "main.h"
#include "changelog.h"
#include "matomlserv.h"
#include "cfg.h"

#define MAXLOGLINESIZE 200000U
#define MAXLOGNUMBER 1000U
static uint32_t BackLogsNumber;
static FILE *logf;

uint64_t findfirstlogversion(const char *fname) {
	uint8_t buff[50];
	int32_t s,p;
	uint64_t fv;
	int fd;

	fd = open(fname,O_RDONLY);
	if (fd<0) {
		return 0;
	}
	s = read(fd,buff,50);
	close(fd);
	if (s<=0) {
		return 0;
	}
	fv = 0;
	p = 0;
	while (p<s && buff[p]>='0' && buff[p]<='9') {
		fv *= 10;
		fv += buff[p]-'0';
		p++;
	}
	if (p>=s || buff[p]!=':') {
		return 0;
	}
	return fv;
}

uint64_t findlastlogversion(const char *fname) {
	struct stat st;
	uint8_t buff[32800];	// 32800 = 32768 + 32
	uint64_t size;
	uint32_t buffpos;
	uint64_t lastnewline,lv;
	int fd;

	fd = open(fname,O_RDONLY);
	if (fd<0) {
		return 0;
	}
	fstat(fd,&st);
	size = st.st_size;
	memset(buff,0,32);
	lastnewline = 0;
	while (size>0 && size+200000>(uint64_t)(st.st_size)) {
		if (size>32768) {
			memcpy(buff+32768,buff,32);
			size-=32768;
			lseek(fd,size,SEEK_SET);
			if (read(fd,buff,32768)!=32768) {
				close(fd);
				return 0;
			}
			buffpos = 32768;
		} else {
			memmove(buff+size,buff,32);
			lseek(fd,0,SEEK_SET);
			if (read(fd,buff,size)!=(ssize_t)size) {
				close(fd);
				return 0;
			}
			buffpos = size;
			size = 0;
		}
		// size = position in file of first byte in buff
		// buffpos = position of last byte in buff to search
		while (buffpos>0) {
			buffpos--;
			if (buff[buffpos]=='\n') {
				if (lastnewline==0) {
					lastnewline = size + buffpos;
				} else {
					if (lastnewline+1 != (uint64_t)(st.st_size)) {	// garbage at the end of file
						close(fd);
						return 0;
					}
					buffpos++;
					lv = 0;
					while (buffpos<32800 && buff[buffpos]>='0' && buff[buffpos]<='9') {
						lv *= 10;
						lv += buff[buffpos]-'0';
						buffpos++;
					}
					if (buffpos==32800 || buff[buffpos]!=':') {
						lv = 0;
					}
					close(fd);
					return lv;
				}
			}
		}
	}
	close(fd);
	return 0;
}

int changelog_checkname(const char *fname) {
    int len = strlen(fname);
	return strncmp(fname,"changelog",9)==0 && strncmp(fname+len-4,".mfs",4)==0;
}

void changelog_rotate() {
	char logname1[100],logname2[100];
	uint32_t i;
	if (logf) {
		fclose(logf);
		logf=NULL;
	}
	if (BackLogsNumber>0) {
		for (i=BackLogsNumber ; i>0 ; i--) {
			snprintf(logname1,100,"changelog.%"PRIu32".mfs",i);
			snprintf(logname2,100,"changelog.%"PRIu32".mfs",i-1);
			rename(logname2,logname1);
		}
	} else {
		unlink("changelog.0.mfs");
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

	if (logf==NULL) {
		logf = fopen("changelog.0.mfs","a");
		if (!logf) {
			syslog(LOG_NOTICE,"lost MFS change %"PRIu64": %s",version,printbuff);
		}
	}

	if (logf) {
		fprintf(logf,"%"PRIu64": %s\n",version,printbuff);
		fflush(logf);
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
	logf = NULL;
	return 0;
}
