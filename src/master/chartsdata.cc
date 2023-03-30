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
#include "master/chartsdata.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include "common/charts.h"
#include "common/event_loop.h"
#include "master/chunks.h"
#include "master/filesystem.h"
#include "master/filesystem_operations.h"
#include "master/matoclserv.h"

#if defined(LIZARDFS_HAVE_GETRUSAGE) && defined(LIZARDFS_HAVE_STRUCT_RUSAGE_RU_MAXRSS)
#  include <sys/types.h>
#  ifdef LIZARDFS_HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#  ifdef LIZARDFS_HAVE_SYS_RUSAGE_H
#    include <sys/rusage.h>
#  endif
#  ifndef RUSAGE_SELF
#    define RUSAGE_SELF 0
#  endif
#  define MEMORY_USAGE 1
#endif

#if defined(LIZARDFS_HAVE_SETITIMER)
#  include <sys/time.h>
#  ifndef ITIMER_REAL
#    define ITIMER_REAL 0
#  endif
#  ifndef ITIMER_VIRTUAL
#    define ITIMER_VIRTUAL 1
#  endif
#  ifndef ITIMER_PROF
#    define ITIMER_PROF 2
#  endif
#  define CPU_USAGE 1
#endif

#define CHARTS_FILENAME "stats.mfs"

#define CHARTS_UCPU 0
#define CHARTS_SCPU 1
#define CHARTS_DELCHUNK 2
#define CHARTS_REPLCHUNK 3
#define CHARTS_STATFS 4
#define CHARTS_GETATTR 5
#define CHARTS_SETATTR 6
#define CHARTS_LOOKUP 7
#define CHARTS_MKDIR 8
#define CHARTS_RMDIR 9
#define CHARTS_SYMLINK 10
#define CHARTS_READLINK 11
#define CHARTS_MKNOD 12
#define CHARTS_UNLINK 13
#define CHARTS_RENAME 14
#define CHARTS_LINK 15
#define CHARTS_READDIR 16
#define CHARTS_OPEN 17
#define CHARTS_READ 18
#define CHARTS_WRITE 19
#define CHARTS_MEMORY 20
#define CHARTS_PACKETSRCVD 21
#define CHARTS_PACKETSSENT 22
#define CHARTS_BYTESRCVD 23
#define CHARTS_BYTESSENT 24

#define CHARTS 25

/* name , join mode , percent , scale , multiplier , divisor */
#define STATDEFS { \
	{"ucpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"scpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"delete"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"replicate"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"statfs"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"getattr"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"setattr"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"lookup"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mkdir"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"rmdir"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"symlink"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"readlink"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mknod"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"unlink"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"rename"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"link"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"readdir"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"open"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"read"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"write"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"memory"       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"prcvd"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"psent"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"brcvd"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"bsent"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{NULL           ,0              ,0,0                 ,   0, 0}  \
};

#define CALCDEFS { \
	CHARTS_DEFS_END \
};

/* c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{CHARTS_DIRECT(CHARTS_UCPU)        ,CHARTS_DIRECT(CHARTS_SCPU)        ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{CHARTS_NONE                       ,CHARTS_NONE                       ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};

static const uint32_t calcdefs[]=CALCDEFS
static const statdef statdefs[]=STATDEFS
static const estatdef estatdefs[]=ESTATDEFS

#ifdef CPU_USAGE
static struct itimerval it_set;

static void chartsdata_refresh_cpu_itcp(int which, uint32_t *xcusec) {
	struct itimerval xc;
	setitimer(which, &it_set, &xc);

	if (xc.it_value.tv_sec<=999) {
		// on fucken linux timers can go backward !!!
		xc.it_value.tv_sec = 999 - xc.it_value.tv_sec;
		xc.it_value.tv_usec = 999999 - xc.it_value.tv_usec;
	} else {
		// TODO(zserik) I don't think that's correct...
		xc.it_value.tv_sec = 0;
		xc.it_value.tv_usec = 0;
	}

	*xcusec = xc.it_value.tv_sec * 1000000U + xc.it_value.tv_usec;
}
#endif

#ifdef MEMORY_USAGE
static uint64_t memusage;
uint64_t chartsdata_memusage(void) {
	return memusage;
}
#else
uint64_t chartsdata_memusage(void) {
	return 0;
}
#endif

void chartsdata_refresh(void) {
	uint64_t data[CHARTS];
	std::array<uint32_t, FsStats::Size> fsdata;
	uint32_t i,del,repl; //,bin,bout,opr,opw,dbr,dbw,dopr,dopw,repl;
#ifdef CPU_USAGE
	struct itimerval uc,pc;
	uint32_t ucusec,pcusec;
#endif
#ifdef MEMORY_USAGE
	struct rusage ru;
#endif

	for (i=0 ; i<CHARTS ; i++) {
		data[i]=CHARTS_NODATA;
	}

#ifdef CPU_USAGE
	// CPU usage
	chartsdata_refresh_cpu_itcp(ITIMER_VIRTUAL, &ucusec); // user time
	chartsdata_refresh_cpu_itcp(ITIMER_PROF,    &pcusec); // user time + system_time
	if (pcusec > ucusec) {
		pcusec -= ucusec;
	} else {
		pcusec = 0;
	}
	data[CHARTS_UCPU] = ucusec;
	data[CHARTS_SCPU] = pcusec;
#endif

// memory usage
#ifdef MEMORY_USAGE
	getrusage(RUSAGE_SELF,&ru);
#  ifdef __APPLE__
	memusage = ru.ru_maxrss;
#  else
	memusage = ru.ru_maxrss * UINT64_C(1024);
#  endif
#  ifdef __linux__
	if (memusage==0) {
		int fd = open("/proc/self/statm",O_RDONLY);
		char statbuff[1000];
		int l;
		if (fd>=0) {
			l = read(fd,statbuff,1000);
			if (l<1000 && l>0) {
				statbuff[l]=0;
				memusage = strtoul(statbuff,NULL,10)*getpagesize();
			}
			close(fd);
		}
	}
#  endif
	if (memusage>0) {
		data[CHARTS_MEMORY] = memusage;
	}
#endif

	chunk_stats(&del,&repl);
	data[CHARTS_DELCHUNK]=del;
	data[CHARTS_REPLCHUNK]=repl;
	fs_retrieve_stats(fsdata);
	for (i = 0 ; i < FsStats::Size; ++i) {
		data[CHARTS_STATFS + i] = fsdata[i];
	}
	matoclserv_stats(data+CHARTS_PACKETSRCVD);

	charts_add(data,eventloop_time()-60);
}

void chartsdata_term(void) {
	chartsdata_refresh();
	charts_store();
	charts_term();
}

int chartsdata_init (void) {
#ifdef CPU_USAGE
	struct itimerval uc,pc;
#endif
#ifdef MEMORY_USAGE
	struct rusage ru;
#endif

#ifdef CPU_USAGE
	it_set.it_interval.tv_sec = 0;
	it_set.it_interval.tv_usec = 0;
	it_set.it_value.tv_sec = 999;
	it_set.it_value.tv_usec = 999999;
	setitimer(ITIMER_VIRTUAL,&it_set,&uc);             // user time
	setitimer(ITIMER_PROF,&it_set,&pc);                // user time + system time
#endif
#ifdef MEMORY_USAGE
	getrusage(RUSAGE_SELF,&ru);
#  ifdef __APPLE__
	memusage = ru.ru_maxrss;
#  else
	memusage = ru.ru_maxrss * 1024;
#  endif
#endif

	eventloop_timeregister(TIMEMODE_RUN_LATE,60,0,chartsdata_refresh);
	eventloop_timeregister(TIMEMODE_RUN_LATE,3600,0,charts_store);
	eventloop_destructregister(chartsdata_term);
	return charts_init(calcdefs,statdefs,estatdefs,CHARTS_FILENAME);
}
