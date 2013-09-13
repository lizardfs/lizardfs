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

#if defined(HAVE_GETRUSAGE) && defined(HAVE_STRUCT_RUSAGE_RU_MAXRSS)
#  include <sys/types.h>
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#  ifdef HAVE_SYS_RUSAGE_H
#    include <sys/rusage.h>
#  endif
#  ifndef RUSAGE_SELF
#    define RUSAGE_SELF 0
#  endif
#  define MEMORY_USAGE 1
#endif

#if defined(HAVE_SETITIMER)
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

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <syslog.h>
#include <errno.h>


#include "charts.h"
#include "main.h"

#include "chunks.h"
#include "filesystem.h"
#include "matoclserv.h"
#include "time_constants.h"

#define CHARTS_FILENAME "stats.mfs"

enum CHARTS_TYPES
{
	CHARTS_UCPU = 0,
	CHARTS_SCPU ,
	CHARTS_DELCHUNK ,
	CHARTS_REPLCHUNK ,
	CHARTS_STATFS ,
	CHARTS_GETATTR ,
	CHARTS_SETATTR ,
	CHARTS_LOOKUP ,
	CHARTS_MKDIR ,
	CHARTS_RMDIR ,
	CHARTS_SYMLINK ,
	CHARTS_READLINK ,
	CHARTS_MKNOD ,
	CHARTS_UNLINK ,
	CHARTS_RENAME ,
	CHARTS_LINK ,
	CHARTS_READDIR ,
	CHARTS_OPEN ,
	CHARTS_READ ,
	CHARTS_WRITE ,
	CHARTS_MEMORY ,
	CHARTS_PACKETSRCVD ,
	CHARTS_PACKETSSENT ,
	CHARTS_BYTESRCVD ,
	CHARTS_BYTESSENT ,

	CHART_COUNT ,
};



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
#endif
#ifdef MEMORY_USAGE
static uint64_t memusage;

uint64_t chartsdata_memusage(void) {  // used only in sections with MEMORY_USAGE
	return memusage;
}
#endif

// variables for stats gathered every 1 minute
static uint64_t data_every_minute[CHART_COUNT];
uint16_t counter_of_seconds = 0;

void chartsdata_refresh(void) {
	uint64_t data_realtime[CHART_COUNT];
	uint32_t fsdata[16];
	uint32_t i,del,repl; //,bin,bout,opr,opw,dbr,dbw,dopr,dopw,repl;
#ifdef CPU_USAGE
	struct itimerval uc,pc;
	uint32_t ucusec,pcusec;
#endif
#ifdef MEMORY_USAGE
	struct rusage ru;
#endif

	for (i=0 ; i<CHART_COUNT ; i++) {
		data_realtime[i]= CHARTS_NODATA;	// Data initialisation
	}
	
	if (counter_of_seconds == 0) {
		for (i = 0; i < CHART_COUNT; ++i) {
			data_every_minute[i] = CHARTS_NODATA;	// Data initialisation & reset every minute
		}
	}
		

#ifdef CPU_USAGE
// CPU usage
	setitimer(ITIMER_VIRTUAL,&it_set,&uc);             // user time
	setitimer(ITIMER_PROF,&it_set,&pc);                // user time + system time

	if (uc.it_value.tv_sec<=999) {	// on fucken linux timers can go backward !!!
		uc.it_value.tv_sec = 999-uc.it_value.tv_sec;
		uc.it_value.tv_usec = 999999-uc.it_value.tv_usec;
	} else {
		uc.it_value.tv_sec = 0;
		uc.it_value.tv_usec = 0;
	}
	if (pc.it_value.tv_sec<=999) {	// as abowe - who the hell has invented this stupid os !!!
		pc.it_value.tv_sec = 999-pc.it_value.tv_sec;
		pc.it_value.tv_usec = 999999-pc.it_value.tv_usec;
	} else {
		pc.it_value.tv_sec = 0;
		uc.it_value.tv_usec = 0;
	}

	ucusec = uc.it_value.tv_sec*1000000U+uc.it_value.tv_usec;
	pcusec = pc.it_value.tv_sec*1000000U+pc.it_value.tv_usec;

	if (pcusec>ucusec) {
		pcusec-=ucusec;
	} else {
		pcusec=0;
	}
	data_realtime[CHARTS_UCPU] = ucusec;
	data_realtime[CHARTS_SCPU] = pcusec;
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
		int length;
		if (fd>=0) {
			length = read(fd,statbuff,1000);
			if (length<1000 && length>0) {
				statbuff[length]=0;
				memusage = strtoul(statbuff,NULL,10)*getpagesize();
			}
			close(fd);
		}
	}
#  endif
	if (memusage>0) {
		data_realtime[CHARTS_MEMORY] = memusage;
	}
#endif

	chunk_stats(&del,&repl);
	data_realtime[CHARTS_DELCHUNK]=del;
	data_realtime[CHARTS_REPLCHUNK]=repl;
	fs_stats(fsdata);
	for (i=CHARTS_STATFS ; i<=CHARTS_WRITE ; i++) {
		data_realtime[i]=fsdata[i-CHARTS_STATFS];
	}
	matoclserv_stats(data_realtime+CHARTS_PACKETSRCVD);


	
	// Gathering data
	for (i = 0; i < CHART_COUNT; ++i) {
		if (data_realtime[i] == CHARTS_NODATA) {
			continue;
		} else if (data_every_minute[i] == CHARTS_NODATA) {
			data_every_minute[i] = data_realtime[i];
		} else if (statdefs[i].mode == CHARTS_MODE_ADD) {
			data_every_minute[i] += data_realtime[i]; // mode add
		} else if ( data_realtime[i] > data_every_minute[i]) {
			data_every_minute[i] = data_realtime[i];  // mode max
		}
	}
	for (i = 0; i < CHART_COUNT; ++i) {
		if (data_realtime[i] == CHARTS_NODATA)
			continue;
		if (i == CHARTS_UCPU || i == CHARTS_SCPU || i == CHARTS_MEMORY || i == CHARTS_PACKETSRCVD || i == CHARTS_PACKETSSENT || i == CHARTS_BYTESRCVD || i == CHARTS_BYTESSENT)
			continue;
		data_realtime[i] *= kMinute;
	}
	charts_add(data_realtime, main_time() - kSecond, true, false);

	++counter_of_seconds;
	// when a minute passed
	if (counter_of_seconds == kMinute) {
		// average needed stats
		if (data_every_minute[CHARTS_UCPU] != CHARTS_NODATA)
			data_every_minute[CHARTS_UCPU] /= kMinute;
		if (data_every_minute[CHARTS_SCPU] != CHARTS_NODATA)
			data_every_minute[CHARTS_SCPU] /= kMinute;
		if (data_every_minute[CHARTS_PACKETSRCVD] != CHARTS_NODATA)
			data_every_minute[CHARTS_PACKETSRCVD] /= kMinute;
		if (data_every_minute[CHARTS_PACKETSSENT]!= CHARTS_NODATA)
			data_every_minute[CHARTS_PACKETSSENT] /= kMinute;
		if (data_every_minute[CHARTS_BYTESRCVD]	!= CHARTS_NODATA)
			data_every_minute[CHARTS_BYTESRCVD] /= kMinute;
		if(data_every_minute[CHARTS_BYTESSENT]!= CHARTS_NODATA)
			data_every_minute[CHARTS_BYTESSENT] /= kMinute;
		
		charts_add(data_every_minute, main_time() - kMinute, false, true);
		
		counter_of_seconds = 0;
	}
}

void chartsdata_term(void) {
	chartsdata_refresh();
	charts_store();
	charts_term();
}

void chartsdata_store(void) {
	charts_store();
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

	main_timeregister(TIMEMODE_RUN_LATE, kSecond, 0, chartsdata_refresh);
	main_timeregister(TIMEMODE_RUN_LATE, kHour, 0, chartsdata_store);
	main_destructregister(chartsdata_term);
	return charts_init(calcdefs,statdefs,estatdefs,CHARTS_FILENAME);
}
