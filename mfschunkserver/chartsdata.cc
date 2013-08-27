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

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/resource.h>

#include "charts.h"
#include "main.h"

#include "csserv.h"
#include "masterconn.h"
#include "hddspacemgr.h"
#include "replicator.h"
#include "time_constants.h"

#define CHARTS_FILENAME "csstats.mfs"

#define CHARTS_UCPU 0
#define CHARTS_SCPU 1
#define CHARTS_MASTERIN 2
#define CHARTS_MASTEROUT 3
#define CHARTS_CSCONNIN 4
#define CHARTS_CSCONNOUT 5
#define CHARTS_CSSERVIN 6
#define CHARTS_CSSERVOUT 7
#define CHARTS_BYTESR 8
#define CHARTS_BYTESW 9
#define CHARTS_LLOPR 10
#define CHARTS_LLOPW 11
#define CHARTS_DATABYTESR 12
#define CHARTS_DATABYTESW 13
#define CHARTS_DATALLOPR 14
#define CHARTS_DATALLOPW 15
#define CHARTS_HLOPR 16
#define CHARTS_HLOPW 17
#define CHARTS_RTIME 18
#define CHARTS_WTIME 19
#define CHARTS_REPL 20
#define CHARTS_CREATE 21
#define CHARTS_DELETE 22
#define CHARTS_VERSION 23
#define CHARTS_DUPLICATE 24
#define CHARTS_TRUNCATE 25
#define CHARTS_DUPTRUNC 26
#define CHARTS_TEST 27
#define CHARTS_CHUNKIOJOBS 28
#define CHARTS_CHUNKOPJOBS 29

#define CHARTS 30

/* name , join mode , percent , scale , multiplier , divisor */
#define STATDEFS { \
	{"ucpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"scpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"masterin"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"masterout"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"csconnin"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"csconnout"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"csservin"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"csservout"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"bytesr"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"bytesw"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"llopr"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"llopw"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"databytesr"   ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"databytesw"   ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"datallopr"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"datallopw"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"hlopr"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"hlopw"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"rtime"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MICRO,   1,60}, \
	{"wtime"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MICRO,   1,60}, \
	{"repl"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"create"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"delete"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"version"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"duplicate"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"truncate"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"duptrunc"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"test"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chunkiojobs"  ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chunkopjobs"  ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{NULL           ,0              ,0,0                 ,   0, 0}  \
};

#define CALCDEFS { \
	CHARTS_DEFS_END \
};

/* c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{CHARTS_DIRECT(CHARTS_UCPU)        ,CHARTS_DIRECT(CHARTS_SCPU)        ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{CHARTS_DIRECT(CHARTS_CSSERVIN)    ,CHARTS_DIRECT(CHARTS_CSCONNIN)    ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{CHARTS_DIRECT(CHARTS_CSSERVOUT)   ,CHARTS_DIRECT(CHARTS_CSCONNOUT)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{CHARTS_DIRECT(CHARTS_BYTESR)      ,CHARTS_DIRECT(CHARTS_DATABYTESR)  ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{CHARTS_DIRECT(CHARTS_BYTESW)      ,CHARTS_DIRECT(CHARTS_DATABYTESW)  ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{CHARTS_DIRECT(CHARTS_LLOPR)       ,CHARTS_DIRECT(CHARTS_DATALLOPR)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_DIRECT(CHARTS_LLOPW)       ,CHARTS_DIRECT(CHARTS_DATALLOPW)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_DIRECT(CHARTS_CHUNKOPJOBS) ,CHARTS_DIRECT(CHARTS_CHUNKIOJOBS) ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_NONE                       ,CHARTS_NONE                       ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};


static const uint32_t calcdefs[]=CALCDEFS
static const statdef statdefs[]=STATDEFS
static const estatdef estatdefs[]=ESTATDEFS

static struct itimerval it_set;

// variables for stats gathered every 1 minute
static uint64_t data_every_minute[CHARTS];
static uint16_t counter_of_seconds = 0;

void chartsdata_refresh(void) {
	uint64_t data_realtime[CHARTS];
	uint64_t bin,bout;
	uint32_t i,opr,opw,dbr,dbw,dopr,dopw,repl;
	uint32_t op_cr,op_de,op_ve,op_du,op_tr,op_dt,op_te;
	uint32_t csservjobs,masterjobs;
	struct itimerval uc,pc;
	uint32_t ucusec,pcusec;

	for (i=0 ; i<CHARTS ; i++) {		// initialisation
		data_realtime[i]=0;
	}

	if (counter_of_seconds == 0) {
		for (i = 0; i < CHARTS; ++i) {	// initialisation & reset after a minute
			data_every_minute[i] = 0;
		}
	}
	
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

	ucusec = uc.it_value.tv_sec*1000000+uc.it_value.tv_usec;
	pcusec = pc.it_value.tv_sec*1000000+pc.it_value.tv_usec;

	if (pcusec>ucusec) {
		pcusec-=ucusec;
	} else {
		pcusec=0;
	}
	data_realtime[CHARTS_UCPU] = ucusec;
	data_realtime[CHARTS_SCPU] = pcusec;

	masterconn_stats(&bin,&bout,&masterjobs);
	data_realtime[CHARTS_MASTERIN]=bin;
	data_realtime[CHARTS_MASTEROUT]=bout;
	data_realtime[CHARTS_CHUNKOPJOBS]=masterjobs;
	data_realtime[CHARTS_CSCONNIN]=0;
	data_realtime[CHARTS_CSCONNOUT]=0;
	csserv_stats(&bin,&bout,&opr,&opw,&csservjobs);
	data_realtime[CHARTS_CSSERVIN]=bin;
	data_realtime[CHARTS_CSSERVOUT]=bout;
	data_realtime[CHARTS_CHUNKIOJOBS]=csservjobs;
	data_realtime[CHARTS_HLOPR]=opr;
	data_realtime[CHARTS_HLOPW]=opw;
	hdd_stats(&bin,&bout,&opr,&opw,&dbr,&dbw,&dopr,&dopw,data_realtime+CHARTS_RTIME,data_realtime+CHARTS_WTIME);
	data_realtime[CHARTS_BYTESR]=bin;
	data_realtime[CHARTS_BYTESW]=bout;
	data_realtime[CHARTS_LLOPR]=opr;
	data_realtime[CHARTS_LLOPW]=opw;
	data_realtime[CHARTS_DATABYTESR]=dbr;
	data_realtime[CHARTS_DATABYTESW]=dbw;
	data_realtime[CHARTS_DATALLOPR]=dopr;
	data_realtime[CHARTS_DATALLOPW]=dopw;
	replicator_stats(&repl);
	data_realtime[CHARTS_REPL]=repl;
	hdd_op_stats(&op_cr,&op_de,&op_ve,&op_du,&op_tr,&op_dt,&op_te);
	data_realtime[CHARTS_CREATE]=op_cr;
	data_realtime[CHARTS_DELETE]=op_de;
	data_realtime[CHARTS_VERSION]=op_ve;
	data_realtime[CHARTS_DUPLICATE]=op_du;
	data_realtime[CHARTS_TRUNCATE]=op_tr;
	data_realtime[CHARTS_DUPTRUNC]=op_dt;
	data_realtime[CHARTS_TEST]=op_te;

	charts_add(data_realtime, main_time() - kSecond, true, false);
	
	// Gathering data
	for (i = 0; i < CHARTS; ++i) {
		data_every_minute[i] += data_realtime[i];
	}
	++counter_of_seconds;
	
	if (counter_of_seconds == kMinute) {
		// average needed stats
		data_every_minute[CHARTS_UCPU]			/= kMinute;
		data_every_minute[CHARTS_SCPU]			/= kMinute;
		data_every_minute[CHARTS_MASTERIN]		/= kMinute;
		data_every_minute[CHARTS_MASTEROUT]		/= kMinute;
		data_every_minute[CHARTS_CSCONNIN]		/= kMinute;
		data_every_minute[CHARTS_CSCONNOUT]		/= kMinute;
		data_every_minute[CHARTS_CSSERVIN]		/= kMinute;
		data_every_minute[CHARTS_CSSERVOUT]		/= kMinute;
		data_every_minute[CHARTS_BYTESR]			/= kMinute;
		data_every_minute[CHARTS_BYTESW]			/= kMinute;
		data_every_minute[CHARTS_DATABYTESR]		/= kMinute;
		data_every_minute[CHARTS_DATABYTESW]		/= kMinute;

		// engage!
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
	struct itimerval uc,pc;

	it_set.it_interval.tv_sec = 0;
	it_set.it_interval.tv_usec = 0;
	it_set.it_value.tv_sec = 999;
	it_set.it_value.tv_usec = 999999;
	setitimer(ITIMER_VIRTUAL,&it_set,&uc);             // user time
	setitimer(ITIMER_PROF,&it_set,&pc);                // user time + system time

	main_timeregister(TIMEMODE_RUN_LATE, kSecond, 0, chartsdata_refresh);
	main_timeregister(TIMEMODE_RUN_LATE, kHour, 0, chartsdata_store);
	main_destructregister(chartsdata_term);
	return charts_init(calcdefs,statdefs,estatdefs,CHARTS_FILENAME);
}
