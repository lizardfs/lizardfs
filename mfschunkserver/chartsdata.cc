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

enum CHARTS_TYPES {
	CHARTS_UCPU,
	CHARTS_SCPU,
	CHARTS_MASTERIN,
	CHARTS_MASTEROUT,
	CHARTS_CSCONNIN,
	CHARTS_CSCONNOUT,
	CHARTS_CSSERVIN,
	CHARTS_CSSERVOUT,
	CHARTS_BYTESR,
	CHARTS_BYTESW,
	CHARTS_LLOPR,
	CHARTS_LLOPW,
	CHARTS_DATABYTESR,
	CHARTS_DATABYTESW,
	CHARTS_DATALLOPR,
	CHARTS_DATALLOPW,
	CHARTS_HLOPR,
	CHARTS_HLOPW,
	CHARTS_RTIME,
	CHARTS_WTIME,
	CHARTS_REPL,
	CHARTS_CREATE,
	CHARTS_DELETE,
	CHARTS_VERSION,
	CHARTS_DUPLICATE,
	CHARTS_TRUNCATE,
	CHARTS_DUPTRUNC,
	CHARTS_TEST,
	CHARTS_CHUNKIOJOBS,
	CHARTS_CHUNKOPJOBS,

	CHART_COUNT
};

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
static uint64_t data_every_minute[CHART_COUNT];
static uint16_t counter_of_seconds = 0;

void chartsdata_refresh(void) {
	uint64_t data_realtime[CHART_COUNT];
	uint64_t bin,bout;
	uint32_t i,opr,opw,dbr,dbw,dopr,dopw,repl;
	uint32_t op_cr,op_de,op_ve,op_du,op_tr,op_dt,op_te;
	uint32_t csservjobs,masterjobs;
	struct itimerval uc,pc;
	uint32_t ucusec,pcusec;

	for (i=0 ; i<CHART_COUNT ; i++) {		// initialisation
		data_realtime[i]=CHARTS_NODATA;
	}

	if (counter_of_seconds == 0) {
		for (i = 0; i < CHART_COUNT; ++i) {	// initialisation & reset after a minute
			data_every_minute[i] = CHARTS_NODATA;
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
		if (i == CHARTS_UCPU || i == CHARTS_SCPU || i == CHARTS_MASTERIN || i == CHARTS_MASTEROUT || i == CHARTS_CSCONNIN || i == CHARTS_CSCONNOUT ||
				i == CHARTS_CSSERVIN || i == CHARTS_CSSERVOUT || i == CHARTS_BYTESR || i == CHARTS_BYTESW || i == CHARTS_DATABYTESR || i == CHARTS_DATABYTESW ||
				i == CHARTS_CHUNKOPJOBS|| i == CHARTS_CHUNKIOJOBS)
			continue;
		data_realtime[i] *= kMinute;
	}
	charts_add(data_realtime, main_time() - kSecond, true, false);

	++counter_of_seconds;
	if (counter_of_seconds == kMinute) {
		// average needed stats
		if (data_every_minute[CHARTS_UCPU] != CHARTS_NODATA)
			data_every_minute[CHARTS_UCPU] /= kMinute;
		if (data_every_minute[CHARTS_SCPU] != CHARTS_NODATA)
			data_every_minute[CHARTS_SCPU] /= kMinute;
		if (data_every_minute[CHARTS_MASTERIN] != CHARTS_NODATA)
			data_every_minute[CHARTS_MASTERIN] /= kMinute;
		if (data_every_minute[CHARTS_MASTEROUT]	!= CHARTS_NODATA)
			data_every_minute[CHARTS_MASTEROUT] /= kMinute;
		if (data_every_minute[CHARTS_CSCONNIN] != CHARTS_NODATA)
			data_every_minute[CHARTS_CSCONNIN] /= kMinute;
		if (data_every_minute[CHARTS_CSCONNOUT]	!= CHARTS_NODATA)
			data_every_minute[CHARTS_CSCONNOUT]	/= kMinute;
		if (data_every_minute[CHARTS_CSSERVIN] != CHARTS_NODATA)
			data_every_minute[CHARTS_CSSERVIN] /= kMinute;
		if (data_every_minute[CHARTS_CSSERVOUT] != CHARTS_NODATA)
			data_every_minute[CHARTS_CSSERVOUT]	/= kMinute;
		if (data_every_minute[CHARTS_BYTESR] != CHARTS_NODATA)
			data_every_minute[CHARTS_BYTESR] /= kMinute;
		if (data_every_minute[CHARTS_BYTESW] != CHARTS_NODATA)
			data_every_minute[CHARTS_BYTESW] /= kMinute;
		if (data_every_minute[CHARTS_DATABYTESR] != CHARTS_NODATA)
			data_every_minute[CHARTS_DATABYTESR] /= kMinute;
		if (data_every_minute[CHARTS_DATABYTESW] != CHARTS_NODATA)
			data_every_minute[CHARTS_DATABYTESW] /= kMinute;

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
