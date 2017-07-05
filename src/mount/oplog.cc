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
#include "mount/oplog.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#define OPBUFFSIZE 0x1000000
#define LINELENG 1000
#define MAXHISTORYSIZE 0xF00000

typedef struct _fhentry {
	unsigned long fh;
	uint64_t readpos;
	uint32_t refcount;
	struct _fhentry *next;
} fhentry;

static unsigned long nextfh=1;
static fhentry *fhhead=NULL;

static uint8_t opbuff[OPBUFFSIZE];
static uint64_t writepos=0;
static uint8_t waiting=0;
static pthread_mutex_t opbufflock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t nodata = PTHREAD_COND_INITIALIZER;

static time_t gConvTmHour = std::numeric_limits<time_t>::max(); // enforce update on first read
static struct tm gConvTm;
static pthread_mutex_t timelock = PTHREAD_MUTEX_INITIALIZER;

static inline void oplog_put(uint8_t *buff,uint32_t leng) {
	uint32_t bpos;
	if (leng>OPBUFFSIZE) {  // just in case
		buff+=leng-OPBUFFSIZE;
		leng=OPBUFFSIZE;
	}
	pthread_mutex_lock(&opbufflock);
	bpos = writepos%OPBUFFSIZE;
	writepos+=leng;
	if (bpos+leng>OPBUFFSIZE) {
		memcpy(opbuff+bpos,buff,OPBUFFSIZE-bpos);
		buff+=OPBUFFSIZE-bpos;
		leng-=OPBUFFSIZE-bpos;
		bpos = 0;
	}
	memcpy(opbuff+bpos,buff,leng);
	if (waiting) {
		pthread_cond_broadcast(&nodata);
		waiting=0;
	}
	pthread_mutex_unlock(&opbufflock);
}

static void get_time(timeval &tv, tm &ltime) {
	gettimeofday(&tv, nullptr);
	static constexpr time_t secs_per_hour = 60 * 60;
	time_t hour = tv.tv_sec / secs_per_hour;
	unsigned secs_this_hour = tv.tv_sec % secs_per_hour;

	pthread_mutex_lock(&timelock);
	if (hour != gConvTmHour) {
		gConvTmHour = hour;
		time_t convts = hour * secs_per_hour;
		localtime_r(&convts, &gConvTm);
	}
	ltime = gConvTm;
	pthread_mutex_unlock(&timelock);

	assert(ltime.tm_sec == 0);
	assert(ltime.tm_min == 0);

	ltime.tm_sec = secs_this_hour % 60;
	ltime.tm_min = secs_this_hour / 60;
}

void oplog_printf(const struct LizardClient::Context &ctx,const char *format,...) {
	struct timeval tv;
	struct tm ltime;
	va_list ap;
	int r, leng = 0;
	char buff[LINELENG];

	get_time(tv, ltime);
	r  = snprintf(buff, LINELENG, "%llu %02u.%02u %02u:%02u:%02u.%06u: uid:%u gid:%u pid:%u cmd:",
		(unsigned long long)tv.tv_sec, ltime.tm_mon + 1, ltime.tm_mday, ltime.tm_hour, ltime.tm_min, ltime.tm_sec, (unsigned)tv.tv_usec,
		(unsigned)ctx.uid, (unsigned)ctx.gid, (unsigned)ctx.pid);
	if (r < 0) {
		return;
	}
	leng = std::min(LINELENG - 1, r);

	va_start(ap, format);
	r = vsnprintf(buff + leng, LINELENG - leng, format, ap);
	va_end(ap);
	if (r < 0) {
		return;
	}
	leng += r;

	leng = std::min(LINELENG - 1, leng);
	buff[leng++] = '\n';
	oplog_put((uint8_t*)buff, leng);
}

void oplog_printf(const char *format, ...) {
	struct timeval tv;
	struct tm ltime;
	va_list ap;
	int r, leng = 0;
	char buff[LINELENG];

	get_time(tv, ltime);
	r = snprintf(buff, LINELENG, "%llu %02u.%02u %02u:%02u:%02u.%06u: cmd:",
		(unsigned long long)tv.tv_sec, ltime.tm_mon + 1, ltime.tm_mday, ltime.tm_hour, ltime.tm_min, ltime.tm_sec, (unsigned)tv.tv_usec);
	if (r < 0) {
		return;
	}
	leng = std::min(LINELENG - 1, r);

	va_start(ap, format);
	r = vsnprintf(buff + leng, LINELENG - leng, format, ap);
	va_end(ap);
	if (r < 0) {
		return;
	}
	leng += r;

	leng = std::min(LINELENG - 1, leng);
	buff[leng++] = '\n';
	oplog_put((uint8_t*)buff, leng);
}

unsigned long oplog_newhandle(int hflag) {
	fhentry *fhptr;
	uint32_t bpos;

	pthread_mutex_lock(&opbufflock);
	fhptr = (fhentry*) malloc(sizeof(fhentry));
	fhptr->fh = nextfh++;
	fhptr->refcount = 1;
	if (hflag) {
		if (writepos<MAXHISTORYSIZE) {
			fhptr->readpos = 0;
		} else {
			fhptr->readpos = writepos - MAXHISTORYSIZE;
			bpos = fhptr->readpos%OPBUFFSIZE;
			while (fhptr->readpos < writepos) {
				if (opbuff[bpos]=='\n') {
					break;
				}
				bpos++;
				bpos%=OPBUFFSIZE;
				fhptr->readpos++;
			}
			if (fhptr->readpos<writepos) {
				fhptr->readpos++;
			}
		}
	} else {
		fhptr->readpos = writepos;
	}
	fhptr->next = fhhead;
	fhhead = fhptr;
	pthread_mutex_unlock(&opbufflock);
	return fhptr->fh;
}

void oplog_releasehandle(unsigned long fh) {
	fhentry **fhpptr,*fhptr;
	pthread_mutex_lock(&opbufflock);
	fhpptr = &fhhead;
	while ((fhptr = *fhpptr)) {
		if (fhptr->fh==fh) {
			fhptr->refcount--;
			if (fhptr->refcount==0) {
				*fhpptr = fhptr->next;
				free(fhptr);
			} else {
				fhpptr = &(fhptr->next);
			}
		} else {
			fhpptr = &(fhptr->next);
		}
	}
	pthread_mutex_unlock(&opbufflock);
}

void oplog_getdata(unsigned long fh,uint8_t **buff,uint32_t *leng,uint32_t maxleng) {
	fhentry *fhptr;
	uint32_t bpos;
	struct timeval tv;
	struct timespec ts;

	pthread_mutex_lock(&opbufflock);
	for (fhptr=fhhead ; fhptr && fhptr->fh != fh ; fhptr=fhptr->next) {
	}
	if (fhptr==NULL) {
		*buff = NULL;
		*leng = 0;
		return;
	}
	fhptr->refcount++;
	while (fhptr->readpos>=writepos) {
		gettimeofday(&tv,NULL);
		ts.tv_sec = tv.tv_sec+1;
		ts.tv_nsec = tv.tv_usec*1000;
		waiting=1;
		if (pthread_cond_timedwait(&nodata,&opbufflock,&ts)==ETIMEDOUT) {
			*buff = (uint8_t*)"#\n";
			*leng = 2;
			return;
		}
	}
	bpos = fhptr->readpos%OPBUFFSIZE;
	*leng = (writepos-(fhptr->readpos));
	*buff = opbuff+bpos;
	if ((*leng)>(OPBUFFSIZE-bpos)) {
		(*leng) = (OPBUFFSIZE-bpos);
	}
	if ((*leng)>maxleng) {
		(*leng) = maxleng;
	}
	fhptr->readpos+=(*leng);
}

void oplog_releasedata(unsigned long fh) {
	fhentry **fhpptr,*fhptr;
	fhpptr = &fhhead;
	while ((fhptr = *fhpptr)) {
		if (fhptr->fh==fh) {
			fhptr->refcount--;
			if (fhptr->refcount==0) {
				*fhpptr = fhptr->next;
				free(fhptr);
			} else {
				fhpptr = &(fhptr->next);
			}
		} else {
			fhpptr = &(fhptr->next);
		}
	}
	pthread_mutex_unlock(&opbufflock);
}
