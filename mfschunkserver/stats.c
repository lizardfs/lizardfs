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

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <syslog.h>
#include <sys/time.h>

//#include "httpuniserv.h"
#include "main.h"
#include "stats.h"
#include "lempelziv.h"
#include "datapack.h"

#include "csserv.h"
#include "cstocsconn.h"
#include "masterconn.h"
#include "hddspacemgr.h"
#include "replicator.h"

#define LENG 950
#define DATA 100
#define XPOS 43
#define YPOS 6
#define XSIZE (LENG+50)
#define YSIZE (DATA+20)

//#define LONGRATIO 6

#define COMPLENG 50000

#define SHORTRANGE 0
#define MEDIUMRANGE 1
#define LONGRANGE 2
#define VERYLONGRANGE 3


#define RANGES 4

#define STATS_UCPU 0
#define STATS_SCPU 1
#define STATS_MASTERIN 2
#define STATS_MASTEROUT 3
#define STATS_CSCONNIN 4
#define STATS_CSCONNOUT 5
#define STATS_CSSERVIN 6
#define STATS_CSSERVOUT 7
#define STATS_BYTESR 8
#define STATS_BYTESW 9
#define STATS_LLOPR 10
#define STATS_LLOPW 11
#define STATS_DATABYTESR 12
#define STATS_DATABYTESW 13
#define STATS_DATALLOPR 14
#define STATS_DATALLOPW 15
#define STATS_HLOPR 16
#define STATS_HLOPW 17
#define STATS_RTIME 18
#define STATS_WTIME 19
#define STATS_REPL 20

#define SERIES 21

#define CSMIN 100
#define CSMAX 106

#define EXT_CPU 100
#define NET_DATA_IN 101
#define NET_DATA_OUT 102
#define HDD_BYTESR 103
#define HDD_BYTESW 104
#define HDD_OPR 105
#define HDD_OPW 106
//#define EXT_CONNS 103
//#define CALC_DELAY 104
//#define CALC_GOODPERCENT 105

#ifdef INTERPOLATION
#define INTERPOLATION_SIZE 12
//static unsigned int wg[INTERPOLATION_SIZE*2+1]={1,16,120,560,1820,4368,8008,11440,12870,11440,8008,4368,1820,560,120,16,1};
static uint32_t wg[INTERPOLATION_SIZE*2+1]={1,24,276,2024,10626,42504,134596,346104,735471,1307504,1961256,2496144,2704156,2496144,1961256,1307504,735471,346104,134596,42504,10626,2024,276,24,1};
#endif

static struct itimerval it_set;

static uint64_t series[RANGES][SERIES][LENG];
static uint32_t pointers[RANGES];
static uint32_t timepoint[RANGES];

//chart times (for subscripts)
static uint32_t shhour,shmin;
static uint32_t medhour,medmin;
static uint32_t lnghalfhour,lngmday,lngmonth,lngyear;
static uint32_t vlngmday,vlngmonth,vlngyear;

static uint8_t chart[(XSIZE)*(YSIZE)];
static uint8_t compressbuff[COMPLENG];
static uint32_t compsize;

#define COLOR_BKG 0
#define COLOR_AXIS 7
#define COLOR_TEXT 4
#define COLOR_AUX 2
#define COLOR_DATA 1
#define COLOR_EXTDATA 3
#define COLOR_MIDDATA 5
#define COLOR_TRANSPARENT 6

//56 bytes
static uint8_t gifheader[]={'G','I','F','8','9','a',	//  0: header
						0,0,0,0,					//  6: xsize,ysize
						0xc2,						// 10: global control byte
						0,0,						// 11: background index
						0xff,0xff,0xff,				// 13: color map 0
						0x00,0xff,0x00,				// 16: color map 1
						0x00,0x00,0x7f,				// 19: color map 2
						0x00,0x96,0x00,				// 22: color map 3
						0x5f,0x20,0x00,				// 25: color map 4
						0x00,0x00,0x00,				// 28: color map 5
						0x00,0x00,0x00,				// 31: color map 6
						0x00,0x00,0x00,				// 34: color map 7
						0x21,0xf9,0x04,				// 37: graphics extension header (21f9 - magic, 04 - size)
						0x01,						// 40: flags - transparency enabled
						0x00,0x00,					// 41: delay
						0x06,						// 43: transparency index
						0x00,						// 44: separator
						0x2c,						// 45: image header
						0,0,0,0,					// 46: image position
						0,0,0,0,					// 50: image size (xsize,ysize)
						0x00,						// 54: control byte
						3};							// 55: bits per pixel

static uint8_t font[18][7]={
	{0x0E,0x11,0x11,0x11,0x11,0x11,0x0E},
	{0x04,0x0C,0x14,0x04,0x04,0x04,0x1F},
	{0x0E,0x11,0x01,0x02,0x04,0x08,0x1F},
	{0x1F,0x02,0x04,0x0E,0x01,0x11,0x0E},
	{0x02,0x06,0x0A,0x12,0x1F,0x02,0x02},
	{0x1F,0x10,0x1E,0x01,0x01,0x11,0x0E},
	{0x06,0x08,0x10,0x1E,0x11,0x11,0x0E},
	{0x1F,0x01,0x02,0x02,0x04,0x04,0x04},
	{0x0E,0x11,0x11,0x0E,0x11,0x11,0x0E},
	{0x0E,0x11,0x11,0x0F,0x01,0x02,0x0C},
	{0x00,0x00,0x00,0x00,0x00,0x04,0x04},
	{0x00,0x00,0x04,0x00,0x04,0x00,0x00},
	{0x08,0x08,0x09,0x0A,0x0C,0x0A,0x09},
	{0x11,0x1B,0x15,0x11,0x11,0x11,0x11},
	{0x0E,0x11,0x10,0x13,0x11,0x11,0x0E},
	{0x00,0x00,0x1E,0x15,0x15,0x15,0x15},
	{0x00,0x00,0x11,0x11,0x11,0x11,0x1E},
	{0x00,0x00,0x00,0x00,0x00,0x00,0x00}
};

#define FDOT 10
#define COLON 11
#define KILO 12
#define MEGA 13
#define GIGA 14
#define MILI 15
#define MICRO 16
#define SPACE 17

uint32_t getmonleng(uint32_t year,uint32_t month) {
	switch (month) {
	case 1:
	case 3:
	case 5:
	case 7:
	case 8:
	case 10:
	case 12:
		return 31;
	case 4:
	case 6:
	case 9:
	case 11:
		return 30;
	case 2:
		if (year%4) return 28;
		if (year%100) return 29;
		if (year%400) return 29;
		return 28;
	}
	return 0;
}

void stats_store (void) {
	int fd;
	uint32_t hdr[3]={RANGES,SERIES,LENG};
	fd = open("csstats.mfs",O_WRONLY | O_TRUNC | O_CREAT,0666);
	if (fd<0) return;
	write(fd,(char*)&hdr,sizeof(uint32_t)*3);
	write(fd,(char*)&shhour,sizeof(uint32_t));
	write(fd,(char*)&shmin,sizeof(uint32_t));
	write(fd,(char*)&medhour,sizeof(uint32_t));
	write(fd,(char*)&medmin,sizeof(uint32_t));
	write(fd,(char*)&lnghalfhour,sizeof(uint32_t));
	write(fd,(char*)&lngmday,sizeof(uint32_t));
	write(fd,(char*)&lngmonth,sizeof(uint32_t));
	write(fd,(char*)&lngyear,sizeof(uint32_t));
	write(fd,(char*)&vlngmday,sizeof(uint32_t));
	write(fd,(char*)&vlngmonth,sizeof(uint32_t));
	write(fd,(char*)&vlngyear,sizeof(uint32_t));
	write(fd,(char*)pointers,sizeof(uint32_t)*RANGES);
	write(fd,(char*)timepoint,sizeof(uint32_t)*RANGES);
	write(fd,(char*)series,sizeof(uint64_t)*RANGES*SERIES*LENG);
	close(fd);
}

void stats_load() {
	int fd;
	uint32_t hdr[3];
	fd = open("csstats.mfs",O_RDONLY);
	if (fd<0) return;
	read(fd,(char*)&hdr,sizeof(uint32_t)*3);
	if (hdr[0]!=RANGES || hdr[1]!=SERIES || hdr[2]!=LENG) {
		if (hdr[0]==RANGES && hdr[2]==LENG) {
			uint32_t i,j,l;
			uint64_t ld[LENG];
			syslog(LOG_NOTICE,"import charts data from old format");
			read(fd,(char*)&shhour,sizeof(uint32_t));
			read(fd,(char*)&shmin,sizeof(uint32_t));
			read(fd,(char*)&medhour,sizeof(uint32_t));
			read(fd,(char*)&medmin,sizeof(uint32_t));
			read(fd,(char*)&lnghalfhour,sizeof(uint32_t));
			read(fd,(char*)&lngmday,sizeof(uint32_t));
			read(fd,(char*)&lngmonth,sizeof(uint32_t));
			read(fd,(char*)&lngyear,sizeof(uint32_t));
			read(fd,(char*)&vlngmday,sizeof(uint32_t));
			read(fd,(char*)&vlngmonth,sizeof(uint32_t));
			read(fd,(char*)&vlngyear,sizeof(uint32_t));
			read(fd,(char*)pointers,sizeof(uint32_t)*RANGES);
			read(fd,(char*)timepoint,sizeof(uint32_t)*RANGES);
			for (i=0 ; i<RANGES ; i++) {
				l=0;
				for (j=0 ; j<hdr[1] ; j++) {
					read(fd,(char*)&ld,sizeof(uint64_t)*LENG);
					if (l<SERIES) {
						memcpy((char*)series[i][l++],(char*)ld,sizeof(uint64_t)*LENG);
					}
				}
			}
		}
		close(fd);
		return;
	}
	read(fd,(char*)&shhour,sizeof(uint32_t));
	read(fd,(char*)&shmin,sizeof(uint32_t));
	read(fd,(char*)&medhour,sizeof(uint32_t));
	read(fd,(char*)&medmin,sizeof(uint32_t));
	read(fd,(char*)&lnghalfhour,sizeof(uint32_t));
	read(fd,(char*)&lngmday,sizeof(uint32_t));
	read(fd,(char*)&lngmonth,sizeof(uint32_t));
	read(fd,(char*)&lngyear,sizeof(uint32_t));
	read(fd,(char*)&vlngmday,sizeof(uint32_t));
	read(fd,(char*)&vlngmonth,sizeof(uint32_t));
	read(fd,(char*)&vlngyear,sizeof(uint32_t));
	read(fd,(char*)pointers,sizeof(uint32_t)*RANGES);
	read(fd,(char*)timepoint,sizeof(uint32_t)*RANGES);
	read(fd,(char*)series,sizeof(uint64_t)*RANGES*SERIES*LENG);
	close(fd);
	return;
}

uint64_t stats_get (uint32_t type,uint32_t numb) {
	uint64_t result=0;
	uint64_t *tab;
	uint32_t i,ptr;
	if (type>=SERIES) return result;
	if (numb==0 || numb>LENG) return result;

	tab = series[SHORTRANGE][type];
	ptr = pointers[SHORTRANGE];

	for (i=0 ; i<numb ; i++) {
		result += tab[(LENG+ptr-i)%LENG];
	}
	return result;
}

void stats_add (uint64_t *data) {
	uint32_t i,j;
	struct tm *ts;
	time_t now = main_time();
	int32_t local;

	int32_t nowtime,delta;

	now--;

	ts = localtime(&now);
#ifdef HAVE_STRUCT_TM_TM_GMTOFF
	local = now+ts->tm_gmtoff;
#else
	local = now;
#endif

// short range chart - every 1 min

	nowtime = local / 60;

	delta = nowtime - timepoint[SHORTRANGE];

	if (delta>0) {
		if (delta>LENG) delta=LENG;
		while (delta>0) {
			pointers[SHORTRANGE]++;
			pointers[SHORTRANGE]%=LENG;
			for (i=0 ; i<SERIES ; i++) {
				series[SHORTRANGE][i][pointers[SHORTRANGE]] = 0;
			}
			delta--;
		}
		timepoint[SHORTRANGE] = nowtime;
		shmin = ts->tm_min;
		shhour = ts->tm_hour;
	}
	if (delta<=0 && delta>-LENG) {
		i = (pointers[SHORTRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<SERIES ; j++) {
			series[SHORTRANGE][j][i] += data[j];
		}
	}

// medium range chart - every 6 min

	nowtime = local / (60 * 6);

	delta = nowtime - timepoint[MEDIUMRANGE];

	if (delta>0) {
		if (delta>LENG) delta=LENG;
		while (delta>0) {
			pointers[MEDIUMRANGE]++;
			pointers[MEDIUMRANGE]%=LENG;
			for (i=0 ; i<SERIES ; i++) {
//				series[MEDIUMRANGE][i][mediumpointer] = mediumcounters[i] / 6;
//				mediumcounters[i]=0;
				series[MEDIUMRANGE][i][pointers[MEDIUMRANGE]] = 0;
			}
			delta--;
		}
		timepoint[MEDIUMRANGE] = nowtime;
		medmin = ts->tm_min;
		medhour = ts->tm_hour;
	}
	if (delta<=0 && delta>-LENG) {
		i = (pointers[MEDIUMRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<SERIES ; j++) {
			series[MEDIUMRANGE][j][i] += data[j];
		}
	}


// long range chart - every 30 min

	nowtime = local / (60 * 30);

	delta = nowtime - timepoint[LONGRANGE];

	if (delta>0) {
		if (delta>LENG) delta=LENG;
		while (delta>0) {
			pointers[LONGRANGE]++;
			pointers[LONGRANGE]%=LENG;
			for (i=0 ; i<SERIES ; i++) {
				series[LONGRANGE][i][pointers[LONGRANGE]] = 0;
			}
			delta--;
		}
		timepoint[LONGRANGE] = nowtime;
		lnghalfhour = ts->tm_hour*2;
		if (ts->tm_min>=30) {
			lnghalfhour++;
		}
		lngmday = ts->tm_mday;
		lngmonth = ts->tm_mon + 1;
		lngyear = ts->tm_year + 1900;
	}
	if (delta<=0 && delta>-LENG) {
		i = (pointers[LONGRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<SERIES ; j++) {
			series[LONGRANGE][j][i] += data[j];
		}
	}
// long range chart - every 1 day

	nowtime = local / (60 * 60 * 24);

	delta = nowtime - timepoint[VERYLONGRANGE];

	if (delta>0) {
		if (delta>LENG) delta=LENG;
		while (delta>0) {
			pointers[VERYLONGRANGE]++;
			pointers[VERYLONGRANGE]%=LENG;
			for (i=0 ; i<SERIES ; i++) {
				series[VERYLONGRANGE][i][pointers[VERYLONGRANGE]] = 0;
			}
			delta--;
		}
		timepoint[VERYLONGRANGE] = nowtime;
		vlngmday = ts->tm_mday;
		vlngmonth = ts->tm_mon + 1;
		vlngyear = ts->tm_year + 1900;
	}
	if (delta<=0 && delta>-LENG) {
		i = (pointers[VERYLONGRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<SERIES ; j++) {
			series[VERYLONGRANGE][j][i] += data[j];
		}
	}
}

void stats_refresh(void) {
	uint64_t data[SERIES];
	uint32_t i,bin,bout,opr,opw,dbr,dbw,dopr,dopw,repl;
	struct itimerval uc,pc;
	uint32_t ucusec,pcusec;

	for (i=0 ; i<SERIES ; i++) {
		data[i]=0;
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
	data[STATS_UCPU] = ucusec;
	data[STATS_SCPU] = pcusec;

	masterconn_stats(&bin,&bout);
	data[STATS_MASTERIN]=bin;
	data[STATS_MASTEROUT]=bout;
	cstocsconn_stats(&bin,&bout);
	data[STATS_CSCONNIN]=bin;
	data[STATS_CSCONNOUT]=bout;
	csserv_stats(&bin,&bout,&opr,&opw);
	data[STATS_CSSERVIN]=bin;
	data[STATS_CSSERVOUT]=bout;
	data[STATS_HLOPR]=opr;
	data[STATS_HLOPW]=opw;
	hdd_stats(&bin,&bout,&opr,&opw,&dbr,&dbw,&dopr,&dopw,data+STATS_RTIME,data+STATS_WTIME);
	data[STATS_BYTESR]=bin;
	data[STATS_BYTESW]=bout;
	data[STATS_LLOPR]=opr;
	data[STATS_LLOPW]=opw;
	data[STATS_DATABYTESR]=dbr;
	data[STATS_DATABYTESW]=dbw;
	data[STATS_DATALLOPR]=dopr;
	data[STATS_DATALLOPW]=dopw;
	replicator_stats(&repl);
	data[STATS_REPL]=repl;

	stats_add(data);
}

void stats_term(void) {
	stats_refresh();
	stats_store();
}

int stats_init (void) {
	uint32_t i;
	uint64_t data[SERIES];
	struct itimerval uc,pc;

	for (i=0 ; i<SERIES ; i++) {
		data[i]=0;
	}
	memset((char*)series,0,sizeof(uint64_t)*RANGES*SERIES*LENG);

	for (i=0 ; i<RANGES ; i++) {
		pointers[i]=0;
		timepoint[i]=0;
	}

	stats_load();
	stats_add(data);

	gifheader[6] = (XSIZE)%256;
	gifheader[7] = (XSIZE)/256;
	gifheader[8] = (YSIZE)%256;
	gifheader[9] = (YSIZE)/256;
	gifheader[50] = (XSIZE)%256;
	gifheader[51] = (XSIZE)/256;
	gifheader[52] = (YSIZE)%256;
	gifheader[53] = (YSIZE)/256;

	it_set.it_interval.tv_sec = 0;
	it_set.it_interval.tv_usec = 0;
	it_set.it_value.tv_sec = 999;
	it_set.it_value.tv_usec = 999999;
	setitimer(ITIMER_VIRTUAL,&it_set,&uc);             // user time
	setitimer(ITIMER_PROF,&it_set,&pc);                // user time + system time

	main_destructregister(stats_term);
	main_timeregister(TIMEMODE_RUNONCE,60,0,stats_refresh);
	main_timeregister(TIMEMODE_RUNONCE,3600,0,stats_store);
	return 0;
}

void stats_puttext(int32_t posx,int32_t posy,uint8_t color,uint8_t *data,uint32_t leng,int32_t minx,int32_t maxx,int32_t miny,int32_t maxy) {
	uint32_t i,fx,fy;
	uint8_t fp,fbits;
	int32_t px,x,y;
	for (i=0 ; i<leng ; i++) {
		px = i*6+posx;
		fp = data[i]&0x1F;
		if (fp>SPACE) fp=SPACE;
		for (fy=0 ; fy<7 ; fy++) {
			fbits = font[fp][fy];
			for (fx=0 ; fx<5 ; fx++) {
				x = px+fx;
				y = posy+fy;
				if (fbits&0x10 && x>=minx && x<=maxx && y>=miny && y<=maxy) {
					chart[(XSIZE)*y+x] = color;
				}
				fbits<<=1;
			}
		}
	}
}

void stats_makechart(uint32_t type,uint32_t range) {
	static const uint8_t jtab[]={MICRO,MILI,SPACE,KILO,MEGA,GIGA};
	int32_t i,j,k,l;
	uint32_t xy,xm,xd,xh,xs,xoff,xbold,ys;
	uint32_t nexp;
	uint64_t ymax,ymin;
	uint64_t max,min;
	uint64_t d,ed;
	uint64_t *tab;
	uint64_t dispdata[LENG];
#ifdef INTERPOLATION
	uint64_t md;
	uint64_t middata[LENG];
	uint32_t cmiddata[LENG];
#endif
	uint32_t scale=0;
	uint32_t pointer;
	uint8_t text[6];
	memset(chart,COLOR_TRANSPARENT,(XSIZE)*(YSIZE));
	if (type<SERIES) {
		tab = series[range][type];
	} else {
		switch (type) {
		case NET_DATA_IN:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_CSCONNIN][i] + series[range][STATS_CSSERVIN][i];
			}
			break;
		case NET_DATA_OUT:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_CSCONNOUT][i] + series[range][STATS_CSSERVOUT][i];
			}
			break;
		case HDD_BYTESR:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_BYTESR][i] + series[range][STATS_DATABYTESR][i];
			}
			break;
		case HDD_BYTESW:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_BYTESW][i] + series[range][STATS_DATABYTESW][i];
			}
			break;
		case HDD_OPR:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_LLOPR][i] + series[range][STATS_DATALLOPR][i];
			}
			break;
		case HDD_OPW:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_LLOPW][i] + series[range][STATS_DATALLOPW][i];
			}
			break;
		case EXT_CPU:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_UCPU][i] + series[range][STATS_SCPU][i];
			}
			break;
/*
		case EST_BW_IN:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_HTTPIN][i] + series[range][STATS_CONNS][i] * CONN_IN_BYTES;
			}
			break;
		case EST_BW_OUT:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_HTTPOUT][i] + series[range][STATS_CONNS][i] * CONN_OUT_BYTES;
			}
			break;
		case EXT_CONNS:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = series[range][STATS_ALLCONNS][i];
			}
			break;
		case CALC_DELAY:
			for (i=0 ; i<LENG ; i++) {
				if (series[range][STATS_CONNS][i]>0) {
					dispdata[i] = series[range][STATS_CONNSUTIME][i] / series[range][STATS_CONNS][i];
				} else {
					dispdata[i] = 0;
				}
			}
			break;
		case CALC_GOODPERCENT:
			for (i=0 ; i<LENG ; i++) {
				if (series[range][STATS_ALLCONNS][i]>0) {
					dispdata[i] = (((uint64_t)1000000)*series[range][STATS_CONNS][i])/series[range][STATS_ALLCONNS][i];
				} else {
					dispdata[i] = 0;
				}
			}
			break;
*/
		default:
			for (i=0 ; i<LENG ; i++) {
				dispdata[i] = 0;
			}
		}
		tab = dispdata;
	}
	pointer = pointers[range];

#ifdef INTERPOLATION
	for (i=0 ; i<LENG ; i++) {
		l = 0;
		md = 0;
		for (j=0 ; j<INTERPOLATION_SIZE*2+1 ; j++) {
			k = (LENG+1+pointer+i+j-INTERPOLATION_SIZE)%LENG;
			if ((i+j-INTERPOLATION_SIZE)>=0 && (i+j-INTERPOLATION_SIZE)<LENG) {
				md+=tab[k]*wg[j];
				l+=wg[j];
			}
		}
		middata[i] = md/l;
	}
#endif

	max = min = tab[0];
//	max = tab[0];	// min scale : 1
//	min = tab[0];
	for (i=0 ; i<LENG ; i++) {
		d = tab[i];
		if (d>max) max=d;
		if ((uint32_t)i!=pointer && d<min) min=d;
	}
//	if (type!=CALC_DELAY && type!=CALC_GOODPERCENT) {
//	if (type==EXT_CPU) {
//		min=0;
//	}
//	if (type!=6) min=0;
	if (max-min < 1) {
		max=min+1;
	}

#ifdef INTERPOLATION
	for (i=0 ; i<LENG ; i++) {
		md = middata[i];
		if (md<min) {
			md=DATA;
		} else {
			md -= min;
			md *= DATA;
			md /= (max-min);
			md = DATA-md;
		}
//		if (j<0) {
//			j = 0;
//		}
//		if (j>=DATA) {
//			j = DATA-1;
//		}
		cmiddata[i] = md;
	}
#endif

//	m = 0;
	for (i=0 ; i<LENG ; i++) {
		j = (LENG+1+pointer+i)%LENG;
		if (type==EXT_CPU) {
			ed = series[range][STATS_SCPU][j];
		} else if (type==NET_DATA_IN) {
			ed = series[range][STATS_CSCONNIN][j];
		} else if (type==NET_DATA_OUT) {
			ed = series[range][STATS_CSCONNOUT][j];
		} else if (type==HDD_BYTESR) {
			ed = series[range][STATS_DATABYTESR][j];
		} else if (type==HDD_BYTESW) {
			ed = series[range][STATS_DATABYTESW][j];
		} else if (type==HDD_OPR) {
			ed = series[range][STATS_DATALLOPR][j];
		} else if (type==HDD_OPW) {
			ed = series[range][STATS_DATALLOPW][j];
/*
		} else if (type==EST_BW_IN) {
			ed = series[range][STATS_HTTPIN][j];
		} else if (type==EST_BW_OUT) {
			ed = series[range][STATS_HTTPOUT][j];
		} else if (type==EXT_CONNS) {
			ed = series[range][STATS_CONNS][j];
*/
		} else {
			ed = min;
		}
		d = tab[j];
		if (d<min) {
			d=0;
		} else {
			d -= min;
			d *= DATA;
			d /= (max-min);
		}
		if (ed<min) {
			ed=0;
		} else {
			ed -= min;
			ed *= DATA;
			ed /= (max-min);
		}
#ifdef INTERPOLATION
		if (i==0) {
			k = cmiddata[i];
		} else {
			k = (cmiddata[i-1]+cmiddata[i])/2;
		}
		if (i==LENG-1) {
			l = cmiddata[i];
		} else {
			l = (cmiddata[i+1]+cmiddata[i])/2;
		}
		if (k>l) {
			int32_t m;
			m = k;
			k = l;
			l = m;
		} else if (k==l) {
			l = k+1;
		}
#endif
		for (j=0 ; j<DATA ; j++) {
			if (DATA<d+j) {
				if (DATA<ed+j) {
					chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_EXTDATA;
				} else {
					chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA;
				}
			} else {
				chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_BKG;
			}
#ifdef INTERPOLATION
			if (j>=k && j<l) {
//				if (m) {
				chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_MIDDATA;
//				}
//				m = 1-m;
			}
#endif
		}
//		for (j=3 ; j<YPOS ; j++) {
//			chart[(XSIZE)*j+(i+XPOS)] = COLOR_BKG;
//		}
	}
	// osie
	for (i=-3 ; i<LENG+3 ; i++) {
		chart[(XSIZE)*(DATA+YPOS)+(i+XPOS)] = COLOR_AXIS;
	}
	for (i=-2 ; i<DATA+5 ; i++) {
		chart[(XSIZE)*(DATA-i+YPOS)+(XPOS-1)] = COLOR_AXIS;
		chart[(XSIZE)*(DATA-i+YPOS)+(XPOS+LENG)] = COLOR_AXIS;
	}
	// podzialka x
	xy = xm = xd = xh = xs = 0;
	if (range<3) {
		if (range==2) {
			xs = 12;
			xoff = lnghalfhour%12;
			xbold = 4;
			xh = lnghalfhour/12;
			xd = lngmday;
			xm = lngmonth;
			xy = lngyear;
		} else if (range==1) {
			xs = 10;
			xoff = medmin/6;
			xbold = 6;
			xh = medhour;
		} else {
			xs = 60;
			xoff = shmin;
			xbold = 1;
			xh = shhour;
		}
		k = LENG;
		for (i=LENG-xoff-1 ; i>=0 ; i-=xs) {
			if (xh%xbold==0) {
				ys=2;
				if ((range==0 && xh%6==0) || (range==1 && xh==0) || (range==2 && xd==1)) {
					ys=1;
				}
				if (range<2) {
					text[0]=xh/10;
					text[1]=xh%10;
					text[2]=COLON;
					text[3]=0;
					text[4]=0;
					stats_puttext(XPOS+i-14,(YPOS+DATA)+4,COLOR_TEXT,text,5,XPOS-1,XPOS+LENG,0,YSIZE-1);
				} else {
					text[0]=xm/10;
					text[1]=xm%10;
					text[2]=FDOT;
					text[3]=xd/10;
					text[4]=xd%10;
					stats_puttext(XPOS+i+10,(YPOS+DATA)+4,COLOR_TEXT,text,5,XPOS-1,XPOS+LENG,0,YSIZE-1);
					xd--;
					if (xd==0) {
						xm--;
						if (xm==0) {
							xm=12;
							xy--;
						}
						xd = getmonleng(xy,xm);
					}
				}
				chart[(XSIZE)*(YPOS+DATA+1)+(i+XPOS)] = COLOR_AXIS;
				chart[(XSIZE)*(YPOS+DATA+2)+(i+XPOS)] = COLOR_AXIS;
			} else {
				ys=4;
			}
			for (j=0 ; j<DATA ; j+=ys) {
				if (ys>1 || (j%4)!=0) {
					chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_AUX;
				}
			}
			if (range<2) {
				if (xh==0) {
					xh=23;
				} else {
					xh--;
				}
			} else {
				if (xh==0) {
					xh=3;
				} else {
					xh--;
				}
			}
		}
		if (range==2) {
			i -= xs*xh;
			text[0]=xm/10;
			text[1]=xm%10;
			text[2]=FDOT;
			text[3]=xd/10;
			text[4]=xd%10;
			stats_puttext(XPOS+i+10,(YPOS+DATA)+4,COLOR_TEXT,text,5,XPOS-1,XPOS+LENG,0,YSIZE-1);
		}
	} else {
		xy = lngyear;
		xm = lngmonth;
		k = LENG;
		for (i=LENG-lngmday ; i>=0 ; ) {
			text[0]=xm/10;
			text[1]=xm%10;
			stats_puttext(XPOS+i+(getmonleng(xy,xm)-11)/2+1,(YPOS+DATA)+4,COLOR_TEXT,text,2,XPOS-1,XPOS+LENG,0,YSIZE-1);
			chart[(XSIZE)*(YPOS+DATA+1)+(i+XPOS)] = COLOR_AXIS;
			chart[(XSIZE)*(YPOS+DATA+2)+(i+XPOS)] = COLOR_AXIS;
			if (xm!=1) {
				for (j=0 ; j<DATA ; j+=2) {
					chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_AUX;
				}
			} else {
				for (j=0 ; j<DATA ; j++) {
					if ((j%4)!=0) {
						chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_AUX;
					}
				}
			}
			xm--;
			if (xm==0) {
				xm=12;
				xy--;
			}
			i-=getmonleng(xy,xm);
			k = i;
		}
		text[0]=xm/10;
		text[1]=xm%10;
		stats_puttext(XPOS+i+(getmonleng(xy,xm)-11)/2+1,(YPOS+DATA)+4,COLOR_TEXT,text,2,XPOS-1,XPOS+LENG,0,YSIZE-1);
	}
	// podzialka y

	// range scale
//	if (type==CALC_DELAY || type==CALC_GOODPERCENT) {
//		ymax = max;
//		ymin = min;
//	} else {
		switch (range) {
		case SHORTRANGE:
			ymax = max;
			ymin = min;
			break;
		case MEDIUMRANGE:
			ymax = max/6;
			ymin = min/6;
			break;
		case LONGRANGE:
			ymax = max/30;
			ymin = min/30;
			break;
		case VERYLONGRANGE:
			ymax = max/(24*60);
			ymin = min/(24*60);
			break;
		default:
			ymax=0;
			ymin=0;
		}
//	}

	// chart scale
	switch (type) {
	case STATS_MASTERIN:
	case STATS_MASTEROUT:
	case STATS_CSSERVIN:
	case STATS_CSSERVOUT:
	case STATS_CSCONNIN:
	case STATS_CSCONNOUT:
	case NET_DATA_IN:
	case NET_DATA_OUT:
		scale = 1;		// micro
		ymax *= 8000;		// bytes -> bits (*1000 - scale = micro)
		ymax /= 60;		// per min -> per sec
		ymin *= 8000;		// bytes -> bits
		ymin /= 60;		// per min -> per sec
		break;
	case STATS_BYTESR:
	case STATS_BYTESW:
	case STATS_DATABYTESR:
	case STATS_DATABYTESW:
	case HDD_BYTESR:
	case HDD_BYTESW:
		scale = 1;		// micro
		ymax *= 1000;		// scale micro
		ymax /= 60;		// per min -> per sec
		ymin *= 1000;		// scale micro
		ymin /= 60;		// per min -> per sec
		break;
	case STATS_LLOPR:
	case STATS_LLOPW:
	case STATS_DATALLOPR:
	case STATS_DATALLOPW:
	case STATS_HLOPR:
	case STATS_HLOPW:
	case STATS_REPL:
	case HDD_OPR:
	case HDD_OPW:
		scale = 2;		// number of operations per minute
		break;
//	case STATS_CONNS:
//	case STATS_ALLCONNS:
//	case EXT_CONNS:
//		scale = 2;		// normal
//		ymax /= 60;		// per min -> per sec
//		ymin /= 60;		// per min -> per sec
//		break;
	case STATS_RTIME:
	case STATS_WTIME:
		scale = 0;		// micro
		ymax /= 60;
		ymin /= 60;
		break;
//	case CALC_GOODPERCENT:
	case STATS_UCPU:
	case STATS_SCPU:
	case EXT_CPU:
		scale = 0;		// micro
		ymax *= 100;	// -> per cent
		ymax /= 60;	// min -> sec
		ymin *= 100;	// -> per cent
		ymin /= 60;	// min -> sec
	}

//	for (i=0 ; i<LENG ; i+=2) {
//		for (j=DATA-20 ; j>=0 ; j-=20) {
//			chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = 2;
//		}
//	}
	// opis y
	if (ymax>=1000000000) {
		nexp=3;
	} else if (ymax>=1000000) {
		nexp=2;
	} else if (ymax>=1000) {
		nexp=1;
	} else {
		nexp=0;
	}
	for (i=0 ; i<=5 ; i++) {
		d = (10*(ymax-ymin)*i)/5 + (ymin*10);
		switch(nexp) {
		case 3:
			d/=1000000000;
			break;
		case 2:
			d/=1000000;
			break;
		case 1:
			d/=1000;
			break;
		}
		l=0;
		if (d>=1000) {
			text[l++]=d/1000;
			d%=1000;
			text[l++]=d/100;
			d%=100;
		} else if (d>=100) {
			text[l++]=d/100;
			d%=100;
		}
		text[l++]=d/10;
		d%=10;
		text[l++]=FDOT;
		text[l++]=d;
		if (jtab[nexp+scale]!=SPACE) {
			text[l++]=jtab[nexp+scale];
		}
		stats_puttext(XPOS - 4 - (l*6),(YPOS+DATA-(20*i))-3,COLOR_TEXT,text,l,0,XSIZE-1,0,YSIZE-1);
		chart[(XSIZE)*(YPOS+DATA-20*i)+(XPOS-2)] = COLOR_AXIS;
		chart[(XSIZE)*(YPOS+DATA-20*i)+(XPOS-3)] = COLOR_AXIS;
		if (i>0) {
			for (j=1 ; j<LENG ; j+=2) {
				chart[(XSIZE)*(YPOS+DATA-20*i)+(XPOS+j)] = COLOR_AUX;
			}
		}
	}
}

// compsize - common for makechart and makegif

uint32_t stats_datasize(uint32_t number) {
	uint32_t chtype,chrange;

	chtype = number / 10;
	chrange = number % 10;
	if (chrange>=RANGES || chtype>=SERIES) {
		return 0;
	}
	return LENG*8+4;
}

void stats_makedata(uint8_t *buff,uint32_t number) {
	uint32_t i,j,ts,pointer,chtype,chrange;
	uint64_t *tab;

	chtype = number / 10;
	chrange = number % 10;
	if (chrange>=RANGES || chtype>=SERIES) {
		return;
	}
	tab = series[chrange][chtype];
	pointer = pointers[chrange];
	ts = timepoint[chrange];
	switch (chrange) {
		case SHORTRANGE:
			ts *= 60;
			break;
		case MEDIUMRANGE:
			ts *= 60*6;
			break;
		case LONGRANGE:
			ts *= 60*30;
			break;
		case VERYLONGRANGE:
			ts *= 60*60*24;
			break;
	}
	PUT32BIT(ts,buff);
	for (i=0 ; i<LENG ; i++) {
		j = (LENG+1+pointer+i)%LENG;
		PUT64BIT(tab[j],buff);
	}
}

uint32_t stats_gifsize(uint32_t number) {
	uint32_t size;
	uint32_t chtype,chrange;

	chtype = number / 10;
	chrange = number % 10;
	if (chrange>=RANGES) {
		return 0;
	}
	if (!(chtype<SERIES || (chtype>=CSMIN && chtype<=CSMAX))) {
		return 0;
	}

	stats_makechart(chtype,chrange);

	compsize = COMPLENG;
	if (lzw_encode(3,chart,(XSIZE)*(YSIZE),compressbuff,&compsize)==0) {
		return 0;
	}
	if ((compsize%254)>0) {
		size = 56 + 255*(compsize/254)+((compsize%254)+1) + 2;
	} else {
		size = 56 + 255*(compsize/254) + 2;
	}
	return size;
}

void stats_makegif(uint8_t *buff) {
	uint8_t *cbuff = compressbuff;
	memcpy(buff,gifheader,56);
	buff+=56;
	while (compsize>254) {
		*buff++ = 254;
		memcpy(buff,cbuff,254);
		buff+=254;
		cbuff+=254;
		compsize-=254;
	}
	if (compsize>0) {
		*buff++ = compsize;
		memcpy(buff,cbuff,compsize);
		buff+=compsize;
	}
	*buff++ = 0;
	*buff++ = ';';
}

/*
uint32_t stats_makegif(uint32_t number,uint8_t *buff,uint32_t maxleng) {
	uint32_t size;
	uint32_t compsize;
	uint8_t *cbuff = compressbuff;
	uint32_t chtype,chrange;

	chtype = number / 10;
	chrange = number % 10;
	if (chrange>=RANGES) {
		return 0;
	}
	if (!(chtype<SERIES || (chtype>=CSMIN && chtype<=CSMAX))) {
		return 0;
	}

	stats_makechart(chtype,chrange);

	compsize = COMPLENG;
	if (lzw_encode(3,chart,(XSIZE)*(YSIZE),cbuff,&compsize)==0) {
		return 0;
	}
	if (compsize%254>0) {
		size = 56 + 255*(compsize/254)+((compsize%254)+1) + 2;
	} else {
		size = 56 + 255*(compsize/254) + 2;
	}
	if (maxleng<size) {
		return 0;
	}
	memcpy(buff,gifheader,56);
	buff+=56;
	while (compsize>254) {
		*buff++ = 254;
		memcpy(buff,cbuff,254);
		buff+=254;
		cbuff+=254;
		compsize-=254;
	}
	if (compsize>0) {
		*buff++ = compsize;
		memcpy(buff,cbuff,compsize);
		buff+=compsize;
//		cbuff+=compsize;
//		compsize-=compsize;
	}
	*buff++ = 0;
	*buff++ = ';';
	return size;
}
*/
