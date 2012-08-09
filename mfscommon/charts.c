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

#ifdef HAVE_CONFIG_H
#include "config.h"
#else
#define HAVE_ZLIB_H 1
#define HAVE_STRUCT_TM_TM_GMTOFF 1
#endif

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <errno.h>
#include <inttypes.h>
#ifdef HAVE_ZLIB_H
#include <zlib.h>
#endif

#include "charts.h"
#include "crc.h"
#include "datapack.h"
#include "massert.h"
#include "slogger.h"

#define USE_NET_ORDER 1

#define LENG 950
#define DATA 100
#define XPOS 43
#define YPOS 6
#define XSIZE (LENG+50)
#define YSIZE (DATA+20)
//#define LONGRATIO 6

#define SHORTRANGE 0
#define MEDIUMRANGE 1
#define LONGRANGE 2
#define VERYLONGRANGE 3

#define RANGES 4

#define CHARTS_DEF_IS_DIRECT(x) ((x)>=CHARTS_DIRECT_START && (x)<CHARTS_DIRECT_START+statdefscount)
#define CHARTS_DIRECT_POS(x) ((x)-CHARTS_DIRECT_START)
#define CHARTS_DEF_IS_CALC(x) ((x)>=CHARTS_CALC_START && (x)<CHARTS_CALC_START+calcdefscount)
#define CHARTS_CALC_POS(x) ((x)-CHARTS_CALC_START)

#define CHARTS_IS_DIRECT_STAT(x) ((x)<statdefscount)
#define CHARTS_EXTENDED_START 100
#define CHARTS_IS_EXTENDED_STAT(x) ((x)>=CHARTS_EXTENDED_START && (x)<CHARTS_EXTENDED_START+estatdefscount)
#define CHARTS_EXTENDED_POS(x) ((x)-CHARTS_EXTENDED_START)

static uint32_t *calcdefs;
static uint32_t **calcstartpos;
static uint32_t calcdefscount;
static statdef *statdefs;
static uint32_t statdefscount;
static estatdef *estatdefs;
static uint32_t estatdefscount;
static char* statsfilename;

typedef uint64_t stat_record[RANGES][LENG];

static stat_record *series;
static uint32_t pointers[RANGES];
static uint32_t timepoint[RANGES];

//chart times (for subscripts)
static uint32_t shhour,shmin;
static uint32_t medhour,medmin;
static uint32_t lnghalfhour,lngmday,lngmonth,lngyear;
static uint32_t vlngmday,vlngmonth,vlngyear;

#define RAWSIZE ((1+(((XSIZE)+1)>>1))*(YSIZE))
#define CBUFFSIZE (((RAWSIZE)*1001)/1000+16)

static uint8_t chart[(XSIZE)*(YSIZE)];
static uint8_t rawchart[RAWSIZE];
static uint8_t compbuff[CBUFFSIZE];
static uint32_t compsize=0;
#ifdef HAVE_ZLIB_H
static z_stream zstr;
#else
static uint8_t warning[50] = {
	0x89,0xCF,0x83,0x8E,0x45,0xE7,0x9F,0x3C,0xF7,0xDE,    /* 10001001 11001111 10000011 10001110 01000101 11100111 10011111 00111100 11110111 11011110 */
	0xCA,0x22,0x04,0x51,0x6D,0x14,0x50,0x41,0x04,0x11,    /* 11001010 00100010 00000100 01010001 01101101 00010100 01010000 01000001 00000100 00010001 */
	0xAA,0x22,0x04,0x11,0x55,0xE7,0x9C,0x38,0xE7,0x11,    /* 10101010 00100010 00000100 00010001 01010101 11100111 10011100 00111000 11100111 00010001 */
	0x9A,0x22,0x04,0x51,0x45,0x04,0x50,0x04,0x14,0x11,    /* 10011010 00100010 00000100 01010001 01000101 00000100 01010000 00000100 00010100 00010001 */
	0x89,0xC2,0x03,0x8E,0x45,0x04,0x5F,0x79,0xE7,0xDE     /* 10001001 11000010 00000011 10001110 01000101 00000100 01011111 01111001 11100111 11011110 */
};
#endif

#define COLOR_TRANSPARENT 0
#define COLOR_BKG 1
#define COLOR_AXIS 2
#define COLOR_AUX 3
#define COLOR_TEXT 4
#define COLOR_DATA1 5
#define COLOR_DATA2 6
#define COLOR_DATA3 7
#define COLOR_NODATA 8

static uint8_t png_header[] = {
	137, 80, 78, 71, 13, 10, 26, 10,        // signature

	0, 0, 0, 13, 'I', 'H', 'D', 'R',        // IHDR chunk
	((XSIZE)>>24)&0xFF, ((XSIZE)>>16)&0xFF, ((XSIZE)>>8)&0xFF, (XSIZE)&0xFF, // width
	((YSIZE)>>24)&0xFF, ((YSIZE)>>16)&0xFF, ((YSIZE)>>8)&0xFF, (YSIZE)&0xFF, // height
	4, 3, 0, 0, 0,                          // 4bits, indexed color mode, default compression, default filters, no interlace
	'C', 'R', 'C', '#',                     // CRC32 placeholder

	0, 0, 0, 0x1B, 'P', 'L', 'T', 'E',      // PLTE chunk
	0xff,0xff,0xff,                         // color map 0 - background (transparent)
	0xff,0xff,0xff,                         // color map 1 - chart background (white)
	0x00,0x00,0x00,                         // color map 2 - axes (black)
	0x00,0x00,0x7f,                         // color map 3 - auxiliary lines (dark blue)
	0x5f,0x20,0x00,                         // color map 4 - texts (brown)
	0x00,0xff,0x00,                         // color map 5 - data1 (light green)
	0x00,0x96,0x00,                         // color map 6 - data2 (green)
	0x00,0x60,0x00,                         // color map 7 - data3 (dark green)
	0xC0,0xC0,0xC0,                         // color map 8 - nodata (grey)
	'C', 'R', 'C', '#',                     // CRC32 placeholder

	0, 0, 0, 1, 't', 'R', 'N', 'S',         // tRNS chunk
	0,                                      // color 0 transparency - alpha = 0
	'C', 'R', 'C', '#',                     // CRC32 placeholder

	0, 0, 0, 1, 'b', 'K', 'G', 'D',         // bKGD chunk
	0,                                      // color 0 = background
	'C', 'R', 'C', '#',                     // CRC32 placeholder

	0, 0, 0, 0, 'I', 'D', 'A', 'T'          // IDAT chunk
};

static uint8_t png_tailer[] = {
	'C', 'R', 'C', '#',			// CRC32 placeholder
	0, 0, 0, 0, 'I', 'E', 'N', 'D',         // IEND chunk
	'C', 'R', 'C', '#',                     // CRC32 placeholder
};

static uint8_t png_1x1[] = {
	137, 80, 78, 71, 13, 10, 26, 10,        // signature

	0, 0, 0, 13, 'I', 'H', 'D', 'R',        // IHDR chunk
	0, 0, 0, 1,				// width
	0, 0, 0, 1,				// height
	8, 4, 0, 0, 0,                          // 8bits, grayscale with alpha color mode, default compression, default filters, no interlace
	0xb5, 0x1c, 0x0c, 0x02,			// CRC

	0, 0, 0, 11, 'I', 'D', 'A', 'T',	// IDAT chunk
	0x08, 0xd7, 0x63, 0x60, 0x60, 0x00,
	0x00, 0x00, 0x03, 0x00, 0x01,
	0x20, 0xd5, 0x94, 0xc7,			// CRC

	0, 0, 0, 0, 'I', 'E', 'N', 'D',		// IEND chunk
	0xae, 0x42, 0x60, 0x82			// CRC
};

static uint8_t font[25][9]={
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x11,0x11,0x11,0x11,0x0E,0x00,0x00},
	/* 00100 */
	/* 01100 */
	/* 10100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x04,0x0C,0x14,0x04,0x04,0x04,0x1F,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 00001 */
	/* 00010 */
	/* 00100 */
	/* 01000 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x01,0x02,0x04,0x08,0x1F,0x00,0x00},
	/* 11111 */
	/* 00010 */
	/* 00100 */
	/* 01110 */
	/* 00001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x02,0x04,0x0E,0x01,0x11,0x0E,0x00,0x00},
	/* 00010 */
	/* 00110 */
	/* 01010 */
	/* 10010 */
	/* 11111 */
	/* 00010 */
	/* 00010 */
	/* 00000 */
	/* 00000 */
	{0x02,0x06,0x0A,0x12,0x1F,0x02,0x02,0x00,0x00},
	/* 11111 */
	/* 10000 */
	/* 11110 */
	/* 00001 */
	/* 00001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x10,0x1E,0x01,0x01,0x11,0x0E,0x00,0x00},
	/* 00110 */
	/* 01000 */
	/* 10000 */
	/* 11110 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x06,0x08,0x10,0x1E,0x11,0x11,0x0E,0x00,0x00},
	/* 11111 */
	/* 00001 */
	/* 00010 */
	/* 00010 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x01,0x02,0x02,0x04,0x04,0x04,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x11,0x0E,0x11,0x11,0x0E,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 01111 */
	/* 00001 */
	/* 00010 */
	/* 01100 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x11,0x0F,0x01,0x02,0x0C,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x00,0x00,0x00,0x04,0x04,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 00100 */
	/* 00000 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x04,0x00,0x04,0x00,0x00,0x00,0x00},
	/* 01000 */
	/* 01000 */
	/* 01001 */
	/* 01010 */
	/* 01100 */
	/* 01010 */
	/* 01001 */
	/* 00000 */
	/* 00000 */
	{0x08,0x08,0x09,0x0A,0x0C,0x0A,0x09,0x00,0x00},
	/* 10001 */
	/* 11011 */
	/* 10101 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 00000 */
	/* 00000 */
	{0x11,0x1B,0x15,0x11,0x11,0x11,0x11,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 10000 */
	/* 10011 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x10,0x13,0x11,0x11,0x0E,0x00,0x00},
	/* 11111 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x04,0x04,0x04,0x04,0x04,0x04,0x00,0x00},
	/* 11110 */
	/* 10001 */
	/* 10001 */
	/* 11110 */
	/* 10000 */
	/* 10000 */
	/* 10000 */
	/* 00000 */
	/* 00000 */
	{0x1E,0x11,0x11,0x1E,0x10,0x10,0x10,0x00,0x00},
	/* 11111 */
	/* 10000 */
	/* 10000 */
	/* 11100 */
	/* 10000 */
	/* 10000 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x10,0x10,0x1C,0x10,0x10,0x1F,0x00,0x00},
	/* 11111 */
	/* 00001 */
	/* 00010 */
	/* 00100 */
	/* 01000 */
	/* 10000 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x01,0x02,0x04,0x08,0x10,0x1F,0x00,0x00},
	/* 10001 */
	/* 10001 */
	/* 01010 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x11,0x11,0x0A,0x04,0x04,0x04,0x04,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 11110 */
	/* 10101 */
	/* 10101 */
	/* 10101 */
	/* 10101 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x1E,0x15,0x15,0x15,0x15,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 10010 */
	/* 10010 */
	/* 10010 */
	/* 10010 */
	/* 11101 */
	/* 10000 */
	/* 10000 */
	{0x00,0x00,0x12,0x12,0x12,0x12,0x1D,0x10,0x10},
	/* 11001 */
	/* 11010 */
	/* 00010 */
	/* 00100 */
	/* 01000 */
	/* 01011 */
	/* 10011 */
	/* 00000 */
	/* 00000 */
	{0x19,0x1A,0x02,0x04,0x08,0x0B,0x13,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00},
	/* 11111 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x11,0x11,0x11,0x11,0x11,0x1F,0x00,0x00}
};

#define FDOT 10
#define COLON 11
#define KILO 12
#define MEGA 13
#define GIGA 14
#define TERA 15
#define PETA 16
#define EXA 17
#define ZETTA 18
#define YOTTA 19
#define MILI 20
#define MICRO 21
#define PERCENT 22
#define SPACE 23
#define SQUARE 24

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

#define CHARTS_FILE_VERSION 0x00010000

void charts_store (void) {
	int fd;
	uint32_t s,i,j,p;
	uint64_t *tab;
#ifdef USE_NET_ORDER
	uint8_t *ptr;
	uint8_t hdr[16];
	uint8_t data[8*LENG];
#else
	uint32_t hdr[4];
#endif
	char namehdr[100];

	fd = open(statsfilename,O_WRONLY | O_TRUNC | O_CREAT,0666);
	if (fd<0) {
		mfs_errlog(LOG_WARNING,"error creating charts data file");
		return;
	}
#ifdef USE_NET_ORDER
	ptr = hdr;
	put32bit(&ptr,CHARTS_FILE_VERSION);
	put32bit(&ptr,LENG);
	put32bit(&ptr,statdefscount);
	put32bit(&ptr,timepoint[SHORTRANGE]);
	if (write(fd,(void*)hdr,16)!=16) {
		mfs_errlog(LOG_WARNING,"error writing charts data file");
		close(fd);
		return;
	}
#else
	hdr[0]=CHARTS_FILE_VERSION;
	hdr[1]=LENG;
	hdr[2]=statdefscount;
	hdr[3]=timepoint[SHORTRANGE];
	if (write(fd,(void*)hdr,sizeof(uint32_t)*4)!=sizeof(uint32_t)*4) {
		mfs_errlog(LOG_WARNING,"error writing charts data file");
		close(fd);
		return;
	}
#endif
	for (i=0 ; i<statdefscount ; i++) {
		s = strlen(statdefs[i].name);
		memset(namehdr,0,100);
		memcpy(namehdr,statdefs[i].name,(s>100)?100:s);
		if (write(fd,(void*)namehdr,100)!=100) {
			mfs_errlog(LOG_WARNING,"error writing charts data file");
			close(fd);
			return;
		}
		for (j=0 ; j<RANGES ; j++) {
			tab = series[i][j];
			p = pointers[j]+1;
#ifdef USE_NET_ORDER
			ptr = data;
			for (s=0 ; s<LENG ; s++) {
				put64bit(&ptr,tab[(p+s)%LENG]);
			}
			if (write(fd,(void*)data,8*LENG)!=(ssize_t)(8*LENG)) {
				mfs_errlog(LOG_WARNING,"error writing charts data file");
				close(fd);
				return;
			}
#else
			if (p<LENG) {
				if (write(fd,(void*)(tab+p),sizeof(uint64_t)*(LENG-p))!=(ssize_t)(sizeof(uint64_t)*(LENG-p))) {
					mfs_errlog(LOG_WARNING,"error writing charts data file");
					close(fd);
					return;
				}
			}
			if (write(fd,(void*)tab,sizeof(uint64_t)*p)!=(ssize_t)(sizeof(uint64_t)*p)) {
				mfs_errlog(LOG_WARNING,"error writing charts data file");
				close(fd);
				return;
			}
#endif
		}
	}
	close(fd);
}

int charts_import_from_old_4ranges_format(int fd) {
	uint32_t hdr[21];
	uint32_t i,j,p,fleng,fcharts;
	uint64_t *tab;
	if (read(fd,(void*)hdr,sizeof(uint32_t)*21)!=(ssize_t)(sizeof(uint32_t)*21)) {
		return -1;
	}
// hdr[0]:charts
// hdr[1]:leng
// hdr[2]:shhour;
// hdr[3]:shmin;
// hdr[4]:medhour;
// hdr[5]:medmin;
// hdr[6]:lnghalfhour;
// hdr[7]:lngmday;
// hdr[8]:lngmonth;
// hdr[9]:lngyear;
// hdr[10]:vlngmday;
// hdr[11]:vlngmonth;
// hdr[12]:vlngyear;
// hdr[13]:pointers[SHORTRANGE];
// hdr[14]:pointers[MEDIUMRANGE];
// hdr[15]:pointers[LONGRANGE];
// hdr[16]:pointers[VERYLONGRANGE];
// hdr[17]:timepoint[SHORTRANGE];
// hdr[18]:timepoint[MEDIUMRANGE];
// hdr[19]:timepoint[LONGRANGE];
// hdr[20]:timepoint[VERYLONGRANGE];
	fcharts = hdr[0];
	fleng = hdr[1];
	timepoint[SHORTRANGE]=hdr[17];
//	timepoint[MEDIUMRANGE]=hdr[17]/6;
//	timepoint[LONGRANGE]=hdr[17]/30;
//	timepoint[VERYLONGRANGE]=hdr[17]/(60*24);
	pointers[SHORTRANGE]=LENG-1;
	pointers[MEDIUMRANGE]=LENG-1;
	pointers[LONGRANGE]=LENG-1;
	pointers[VERYLONGRANGE]=LENG-1;
	for (j=0 ; j<RANGES ; j++) {
		p = hdr[13+j];
		for (i=0 ; i<fcharts ; i++) {
			if (i<statdefscount) {
				tab = series[i][j];
				if (fleng>=LENG) {
					if (p>=LENG-1) {
						if (p>LENG-1) {
							lseek(fd,(p-(LENG-1))*sizeof(uint64_t),SEEK_CUR);
						}
						if (read(fd,(void*)tab,sizeof(uint64_t)*LENG)!=(ssize_t)(sizeof(uint64_t)*LENG)) {
							return -1;
						}
						if (fleng-1>p) {
							lseek(fd,((fleng-1-p))*sizeof(uint64_t),SEEK_CUR);
						}
					} else {
						if (read(fd,(void*)(tab+(LENG-1-p)),sizeof(uint64_t)*(p+1))!=(ssize_t)(sizeof(uint64_t)*(p+1))) {
							return -1;
						}
						if (LENG>fleng) {
							lseek(fd,(fleng-LENG)*sizeof(uint64_t),SEEK_CUR);
						}
						if (read(fd,(void*)tab,(LENG-1-p)*sizeof(uint64_t))!=(ssize_t)((LENG-1-p)*sizeof(uint64_t))) {
							return -1;
						}
					}
				} else {
					if (read(fd,(void*)(tab+(LENG-1-p)),sizeof(uint64_t)*(p+1))!=(ssize_t)(sizeof(uint64_t)*(p+1))) {
						return -1;
					}
					if (p+1<fleng) {
						if (read(fd,(void*)(tab+(LENG-fleng)),(fleng-1-p)*sizeof(uint64_t))!=(ssize_t)((fleng-1-p)*sizeof(uint64_t))) {
							return -1;
						}
					}
				}
			} else {
				lseek(fd,fleng*sizeof(uint64_t),SEEK_CUR);
			}
		}
	}
	return 0;
}

int charts_import_from_old_3ranges_format(int fd) {
	uint32_t hdr[15];
	uint32_t i,j,p,fleng,fcharts;
	uint64_t *tab;
	if (read(fd,(void*)hdr,sizeof(uint32_t)*15)!=(ssize_t)(sizeof(uint32_t)*15)) {
		return -1;
	}
// hdr[0]:charts
// hdr[1]:leng
// hdr[2]:shhour;
// hdr[3]:shmin;
// hdr[4]:medhour;
// hdr[5]:medmin;
// hdr[6]:vlngmday;
// hdr[7]:vlngmonth;
// hdr[8]:vlngyear;
// hdr[9]:pointers[SHORTRANGE];
// hdr[10]:pointers[MEDIUMRANGE];
// hdr[11]:pointers[VERYLONGRANGE];
// hdr[12]:timepoint[SHORTRANGE];
// hdr[13]:timepoint[MEDIUMRANGE];
// hdr[14]:timepoint[VERYLONGRANGE];
	fcharts = hdr[0];
	fleng = hdr[1];
	timepoint[SHORTRANGE]=hdr[12];
//	timepoint[MEDIUMRANGE]=hdr[12]/6;
//	timepoint[LONGRANGE]=hdr[12]/30;
//	timepoint[VERYLONGRANGE]=hdr[12]/(60*24);
	pointers[SHORTRANGE]=LENG-1;
	pointers[MEDIUMRANGE]=LENG-1;
	pointers[LONGRANGE]=LENG-1;
	pointers[VERYLONGRANGE]=LENG-1;
	for (j=0 ; j<RANGES ; j++) {
		if (j==2) {
			continue;
		}
		if (j<2) {
			p = hdr[9+j];
		} else {
			p = hdr[11];
		}
		for (i=0 ; i<fcharts ; i++) {
			if (i<statdefscount) {
				tab = series[i][j];
				if (fleng>=LENG) {
					if (p>=LENG-1) {
						if (p>LENG-1) {
							lseek(fd,(p-(LENG-1))*sizeof(uint64_t),SEEK_CUR);
						}
						if (read(fd,(void*)tab,sizeof(uint64_t)*LENG)!=(ssize_t)(sizeof(uint64_t)*LENG)) {
							return -1;
						}
						if (fleng-1>p) {
							lseek(fd,((fleng-1-p))*sizeof(uint64_t),SEEK_CUR);
						}
					} else {
						if (read(fd,(void*)(tab+(LENG-1-p)),sizeof(uint64_t)*(p+1))!=(ssize_t)(sizeof(uint64_t)*(p+1))) {
							return -1;
						}
						if (LENG>fleng) {
							lseek(fd,(fleng-LENG)*sizeof(uint64_t),SEEK_CUR);
						}
						if (read(fd,(void*)tab,(LENG-1-p)*sizeof(uint64_t))!=(ssize_t)((LENG-1-p)*sizeof(uint64_t))) {
							return -1;
						}
					}
				} else {
					if (read(fd,(void*)(tab+(LENG-1-p)),sizeof(uint64_t)*(p+1))!=(ssize_t)(sizeof(uint64_t)*(p+1))) {
						return -1;
					}
					if (p+1<fleng) {
						if (read(fd,(void*)(tab+(LENG-fleng)),(fleng-1-p)*sizeof(uint64_t))!=(ssize_t)((fleng-1-p)*sizeof(uint64_t))) {
							return -1;
						}
					}
				}
			} else {
				lseek(fd,fleng*sizeof(uint64_t),SEEK_CUR);
			}
		}
	}
	return 0;
}

void charts_load(void) {
	int fd;
	uint32_t i,j,k,fleng,fcharts;
	uint64_t *tab;
#ifdef USE_NET_ORDER
	uint32_t l;
	const uint8_t *ptr;
	uint8_t hdr[16];
	uint8_t data[8*LENG];
#else
	uint32_t hdr[3];
#endif
	char namehdr[101];

	fd = open(statsfilename,O_RDONLY);
	if (fd<0) {
		if (errno!=ENOENT) {
			mfs_errlog(LOG_WARNING,"error reading charts data file");
		} else {
			mfs_syslog(LOG_NOTICE,"no charts data file - initializing empty charts");
		}
		return;
	}
#ifdef USE_NET_ORDER
	if (read(fd,(void*)hdr,16)!=16) {
		mfs_errlog(LOG_WARNING,"error reading charts data file");
		close(fd);
		return;
	}
	ptr = hdr;
	i = get32bit(&ptr);
	if (i!=CHARTS_FILE_VERSION) {
		lseek(fd,4,SEEK_SET);
		memcpy((void*)&j,hdr,4);	// get first 4 bytes of hdr as a 32-bit number in "natural" order
		if (j==4) {
			if (charts_import_from_old_4ranges_format(fd)<0) {
				mfs_syslog(LOG_WARNING,"error importing charts data from 4-ranges format");
			}
		} else if (j==3) {
			if (charts_import_from_old_3ranges_format(fd)<0) {
				mfs_syslog(LOG_WARNING,"error importing charts data from 3-ranges format");
			}
		} else {
			mfs_syslog(LOG_WARNING,"unrecognized charts data file format - initializing empty charts");
		}
		close(fd);
		return;
	}
	fleng = get32bit(&ptr);
	fcharts = get32bit(&ptr);
	i = get32bit(&ptr);
	timepoint[SHORTRANGE]=i;
//	timepoint[MEDIUMRANGE]=i/6;
//	timepoint[LONGRANGE]=i/30;
//	timepoint[VERYLONGRANGE]=i/(24*60);
#else
	if (read(fd,(void*)hdr,sizeof(uint32_t))!=sizeof(uint32_t)) {
		mfs_errlog(LOG_WARNING,"error reading charts data file");
		close(fd);
		return;
	}
	if (hdr[0]!=CHARTS_FILE_VERSION) {
		if (hdr[0]==4) {
			if (charts_import_from_old_4ranges_format(fd)<0) {
				mfs_syslog(LOG_WARNING,"error importing charts data from 4-ranges format");
			}
		} else if (hdr[0]==3) {
			if (charts_import_from_old_3ranges_format(fd)<0) {
				mfs_syslog(LOG_WARNING,"error importing charts data from 3-ranges format");
			}
		} else {
			mfs_syslog(LOG_WARNING,"unrecognized charts data file format - initializing empty charts");
		}
		close(fd);
		return;
	}
	if (read(fd,(void*)hdr,sizeof(uint32_t)*3)!=sizeof(uint32_t)*3) {
		mfs_errlog(LOG_WARNING,"error reading charts data file");
		close(fd);
		return;
	}
	fleng = hdr[0];
	fcharts = hdr[1];
	timepoint[SHORTRANGE]=hdr[2];
//	timepoint[MEDIUMRANGE]=hdr[2]/6;
//	timepoint[LONGRANGE]=hdr[2]/30;
//	timepoint[VERYLONGRANGE]=hdr[2]/(24*60);
#endif
	pointers[SHORTRANGE]=LENG-1;
	pointers[MEDIUMRANGE]=LENG-1;
	pointers[LONGRANGE]=LENG-1;
	pointers[VERYLONGRANGE]=LENG-1;
	for (i=0 ; i<fcharts ; i++) {
		if (read(fd,namehdr,100)!=100) {
			mfs_errlog(LOG_WARNING,"error reading charts data file");
			close(fd);
			return;
		}
		namehdr[100]=0;
		for (j=0 ; j<statdefscount && strcmp(statdefs[j].name,namehdr)!=0 ; j++) {}
		if (j>=statdefscount) {
			lseek(fd,RANGES*fleng*8,SEEK_CUR);
			// ignore data
		} else {
			for (k=0 ; k<RANGES ; k++) {
				tab = series[j][k];
				if (fleng>LENG) {
					lseek(fd,(fleng-LENG)*sizeof(uint64_t),SEEK_CUR);
				}
#ifdef USE_NET_ORDER
				if (fleng<LENG) {
					if (read(fd,(void*)data,8*fleng)!=(ssize_t)(8*fleng)) {
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return;
					}
					ptr = data;
					for (l=LENG-fleng ; l<LENG ; l++) {
						tab[l] = get64bit(&ptr);
					}
				} else {
					if (read(fd,(void*)data,8*LENG)!=(ssize_t)(8*LENG)) {
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return;
					}
					ptr = data;
					for (l=0 ; l<LENG ; l++) {
						tab[l] = get64bit(&ptr);
					}
				}
#else
				if (fleng<LENG) {
					if (read(fd,(void*)(tab+(LENG-fleng)),sizeof(uint64_t)*fleng)!=(ssize_t)(sizeof(uint64_t)*fleng)) {
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return;
					}
				} else {
					if (read(fd,(void*)tab,sizeof(uint64_t)*LENG)!=(ssize_t)(sizeof(uint64_t)*LENG)) {
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return;
					}
				}
#endif
			}
		}
	}
	close(fd);
	mfs_syslog(LOG_NOTICE,"stats file has been loaded");
	return;
}

void charts_filltab(uint64_t *datatab,uint32_t range,uint32_t type,uint32_t cno) {
#if defined(INT64_MIN)
#  define STACK_NODATA INT64_MIN
#elif defined(INT64_C)
#  define STACK_NODATA (-INT64_C(9223372036854775807)-1)
#else
#  define STACK_NODATA (-9223372036854775807LL-1)
#endif

	uint32_t i;
	uint32_t src,*ops;
	int64_t stack[50];
	uint32_t sp;
	if (range>=RANGES || cno==0 || cno>3) {
		for (i=0 ; i<LENG ; i++) {
			datatab[i] = CHARTS_NODATA;
		}
		return;
	}
	if (CHARTS_IS_DIRECT_STAT(type)) {
		if (cno==1) {
			for (i=0 ; i<LENG ; i++) {
				datatab[i] = series[type][range][i];
			}
		} else {
			for (i=0 ; i<LENG ; i++) {
				datatab[i] = CHARTS_NODATA;
			}
		}
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		if (cno==1) {
			src = estatdefs[CHARTS_EXTENDED_POS(type)].c1src;
		} else if (cno==2) {
			src = estatdefs[CHARTS_EXTENDED_POS(type)].c2src;
		} else {
			src = estatdefs[CHARTS_EXTENDED_POS(type)].c3src;
		}
		if (CHARTS_DEF_IS_DIRECT(src)) {
			for (i=0 ; i<LENG ; i++) {
				datatab[i] = series[CHARTS_DIRECT_POS(src)][range][i];
			}
		} else if (CHARTS_DEF_IS_CALC(src)) {
			for (i=0 ; i<LENG ; i++) {
				sp=0;
				ops = calcstartpos[CHARTS_CALC_POS(src)];
				while (*ops!=CHARTS_OP_END) {
					if (CHARTS_IS_DIRECT_STAT(*ops)) {
						if (sp<50) {
							if (series[*ops][range][i]==CHARTS_NODATA) {
								stack[sp]=STACK_NODATA;
							} else {
								stack[sp]=series[*ops][range][i];
							}
							sp++;
						}
					} else if (*ops==CHARTS_OP_ADD) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]+=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_SUB) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]-=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_MIN) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else if (stack[sp-1]<stack[sp-2]) {
								stack[sp-2]=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_MAX) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else if (stack[sp-1]>stack[sp-2]) {
								stack[sp-2]=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_MUL) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]*=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_DIV) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA || stack[sp-1]==0) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]/=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_NEG) {
						if (sp>=1) {
							if (stack[sp-1]!=STACK_NODATA) {
								stack[sp-1]=-stack[sp-1];
							}
						}
					} else if (*ops==CHARTS_OP_CONST) {
						ops++;
						if (sp<50) {
							stack[sp]=*ops;
							sp++;
						}
					}
					ops++;
				}
				if (sp>=1 && stack[sp-1]>=0) {	// STACK_NODATA < 0, so this condition is enough for STACK_NODATA
					datatab[i]=stack[sp-1];
				} else {
					datatab[i]=CHARTS_NODATA;
				}
			}
		} else {
			for (i=0 ; i<LENG ; i++) {
				datatab[i] = CHARTS_NODATA;
			}
		}
	} else {
		for (i=0 ; i<LENG ; i++) {
			datatab[i] = CHARTS_NODATA;
		}
	}
}

uint64_t charts_get (uint32_t type,uint32_t numb) {
	uint64_t result=0,cnt;
	uint64_t *tab;
	uint32_t i,ptr;

	if (numb==0 || numb>LENG) return result;
	if (CHARTS_IS_DIRECT_STAT(type)) {
		tab = series[type][SHORTRANGE];
		ptr = pointers[SHORTRANGE];
		if (statdefs[type].mode == CHARTS_MODE_ADD) {
			cnt=0;
			for (i=0 ; i<numb ; i++) {
				if (tab[(LENG+ptr-i)%LENG]!=CHARTS_NODATA) {
					result += tab[(LENG+ptr-i)%LENG];
					cnt++;
				}
			}
			if (cnt>0) {
				result /= cnt;
			}
		} else {
			for (i=0 ; i<numb ; i++) {
				if (tab[(LENG+ptr-i)%LENG]!=CHARTS_NODATA && tab[(LENG+ptr-i)%LENG]>result) {
					result = tab[(LENG+ptr-i)%LENG];
				}
			}
		}
	}
	return result;
}

void charts_inittimepointers (void) {
	time_t now;
	int32_t local;
	struct tm *ts;

	if (timepoint[SHORTRANGE]==0) {
		now = time(NULL);
		ts = localtime(&now);
#ifdef HAVE_STRUCT_TM_TM_GMTOFF
		local = now+ts->tm_gmtoff;
#else
		local = now;
#endif
	} else {
		now = timepoint[SHORTRANGE]*60;
		ts = gmtime(&now);
		local = now;
	}

	timepoint[SHORTRANGE] = local / 60;
	shmin = ts->tm_min;
	shhour = ts->tm_hour;
	timepoint[MEDIUMRANGE] = local / (60 * 6);
	medmin = ts->tm_min;
	medhour = ts->tm_hour;
	timepoint[LONGRANGE] = local / (60 * 30);
	lnghalfhour = ts->tm_hour*2;
	if (ts->tm_min>=30) {
		lnghalfhour++;
	}
	lngmday = ts->tm_mday;
	lngmonth = ts->tm_mon + 1;
	lngyear = ts->tm_year + 1900;
	timepoint[VERYLONGRANGE] = local / (60 * 60 * 24);
	vlngmday = ts->tm_mday;
	vlngmonth = ts->tm_mon + 1;
	vlngyear = ts->tm_year + 1900;
}

void charts_add (uint64_t *data,uint32_t datats) {
	uint32_t i,j;
	struct tm *ts;
	time_t now = datats;
	int32_t local;

	int32_t nowtime,delta;

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
			for (i=0 ; i<statdefscount ; i++) {
				series[i][SHORTRANGE][pointers[SHORTRANGE]] = CHARTS_NODATA;
			}
			delta--;
		}
		timepoint[SHORTRANGE] = nowtime;
		shmin = ts->tm_min;
		shhour = ts->tm_hour;
	}
	if (delta<=0 && delta>-LENG && data) {
		i = (pointers[SHORTRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][SHORTRANGE][i]==CHARTS_NODATA) {   // no data
				series[j][SHORTRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {  // add mode
				series[j][SHORTRANGE][i] += data[j];
			} else if (data[j]>series[j][SHORTRANGE][i]) {   // max mode
				series[j][SHORTRANGE][i] = data[j];
			}
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
			for (i=0 ; i<statdefscount ; i++) {
				series[i][MEDIUMRANGE][pointers[MEDIUMRANGE]] = CHARTS_NODATA;
			}
			delta--;
		}
		timepoint[MEDIUMRANGE] = nowtime;
		medmin = ts->tm_min;
		medhour = ts->tm_hour;
	}
	if (delta<=0 && delta>-LENG && data) {
		i = (pointers[MEDIUMRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][MEDIUMRANGE][i]==CHARTS_NODATA) {  // no data
				series[j][MEDIUMRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {  // add mode
				series[j][MEDIUMRANGE][i] += data[j];
			} else if (data[j]>series[j][MEDIUMRANGE][i]) {  // max mode
				series[j][MEDIUMRANGE][i] = data[j];
			}
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
			for (i=0 ; i<statdefscount ; i++) {
				series[i][LONGRANGE][pointers[LONGRANGE]] = CHARTS_NODATA;
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
	if (delta<=0 && delta>-LENG && data) {
		i = (pointers[LONGRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][LONGRANGE][i]==CHARTS_NODATA) {    // no data
				series[j][LONGRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {  // add mode
				series[j][LONGRANGE][i] += data[j];
			} else if (data[j]>series[j][LONGRANGE][i]) {    // max mode
				series[j][LONGRANGE][i] = data[j];
			}
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
			for (i=0 ; i<statdefscount ; i++) {
				series[i][VERYLONGRANGE][pointers[VERYLONGRANGE]] = CHARTS_NODATA;
			}
			delta--;
		}
		timepoint[VERYLONGRANGE] = nowtime;
		vlngmday = ts->tm_mday;
		vlngmonth = ts->tm_mon + 1;
		vlngyear = ts->tm_year + 1900;
	}
	if (delta<=0 && delta>-LENG && data) {
		i = (pointers[VERYLONGRANGE] + LENG + delta) % LENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][VERYLONGRANGE][i]==CHARTS_NODATA) {  // no data
				series[j][VERYLONGRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {    // add mode
				series[j][VERYLONGRANGE][i] += data[j];
			} else if (data[j]>series[j][VERYLONGRANGE][i]) {  // max mode
				series[j][VERYLONGRANGE][i] = data[j];
			}
		}
	}
}

void charts_term (void) {
	uint32_t i;
	free(statsfilename);
	if (calcdefs) {
		free(calcdefs);
	}
	if (calcstartpos) {
		free(calcstartpos);
	}
	if (estatdefs) {
		free(estatdefs);
	}
	for (i=0 ; i<statdefscount ; i++) {
		free(statdefs[i].name);
	}
	if (statdefs) {
		free(statdefs);
	}
	if (series) {
		free(series);
	}
#ifdef HAVE_ZLIB_H
	deflateEnd(&zstr);
#endif
}

int charts_init (const uint32_t *calcs,const statdef *stats,const estatdef *estats,const char *filename) {
	uint32_t i,j;

	statsfilename = strdup(filename);
	passert(statsfilename);

	for (i=0,calcdefscount=0 ; calcs[i]!=CHARTS_DEFS_END ; i++) {
		if (calcs[i]==CHARTS_OP_END) {
			calcdefscount++;
		}
	}
	if (i>0 && calcdefscount>0) {
		calcdefs = (uint32_t*)malloc(sizeof(uint32_t)*i);
		passert(calcdefs);
		calcstartpos = (uint32_t**)malloc(sizeof(uint32_t*)*calcdefscount);
		passert(calcstartpos);
		j=0;
		calcstartpos[j]=calcdefs;
		j++;
		for (i=0 ; calcs[i]!=CHARTS_DEFS_END ; i++) {
			calcdefs[i] = calcs[i];
			if (calcs[i]==CHARTS_OP_END) {
				if (j<calcdefscount) {
					calcstartpos[j]=calcdefs+i+1;
					j++;
				}
			}
		}
	} else {
		calcdefs = NULL;
		calcstartpos = NULL;
	}
	for (statdefscount=0 ; stats[statdefscount].divisor ; statdefscount++) {}
	if (statdefscount>0) {
		statdefs = (statdef*)malloc(sizeof(statdef)*statdefscount);
		passert(statdefs);
	} else {
		statdefs = NULL;
	}
	for (i=0 ; i<statdefscount ; i++) {
		statdefs[i].name = strdup(stats[i].name);
		passert(statdefs[i].name);
		statdefs[i].mode = stats[i].mode;
		statdefs[i].percent = stats[i].percent;
		statdefs[i].scale = stats[i].scale;
		statdefs[i].multiplier = stats[i].multiplier;
		statdefs[i].divisor = stats[i].divisor;
	}
	for (estatdefscount=0 ; estats[estatdefscount].divisor ; estatdefscount++) {}
	if (estatdefscount>0) {
		estatdefs = (estatdef*)malloc(sizeof(estatdef)*estatdefscount);
		passert(estatdefs);
	} else {
		estatdefs = NULL;
	}
	for (i=0 ; i<estatdefscount ; i++) {
		estatdefs[i].c1src = estats[i].c1src;
		estatdefs[i].c2src = estats[i].c2src;
		estatdefs[i].c3src = estats[i].c3src;
		estatdefs[i].mode = estats[i].mode;
		estatdefs[i].percent = estats[i].percent;
		estatdefs[i].scale = estats[i].scale;
		estatdefs[i].multiplier = estats[i].multiplier;
		estatdefs[i].divisor = estats[i].divisor;
	}

	if (statdefscount>0) {
		series = (stat_record*)malloc(sizeof(stat_record)*statdefscount);
		passert(series);
	} else {
		series = NULL;
	}
	for (i=0 ; i<statdefscount ; i++) {
		memset(series+i,0xFF,sizeof(stat_record));
	}

	for (i=0 ; i<RANGES ; i++) {
		pointers[i]=0;
		timepoint[i]=0;
	}

	charts_load();
	charts_inittimepointers();
	charts_add(NULL,time(NULL));

#ifdef HAVE_ZLIB_H
	zstr.zalloc = NULL;
	zstr.zfree = NULL;
	zstr.opaque = NULL;
	if (deflateInit(&zstr,Z_DEFAULT_COMPRESSION)!=Z_OK) {
		return -1;
	}
#endif /* HAVE_ZLIB_H */
	return 0;
}

#ifndef HAVE_ZLIB_H
static inline void charts_putwarning(uint32_t posx,uint32_t posy,uint8_t color) {
	uint8_t *w,c,fx,fy,b;
	uint32_t x,y;
	w = warning;
	y = posy;
	for (fy=0 ; fy<5 ; fy++) {
		x = posx;
		for (b=0 ; b<10 ; b++) {
			c = *w;
			w++;
			for (fx=0 ; fx<8 ; fx++) {
				if (c&0x80 && x<XSIZE && y<YSIZE) {
					chart[(XSIZE)*y+x] = color;
				}
				c<<=1;
				x++;
			}
		}
		y++;
	}
}
#endif

static inline void charts_puttext(int32_t posx,int32_t posy,uint8_t color,uint8_t *data,uint32_t leng,int32_t minx,int32_t maxx,int32_t miny,int32_t maxy) {
	uint32_t i,fx,fy;
	uint8_t fp,fbits;
	int32_t px,x,y;
	for (i=0 ; i<leng ; i++) {
		px = i*6+posx;
		fp = data[i];
		if (fp>SQUARE) {
			fp=SQUARE;
		}
		for (fy=0 ; fy<9 ; fy++) {
			fbits = font[fp][fy];
			if (fbits) {
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
}

double charts_fixmax(uint64_t max,uint8_t *scale,uint8_t *mode,uint16_t *base) {
	double rmax;
	if (max>995000000000000000ULL) {						// ##.# E
		(*base) = (max+499999999999999999ULL)/500000000000000000ULL;
		rmax = (*base)*500000000000000000ULL;
		(*mode) = 1;
		(*scale) += 6;
	} else if (max>99500000000000000ULL) {						// ### P
		(*base) = (max+4999999999999999ULL)/5000000000000000ULL;
		rmax = (*base)*5000000000000000ULL;
		(*mode) = 0;
		(*scale) += 5;
	} else if (max>995000000000000ULL) {						// ##.# P
		(*base) = (max+499999999999999ULL)/500000000000000ULL;
		rmax = (*base)*500000000000000ULL;
		(*mode) = 1;
		(*scale) += 5;
	} else if (max>99500000000000ULL) {						// ### T
		(*base) = (max+4999999999999ULL)/5000000000000ULL;
		rmax = (*base)*5000000000000ULL;
		(*mode) = 0;
		(*scale) += 4;
	} else if (max>995000000000ULL) {						// ##.# T
		(*base) = (max+499999999999ULL)/500000000000ULL;
		rmax = (*base)*500000000000ULL;
		(*mode) = 1;
		(*scale) += 4;
	} else if (max>99500000000ULL) {						// ### G
		(*base) = (max+4999999999ULL)/5000000000ULL;
		rmax = (*base)*5000000000ULL;
		(*mode) = 0;
		(*scale) += 3;
	} else if (max>995000000ULL) {							// ##.# G
		(*base) = (max+499999999ULL)/500000000ULL;
		rmax = (*base)*500000000ULL;
		(*mode) = 1;
		(*scale) += 3;
	} else if (max>99500000ULL) {							// ### M
		(*base) = (max+4999999ULL)/5000000ULL;
		rmax = (*base)*5000000ULL;
		(*mode) = 0;
		(*scale) += 2;
	} else if (max>995000ULL) {							// ##.# M
		(*base) = (max+499999ULL)/500000ULL;
		rmax = (*base)*500000ULL;
		(*mode) = 1;
		(*scale) += 2;
	} else if (max>99500ULL) {							// ### k
		(*base) = (max+4999ULL)/5000ULL;
		rmax = (*base)*5000ULL;
		(*mode) = 0;
		(*scale) += 1;
	} else if (max>995ULL) {							// ##.# k
		(*base) = (max+499ULL)/500ULL;
		rmax = (*base)*500ULL;
		(*mode) = 1;
		(*scale) += 1;
	} else if (max>99ULL) {								// ###
		(*base) = (max+4ULL)/5ULL;
		rmax = (*base)*5ULL;
		(*mode) = 0;
	} else {									// ##.#
		if (max==0) {
			max=1;
		}
		(*base) = max*2;
		rmax = ((*base)*5)/10.0;
		(*mode) = 1;
	}
	return rmax;
}

void charts_makechart(uint32_t type,uint32_t range) {
	static const uint8_t jtab[11]={MICRO,MILI,SPACE,KILO,MEGA,GIGA,TERA,PETA,EXA,ZETTA,YOTTA};
	int32_t i,j;
	uint32_t xy,xm,xd,xh,xs,xoff,xbold,ys;
	uint64_t max;
	double dmax;
	uint64_t d,c1d,c2d,c3d;
	uint64_t c1dispdata[LENG];
	uint64_t c2dispdata[LENG];
	uint64_t c3dispdata[LENG];
	uint8_t scale,mode=0,percent=0;
	uint16_t base=0;
	uint32_t pointer;
	uint8_t text[6];

	memset(chart,COLOR_TRANSPARENT,(XSIZE)*(YSIZE));

	charts_filltab(c1dispdata,range,type,1);
	charts_filltab(c2dispdata,range,type,2);
	charts_filltab(c3dispdata,range,type,3);

	pointer = pointers[range];

	max = 0;
	for (i=0 ; i<LENG ; i++) {
		d = 0;
		if (c1dispdata[i]!=CHARTS_NODATA) {
			d += c1dispdata[i];
		}
		if (c2dispdata[i]!=CHARTS_NODATA) {
			d += c2dispdata[i];
		}
		if (c3dispdata[i]!=CHARTS_NODATA) {
			d += c3dispdata[i];
		}
		if (d>max) {
			max=d;
		}
	}
	if (max>1000000000000000000ULL) {	// arithmetic overflow protection
		for (i=0 ; i<LENG ; i++) {
			if (c1dispdata[i]!=CHARTS_NODATA) {
				c1dispdata[i]/=1000;
			}
			if (c2dispdata[i]!=CHARTS_NODATA) {
				c2dispdata[i]/=1000;
			}
			if (c3dispdata[i]!=CHARTS_NODATA) {
				c3dispdata[i]/=1000;
			}
		}
		max/=1000;
		scale=1;
	} else {
		scale=0;
	}

	// range scale
	if ((CHARTS_IS_DIRECT_STAT(type) && statdefs[type].mode==CHARTS_MODE_ADD) || (CHARTS_IS_EXTENDED_STAT(type) && estatdefs[CHARTS_EXTENDED_POS(type)].mode==CHARTS_MODE_ADD)) {
		switch (range) {
			case MEDIUMRANGE:
				max = (max+5)/6;
				break;
			case LONGRANGE:
				max = (max+29)/30;
				break;
			case VERYLONGRANGE:
				max = (max+1439)/(24*60);
				break;
		}
	}

	if (CHARTS_IS_DIRECT_STAT(type)) {
		scale += statdefs[type].scale;
		percent = statdefs[type].percent;
		max *= statdefs[type].multiplier;
		max /= statdefs[type].divisor;
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		scale += estatdefs[CHARTS_EXTENDED_POS(type)].scale;
		percent = estatdefs[CHARTS_EXTENDED_POS(type)].percent;
		max *= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
		max /= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
	}

	dmax = charts_fixmax(max,&scale,&mode,&base);

	if (CHARTS_IS_DIRECT_STAT(type)) {
		dmax *= statdefs[type].divisor;
		dmax /= statdefs[type].multiplier;
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		dmax *= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
		dmax /= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
	}

	// range scale
	if ((CHARTS_IS_DIRECT_STAT(type) && statdefs[type].mode==CHARTS_MODE_ADD) || (CHARTS_IS_EXTENDED_STAT(type) && estatdefs[CHARTS_EXTENDED_POS(type)].mode==CHARTS_MODE_ADD)) {
		switch (range) {
			case MEDIUMRANGE:
				dmax *= 6;
				break;
			case LONGRANGE:
				dmax *= 30;
				break;
			case VERYLONGRANGE:
				dmax *= (24*60);
				break;
		}
	}

//	m = 0;
	for (i=0 ; i<LENG ; i++) {
		j = (LENG+1+pointer+i)%LENG;
		if ((c1dispdata[j]&c2dispdata[j]&c3dispdata[j])==CHARTS_NODATA) {
			for (j=0 ; j<DATA ; j++) {
				chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = ((j+i)%3)?COLOR_BKG:COLOR_NODATA; //(((j+i)&3)&&((j+2+LENG-i)&3))?COLOR_BKG:COLOR_DATA1;
			}
		} else {
			if (c3dispdata[j]!=CHARTS_NODATA) {
				c3d = c3dispdata[j];
			} else {
				c3d = 0;
			}
			if (c2dispdata[j]!=CHARTS_NODATA) {
				c2d = c3d + c2dispdata[j];
			} else {
				c2d = c3d;
			}
			if (c1dispdata[j]!=CHARTS_NODATA) {
				c1d = c2d + c1dispdata[j];
			} else {
				c1d = c2d;
			}
			c1d *= DATA;
			c1d /= dmax;
			c2d *= DATA;
			c2d /= dmax;
			c3d *= DATA;
			c3d /= dmax;

			j=0;
			while (DATA>=c1d+j) {
				chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_BKG;
				j++;
			}
			while (DATA>=c2d+j) {
				chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA1;
				j++;
			}
			while (DATA>=c3d+j) {
				chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA2;
				j++;
			}
			while (DATA>j) {
				chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA3;
				j++;
			}
		}
	}
	// axes
	for (i=-3 ; i<LENG+3 ; i++) {
		chart[(XSIZE)*(DATA+YPOS)+(i+XPOS)] = COLOR_AXIS;
	}
	for (i=-2 ; i<DATA+5 ; i++) {
		chart[(XSIZE)*(DATA-i+YPOS)+(XPOS-1)] = COLOR_AXIS;
		chart[(XSIZE)*(DATA-i+YPOS)+(XPOS+LENG)] = COLOR_AXIS;
	}
	// x scale
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
//		k = LENG;
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
					charts_puttext(XPOS+i-14,(YPOS+DATA)+4,COLOR_TEXT,text,5,XPOS-1,XPOS+LENG,0,YSIZE-1);
				} else {
					text[0]=xm/10;
					text[1]=xm%10;
					text[2]=FDOT;
					text[3]=xd/10;
					text[4]=xd%10;
					charts_puttext(XPOS+i+10,(YPOS+DATA)+4,COLOR_TEXT,text,5,XPOS-1,XPOS+LENG,0,YSIZE-1);
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
			charts_puttext(XPOS+i+10,(YPOS+DATA)+4,COLOR_TEXT,text,5,XPOS-1,XPOS+LENG,0,YSIZE-1);
		}
	} else {
		xy = lngyear;
		xm = lngmonth;
//		k = LENG;
		for (i=LENG-lngmday ; i>=0 ; ) {
			text[0]=xm/10;
			text[1]=xm%10;
			charts_puttext(XPOS+i+(getmonleng(xy,xm)-11)/2+1,(YPOS+DATA)+4,COLOR_TEXT,text,2,XPOS-1,XPOS+LENG,0,YSIZE-1);
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
//			k = i;
		}
		text[0]=xm/10;
		text[1]=xm%10;
		charts_puttext(XPOS+i+(getmonleng(xy,xm)-11)/2+1,(YPOS+DATA)+4,COLOR_TEXT,text,2,XPOS-1,XPOS+LENG,0,YSIZE-1);
	}
	// y scale

/*
	// range scale
	if ((CHARTS_IS_DIRECT_STAT(type) && statdefs[type].mode==CHARTS_MODE_ADD) || (CHARTS_IS_EXTENDED_STAT(type) && estatdefs[CHARTS_EXTENDED_POS(type)].mode==CHARTS_MODE_ADD)) {
		switch (range) {
			case SHORTRANGE:
				ymax = max;
				break;
			case MEDIUMRANGE:
				ymax = max/6;
				break;
			case LONGRANGE:
				ymax = max/30;
				break;
			case VERYLONGRANGE:
				ymax = max/(24*60);
				break;
			default:
				ymax=0;
		}
	} else {
		ymax = max;
	}

	if (CHARTS_IS_DIRECT_STAT(type)) {
		scale = statdefs[type].scale;
		ymax *= statdefs[type].multiplier;
//		ymin *= statdefs[type].multiplier;
		ymax /= statdefs[type].divisor;
//		ymin /= statdefs[type].divisor;
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		scale = estatdefs[CHARTS_EXTENDED_POS(type)].scale;
		ymax *= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
//		ymin *= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
		ymax /= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
//		ymin /= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
	}
*/
//	for (i=0 ; i<LENG ; i+=2) {
//		for (j=DATA-20 ; j>=0 ; j-=20) {
//			chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = 2;
//		}
//	}
	for (i=0 ; i<=5 ; i++) {
		d = base*i;
		j=0;
		if (mode==0) {	// ###
			if (d>=10) {
				if (d>=100) {
					text[j++]=d/100;
					d%=100;
				}
				text[j++]=d/10;
			}
			text[j++]=d%10;
		} else {	// ##.#
			if (d>=100) {
				text[j++]=d/100;
				d%=100;
			}
			text[j++]=d/10;
			text[j++]=FDOT;
			text[j++]=d%10;
		}
		if (scale<11) {
			if (jtab[scale]!=SPACE) {
				text[j++]=jtab[scale];
			}
		} else {
			text[j++]=SQUARE;
		}
		if (percent) {
			text[j++]=PERCENT;
		}
		charts_puttext(XPOS - 4 - (j*6),(YPOS+DATA-(20*i))-3,COLOR_TEXT,text,j,0,XSIZE-1,0,YSIZE-1);
		chart[(XSIZE)*(YPOS+DATA-20*i)+(XPOS-2)] = COLOR_AXIS;
		chart[(XSIZE)*(YPOS+DATA-20*i)+(XPOS-3)] = COLOR_AXIS;
		if (i>0) {
			for (j=1 ; j<LENG ; j+=2) {
				chart[(XSIZE)*(YPOS+DATA-20*i)+(XPOS+j)] = COLOR_AUX;
			}
		}
	}
}

uint32_t charts_datasize(uint32_t number) {
	uint32_t chtype,chrange;

	chtype = number / 10;
	chrange = number % 10;
	return (chrange<RANGES && CHARTS_IS_DIRECT_STAT(chtype))?LENG*8+4:0;
}

void charts_makedata(uint8_t *buff,uint32_t number) {
	uint32_t i,j,ts,pointer,chtype,chrange;
	uint64_t *tab;

	chtype = number / 10;
	chrange = number % 10;
	if (chrange<RANGES && CHARTS_IS_DIRECT_STAT(chtype)) {
		tab = series[chtype][chrange];
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
		put32bit(&buff,ts);
		for (i=0 ; i<LENG ; i++) {
			j = (LENG+1+pointer+i)%LENG;
			put64bit(&buff,tab[j]);
		}
	}
}

void charts_chart_to_rawchart() {
	uint32_t y;
	uint32_t x;
	uint8_t *cp,*rp;
	cp = chart;
	rp = rawchart;
	for (y=0 ; y<(YSIZE) ; y++) {
		*rp=0;
		rp++;
		for (x=0 ; x<(XSIZE) ; x+=2) {
			if (x+1<(XSIZE)) {
				*rp = ((*cp)<<4) | ((cp[1])&0x0F);
			} else {
				*rp = ((*cp)<<4);
			}
			rp++;
			cp+=2;
		}
	}
}

void charts_fill_crc(uint8_t *buff,uint32_t leng) {
	uint8_t *ptr,*eptr;
	uint32_t crc,chleng;
	ptr = buff+8;
	eptr = buff+leng;
	while (ptr+4<=eptr) {
		chleng = get32bit((const uint8_t **)&ptr);
		if (ptr+8+chleng<=eptr) {
			crc = mycrc32(0,ptr,chleng+4);
			ptr += chleng+4;
			if (memcmp(ptr,"CRC#",4)==0) {
				put32bit(&ptr,crc);
			} else {
				syslog(LOG_WARNING,"charts: unexpected data in generated png stream");
			}
		}
	}
}

#ifndef HAVE_ZLIB_H

#define MOD_ADLER 65521

static uint32_t charts_adler32(uint8_t *data,uint32_t len) {
	uint32_t a = 1, b = 0;
	uint32_t i;

	for (i=0 ; i<len ; i++) {
		a = (a + data[i]) % MOD_ADLER;
		b = (b + a) % MOD_ADLER;
	}

	return (b << 16) | a;
}

int charts_fake_compress(uint8_t *src,uint32_t srcsize,uint8_t *dst,uint32_t *dstsize) {
	uint32_t edstsize,adler;
	edstsize = 6+(65535+5)*(srcsize/65535);
	if (srcsize%65535) {
		edstsize+=5+(srcsize%65535);
	}
	if (edstsize>*dstsize) {
		return -1;
	}
	adler = charts_adler32(src,srcsize);
	*dst++=0x78;
	*dst++=0x9C;
	while (srcsize>65535) {
		*dst++ = 0x00;
		*dst++ = 0xFF;
		*dst++ = 0xFF;
		*dst++ = 0x00;
		*dst++ = 0x00;
		memcpy(dst,src,65535);
		dst+=65535;
		src+=65535;
		srcsize-=65535;
	}
	if (srcsize>0) {
		*dst++ = 0x01;
		*dst++ = srcsize&0xFF;
		*dst++ = srcsize>>8;
		*dst++ = (srcsize&0xFF)^0xFF;
		*dst++ = (srcsize>>8)^0xFF;
		memcpy(dst,src,srcsize);
		dst+=srcsize;
	}
	*dst++ = (adler>>24) & 0xFF;
	*dst++ = (adler>>16) & 0xFF;
	*dst++ = (adler>>8) & 0xFF;
	*dst++ = adler & 0xFF;
	*dstsize = edstsize;
	return 0;
}
#endif /* ! HAVE_ZLIB_H */

uint32_t charts_make_png(uint32_t number) {
	uint32_t chtype,chrange;
	chtype = number / 10;
	chrange = number % 10;
	if (chrange>=RANGES) {
		compsize = 0;
		return sizeof(png_1x1);
	}
	if (!(CHARTS_IS_DIRECT_STAT(chtype) || CHARTS_IS_EXTENDED_STAT(chtype))) {
		compsize = 0;
		return sizeof(png_1x1);
	}

	charts_makechart(chtype,chrange);
#ifndef HAVE_ZLIB_H
	charts_putwarning(47,0,COLOR_TEXT);
#endif

	charts_chart_to_rawchart();

#ifdef HAVE_ZLIB_H
	if (deflateReset(&zstr)!=Z_OK) {
		compsize = 0;
		return sizeof(png_1x1);
	}
	zstr.next_in = rawchart;
	zstr.avail_in = RAWSIZE;
	zstr.total_in = 0;
	zstr.next_out = compbuff;
	zstr.avail_out = CBUFFSIZE;
	zstr.total_out = 0;

	if (deflate(&zstr,Z_FINISH)!=Z_STREAM_END) {
		compsize = 0;
		return sizeof(png_1x1);
	}

	compsize = zstr.total_out;
#else /* HAVE_ZLIB_H */
	compsize = CBUFFSIZE;
	if (charts_fake_compress(rawchart,RAWSIZE,compbuff,&compsize)<0) {
		compsize = 0;
		return sizeof(png_1x1);
	}
#endif /* HAVE_ZLIB_H */

	return sizeof(png_header)+compsize+sizeof(png_tailer);
}

void charts_get_png(uint8_t *buff) {
	uint8_t *ptr;
	if (compsize==0) {
		memcpy(buff,png_1x1,sizeof(png_1x1));
	} else {
		memcpy(buff,png_header,sizeof(png_header));
		ptr = buff+(sizeof(png_header)-8);
		put32bit(&ptr,compsize);
		memcpy(buff+sizeof(png_header),compbuff,compsize);
		memcpy(buff+sizeof(png_header)+compsize,png_tailer,sizeof(png_tailer));
		charts_fill_crc(buff,sizeof(png_header)+compsize+sizeof(png_tailer));
	}
	compsize=0;
}
