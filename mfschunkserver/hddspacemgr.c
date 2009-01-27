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

//#define USE_PIO

#include <inttypes.h>
#include <syslog.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#ifdef USE_MMAP
#include <sys/mman.h>
#endif
#include <dirent.h>
#include <errno.h>

#include "MFSCommunication.h"
#include "cfg.h"
#include "datapack.h"
#include "crc.h"
#include "main.h"
#include "masterconn.h"

#include "config.h"

#define CLOSEDELAY 60

#define CHUNKHDRSIZE (1024+4*1024)
#define CHUNKHDRCRC 1024

#define LASTERRSIZE 3
#define LASTERRTIME 3600

#define HASHSIZE 32768
#define HASHPOS(chunkid) ((chunkid)&0x7FFF)

typedef struct chunkwcrc {
	uint64_t chunkid;
	struct chunkwcrc *next;
} chunkwcrc;

struct folder;

typedef struct ioerror {
	uint64_t chunkid;
	uint32_t timestamp;
} ioerror;

typedef struct chunk {
	char *filename;
	uint64_t chunkid;
	struct folder *owner;
	uint32_t version;
	uint16_t blocks;
	uint16_t crcrefcount;
	uint32_t lastactivity;
	uint8_t crcchanged;
	uint8_t *crc;
	int fd;
//	uint8_t *block;
//	uint8_t blockchanged;
//	uint16_t blockno;	// 0xFFFF == invalid
//	uint8_t erroroccured;
	struct chunk *hashnext;
} chunk;

typedef struct folder {
	char *path;
	int needrefresh;
	int todel;
	uint64_t leavefree;
	uint64_t avail;
	uint64_t total;
	ioerror lasterrtab[LASTERRSIZE];
	uint32_t chunkcount;
	uint32_t lasterrindx;
	struct folder *next;
	dev_t devid;
	ino_t lockinode;
} folder;

/*
typedef struct damaged {
	char *path;
	uint64_t avail;
	uint64_t total;
	ioerror lasterror;
	uint32_t chunkcount;
	struct damaged_disk *next;
} damaged;
*/

//uint8_t erroroccured=0;
static folder *damagedhead=NULL;
static folder *folderhead=NULL;
static chunk* hashtab[HASHSIZE];

static chunkwcrc *chunkswithcrc;

static uint8_t hdrbuffer[CHUNKHDRSIZE];
static uint8_t blockbuffer[0x10000];
static uint32_t emptyblockcrc;

static uint32_t stats_bytesr=0;
static uint32_t stats_bytesw=0;
static uint32_t stats_opr=0;
static uint32_t stats_opw=0;
static uint32_t stats_databytesr=0;
static uint32_t stats_databytesw=0;
static uint32_t stats_dataopr=0;
static uint32_t stats_dataopw=0;
static uint64_t stats_rtime=0;
static uint64_t stats_wtime=0;

void hdd_stats(uint32_t *br,uint32_t *bw,uint32_t *opr,uint32_t *opw,uint32_t *dbr,uint32_t *dbw,uint32_t *dopr,uint32_t *dopw,uint64_t *rtime,uint64_t *wtime) {
	*br = stats_bytesr;
	*bw = stats_bytesw;
	*opr = stats_opr;
	*opw = stats_opw;
	*dbr = stats_databytesr;
	*dbw = stats_databytesw;
	*dopr = stats_dataopr;
	*dopw = stats_dataopw;
	*rtime = stats_rtime;
	*wtime = stats_wtime;
	stats_bytesr=0;
	stats_bytesw=0;
	stats_opr=0;
	stats_opw=0;
	stats_databytesr=0;
	stats_databytesw=0;
	stats_dataopr=0;
	stats_dataopw=0;
	stats_rtime=0;
	stats_wtime=0;
}

uint32_t hdd_diskinfo_size() {
	folder *f;
	uint32_t s=0,sl;
	for (f=folderhead ; f ; f=f->next ) {
		sl = strlen(f->path);
		if (sl>255) {
			sl=255;
		}
		s+=34+sl;
	}
	for (f=damagedhead ; f ; f=f->next ) {
		sl = strlen(f->path);
		if (sl>255) {
			sl=255;
		}
		s+=34+sl;
	}
	return s;
}

void hdd_diskinfo_data(uint8_t *buff) {
	folder *f;
	uint32_t sl;
	uint64_t t64;
	uint32_t t32,ei;
	for (f=folderhead ; f ; f=f->next ) {
		sl = strlen(f->path);
		if (sl>255) {
			PUT8BIT(255,buff);
			memcpy(buff,"(...)",5);
			memcpy(buff+5,f->path+(sl-250),250);
			buff+=255;
		} else {
			PUT8BIT(sl,buff);
			if (sl>0) {
				memcpy(buff,f->path,sl);
				buff+=sl;
			}
		}
		if (f->todel) {
			PUT8BIT(1,buff);
		} else {
			PUT8BIT(0,buff);
		}
		ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
		t64 = f->lasterrtab[ei].chunkid;
		PUT64BIT(t64,buff);
		t32 = f->lasterrtab[ei].timestamp;
		PUT32BIT(t32,buff);
		t64 = f->total - f->avail;
		PUT64BIT(t64,buff);
		t64 = f->total;
		PUT64BIT(t64,buff);
		t32 = f->chunkcount;
		PUT32BIT(t32,buff);
	}
	for (f=damagedhead ; f ; f=f->next ) {
		sl = strlen(f->path);
		if (sl>255) {
			PUT8BIT(255,buff);
			memcpy(buff,"(...)",5);
			memcpy(buff+5,f->path+(sl-250),250);
			buff+=255;
		} else {
			PUT8BIT(sl,buff);
			if (sl>0) {
				memcpy(buff,f->path,sl);
				buff+=sl;
			}
		}
		if (f->todel) {
			PUT8BIT(3,buff);
		} else {
			PUT8BIT(2,buff);
		}
		ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
		t64 = f->lasterrtab[ei].chunkid;
		PUT64BIT(t64,buff);
		t32 = f->lasterrtab[ei].timestamp;
		PUT32BIT(t32,buff);
		t64 = f->total - f->avail;
		PUT64BIT(t64,buff);
		t64 = f->total;
		PUT64BIT(t64,buff);
		t32 = f->chunkcount;
		PUT32BIT(t32,buff);
	}
}

void hdd_refresh_usage(folder *f) {
	struct statvfs fsinfo;

	if (statvfs(f->path,&fsinfo)<0) {
		f->avail = 0ULL;
		f->total = 0ULL;
	}
	f->avail = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_bavail);
	f->total = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks);
	if (f->avail < f->leavefree) {
		f->avail = 0ULL;
	} else {
		f->avail -= f->leavefree;
	}
}

void hdd_send_space() {
	folder **fptr,*f;
	chunk **cptr,*c;
	uint32_t i;
	uint32_t now;
	int changed=0,err;
	uint64_t avail,total;
	uint64_t tdavail,tdtotal;
	uint32_t chunks,tdchunks;
	now = main_time();
	avail = total = tdavail = tdtotal = 0ULL;
	chunks = tdchunks = 0;
	fptr = &(folderhead);
	while ((f=*fptr)) {
		err=1;
		for (i=0 ; err && i<LASTERRSIZE ; i++) {
			if (f->lasterrtab[i].timestamp+3600<now) {
			   err=0;
			}
		}
		if (err) {
			syslog(LOG_WARNING,"%u errors occurred in %u seconds on folder: %s",LASTERRSIZE,LASTERRTIME,f->path);
			for (i=0 ; i<HASHSIZE ; i++) {
				cptr = &(hashtab[i]);
				while ((c=*cptr)) {
					if (c->owner == f) {
						(*cptr)=c->hashnext;
						masterconn_send_chunk_lost(c->chunkid);
						if (c->fd>=0) {
							close(c->fd);
						}
						if (c->crc!=NULL) {
							free(c->crc);
						}
						free(c->filename);
						free(c);
					} else {
						cptr = &(c->hashnext);
					}
				}
			}
			(*fptr)=f->next;
//			free(f->path);
//			free(f);
			f->next = damagedhead;
			damagedhead = f;
			changed=1;
		} else {
			if (f->needrefresh) {
				hdd_refresh_usage(f);
				f->needrefresh = 0;
				changed=1;
			}
			if (f->todel==0) {
				avail += f->avail;
				total += f->total;
				chunks += f->chunkcount;
			} else {
				tdavail += f->avail;
				tdtotal += f->total;
				tdchunks += f->chunkcount;
			}
			fptr = &(f->next);
		}
	}
	if (changed) {
		masterconn_send_space(total-avail,total,chunks,tdtotal-tdavail,tdtotal,tdchunks);
	}
}

void hdd_time_refresh() {
	folder *f;
	for (f=folderhead ; f ; f=f->next ) {
		f->needrefresh=1;
	}
}

void hdd_error_occured(chunk *c) {
	uint32_t i;
	folder *f;
	f = c->owner;
	i = f->lasterrindx;
	f->lasterrtab[i].chunkid = c->chunkid;
	f->lasterrtab[i].timestamp = main_time();
	i = (i+1)%LASTERRSIZE;
	f->lasterrindx = i;
	masterconn_send_error_occurred();
}

chunk* chunk_new(folder *f,uint64_t chunkid,uint32_t version) {
	chunk *new;
	uint32_t leng;
	uint32_t hashpos = HASHPOS(chunkid);
	char c;
	new = (chunk*)malloc(sizeof(chunk));
	leng = strlen(f->path);
	new->filename = malloc(leng+38);
	memcpy(new->filename,f->path,leng);
	c = chunkid&0xF;
	if (c<10) {
		c+='0';
	} else {
		c-=10;
		c+='A';
	}
	sprintf(new->filename+leng,"%c/chunk_%016"PRIX64"_%08"PRIX32".mfs",c,chunkid,version);
	new->chunkid = chunkid;
	new->version = version;
	new->blocks = 0;
	new->crc = NULL;
	new->crcrefcount = 0;
	new->crcchanged = 0;
	new->fd = -1;
	new->owner = f;
	f->needrefresh = 1;
	f->chunkcount++;
	new->hashnext = hashtab[hashpos];
	hashtab[hashpos] = new;
	return new;
}

void chunk_delete(uint64_t chunkid) {
	folder *f;
	chunk **cptr,*c;
	uint32_t hashpos = HASHPOS(chunkid);
	cptr = &(hashtab[hashpos]);
	while ((c=*cptr)) {
		if (c->chunkid == chunkid) {
			(*cptr)=c->hashnext;
			f = c->owner;
			f->chunkcount--;
			f->needrefresh = 1;
			if (c->fd>=0) {
				close(c->fd);
			}
			if (c->crc!=NULL) {
				free(c->crc);
			}
			free(c->filename);
			free(c);
			break;
		} else {
			cptr = &(c->hashnext);
		}
	}
}

chunk* chunk_find(uint64_t chunkid) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
	for (c = hashtab[hashpos] ; c ; c=c->hashnext) {
		if (c->chunkid == chunkid) {
			return c;
		}
	}
	return NULL;
}
/* */

folder* find_best_folder() {
	folder *f,*result;
	double avail,bestavail=0.0;
	result=NULL;
	for (f=folderhead ; f ; f=f->next) {
		if (f->total>0 && f->avail>0 && f->todel==0) {
			avail = (double)(f->avail)/(double)(f->total);
			if (avail>bestavail) {
				bestavail = avail;
				result = f;
			}
		}
	}
	return result;
}

/* interface */

uint32_t get_chunkscount() {
	uint32_t res=0;
	uint32_t i;
	chunk *c;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c = hashtab[i] ; c ; c=c->hashnext) {
			res++;
		}
	}
	return res;
}

void fill_chunksinfo(uint8_t *buff) {
	uint32_t i,v;
	chunk *c;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c = hashtab[i] ; c ; c=c->hashnext) {
			PUT64BIT(c->chunkid,buff);
			v = c->version;
			if (c->owner->todel) {
				v|=0x80000000;
			}
			PUT32BIT(v,buff);
		}
	}
}

/*
uint32_t get_changedchunkscount() {
	uint32_t res=0;
	folder *f;
	chunk *c;
	if (somethingchanged==0) {
		return 0;
	}
	for (f = folderhead ; f ; f=f->next) {
		for (c = f->chunkhead ; c ; c=c->next) {
			if (c->lengthchanged) {
				res++;
			}
		}
	}
	return res;
}

void fill_changedchunksinfo(uint8_t *buff) {
	folder *f;
	chunk *c;
	for (f = folderhead ; f ; f=f->next) {
		for (c = f->chunkhead ; c ; c=c->next) {
			if (c->lengthchanged) {
				PUT64BIT(c->chunkid,buff);
				PUT32BIT(c->version,buff);
				c->lengthchanged = 0;
			}
		}
	}
	somethingchanged = 0;
}
*/

void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount) {
	folder *f;
	uint64_t avail,total;
	uint64_t tdavail,tdtotal;
	uint32_t chunks,tdchunks;
	avail = total = tdavail = tdtotal = 0ULL;
	chunks = tdchunks = 0;
	for (f = folderhead ; f ; f=f->next) {
		if (f->todel==0) {
			avail+=f->avail;
			total+=f->total;
			chunks+=f->chunkcount;
		} else {
			tdavail+=f->avail;
			tdtotal+=f->total;
			tdchunks+=f->chunkcount;
		}
	}
	*usedspace = total - avail;
	*totalspace = total;
	*chunkcount = chunks;
	*tdusedspace = tdtotal - tdavail;
	*tdtotalspace = tdtotal;
	*tdchunkcount = tdchunks;
}

int check_chunk(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		return ERROR_WRONGVERSION;
	}
	return STATUS_OK;
}

int get_chunk_blocks(uint64_t chunkid,uint32_t version,uint16_t *blocks) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		return ERROR_WRONGVERSION;
	}
	*blocks = c->blocks;
	return STATUS_OK;
}

int create_newchunk(uint64_t chunkid,uint32_t version) {
	folder *bestfolder;
	chunk *c;
	int fd;
	uint8_t *ptr,buff[CHUNKHDRSIZE];
	uint32_t t32;
	uint64_t t64;

	c = chunk_find(chunkid);
	if (c!=NULL) {
		return ERROR_CHUNKEXIST;
	}
	bestfolder = find_best_folder();
	if (bestfolder==NULL) {
		return ERROR_NOSPACE;
	}
	c = chunk_new(bestfolder,chunkid,version);
	fd = open(c->filename,O_CREAT | O_TRUNC | O_WRONLY,0666);
	if (fd<0) {
		syslog(LOG_WARNING,"create_newchunk: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	memset(buff,0,CHUNKHDRSIZE);
	memcpy(buff,"MFSC 1.0",8);
	ptr = buff+8;
	t64 = chunkid;
	PUT64BIT(t64,ptr);
	t32 = version;
	PUT32BIT(t32,ptr);
	if (write(fd,buff,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"create_newchunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
		close(fd);
		unlink(c->filename);
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	stats_opw++;
	stats_bytesw+=CHUNKHDRSIZE;
	if (close(fd)<0) {
		syslog(LOG_WARNING,"create_newchunk: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
		unlink(c->filename);
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	return STATUS_OK;
}

int chunk_readcrc(chunk *c) {
	int ret;
	c->crc = (uint8_t*)malloc(4096);
#ifdef USE_PIO
	ret = pread(c->fd,c->crc,4096,CHUNKHDRCRC);
#else
	lseek(c->fd,CHUNKHDRCRC,SEEK_SET);
	ret = read(c->fd,c->crc,4096);
#endif
	stats_opr++;
	stats_bytesr+=4096;
	if (ret!=4096) {
		free(c->crc);
		c->crc = NULL;
		return ERROR_IO;
	}
	return STATUS_OK;
}

void chunk_freecrc(chunk *c) {
	free(c->crc);
	c->crc = NULL;
}

int chunk_writecrc(chunk *c) {
	int ret;
	c->owner->needrefresh = 1;
#ifdef USE_PIO
	ret = pwrite(c->fd,c->crc,4096,CHUNKHDRCRC);
#else
	lseek(c->fd,CHUNKHDRCRC,SEEK_SET);
	ret = write(c->fd,c->crc,4096);
#endif
	stats_opw++;
	stats_bytesw+=4096;
	if (ret!=4096) {
		chunk_freecrc(c);
		return ERROR_IO;
	}
	chunk_freecrc(c);
	return STATUS_OK;
}

void hdd_flush_crc() {
	chunkwcrc **ccp,*cc;
	chunk *c;
	ccp = &chunkswithcrc;
	while ((cc=*ccp)) {
		c = chunk_find(cc->chunkid);
		if (c) {
			if (c->crcchanged) {
				chunk_writecrc(c);
			} else {
				chunk_freecrc(c);
			}
			close(c->fd);
			c->fd=-1;
			*ccp = cc->next;
			free(cc);
		}
	}
}

void hdd_check_crc() {
	chunkwcrc **ccp,*cc;
	chunk *c;
	int status;
	uint32_t now = main_time();
	ccp = &chunkswithcrc;
	while ((cc=*ccp)) {
		c = chunk_find(cc->chunkid);
		if (c && c->crcrefcount==0 && c->lastactivity+CLOSEDELAY<now) {
			if (c->crcchanged) {
				status = chunk_writecrc(c);
				if (status!=STATUS_OK) {
					syslog(LOG_WARNING,"hdd_check_crc: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
					hdd_error_occured(c);
					masterconn_send_chunk_damaged(c->chunkid);
				}
			} else {
				chunk_freecrc(c);
			}
			if (close(c->fd)<0) {
				syslog(LOG_WARNING,"hdd_check_crc: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
				c->fd = -1;
				hdd_error_occured(c);
				masterconn_send_chunk_damaged(c->chunkid);
			}
			c->fd = -1;
			*ccp = cc->next;
			free(cc);
		} else if (c==NULL) {
			*ccp = cc->next;
			free(cc);
		} else {
			ccp = &(cc->next);
		}
	}
}

int chunk_before_io_int(chunk *c) {
	chunkwcrc *cc;
	int status;
//	syslog(LOG_NOTICE,"chunk: %"PRIu64" - before io",c->chunkid);
	if (c->crcrefcount==0) {
		if (c->crc==NULL) {
			c->fd = open(c->filename,O_RDWR);
			if (c->fd<0) {
				syslog(LOG_WARNING,"chunk_before_io_int: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
				return ERROR_IO;	//read error - mark chunk as bad
			}
			status = chunk_readcrc(c);
			if (status!=STATUS_OK) {
				close(c->fd);
				c->fd=-1;
				syslog(LOG_WARNING,"chunk_before_io_int: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
				return status;
			}
			c->crcchanged=0;
			cc = malloc(sizeof(chunkwcrc));
			cc->chunkid = c->chunkid;
			cc->next = chunkswithcrc;
			chunkswithcrc = cc;
		}
	}
	c->crcrefcount++;
	return STATUS_OK;
}

int chunk_after_io_int(chunk *c) {
//	int status;
//	syslog(LOG_NOTICE,"chunk: %"PRIu64" - after io",c->chunkid);
	c->crcrefcount--;
	if (c->crcrefcount==0) {
		c->lastactivity = main_time();
/*
		if (c->crcchanged) {
			status = chunk_writecrc(c);
			if (status!=STATUS_OK) {
				syslog(LOG_WARNING,"chunk_after_io_int: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
				return status;
			}
		} else {
			chunk_freecrc(c);
		}
		if (close(c->fd)<0) {
			syslog(LOG_WARNING,"chunk_after_io_int: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
			c->fd = -1;
			return ERROR_IO;
		}
		c->fd = -1;
*/
	}
	return STATUS_OK;
}

int chunk_before_io(uint64_t chunkid) {
	int status;
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	status = chunk_before_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		masterconn_send_chunk_damaged(chunkid);
	}
	return status;
}

int chunk_after_io(uint64_t chunkid) {
	int status;
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	status = chunk_after_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		masterconn_send_chunk_damaged(chunkid);
	}
	return status;
}

int get_chunk_checksum(uint64_t chunkid, uint32_t version, uint32_t *checksum) {
	int status;
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version != version && version>0) {
		return ERROR_WRONGVERSION;
	}
	status = chunk_before_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		masterconn_send_chunk_damaged(chunkid);
		return status;
	}
	*checksum = crc32(0,c->crc,4096);
	status = chunk_after_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		masterconn_send_chunk_damaged(chunkid);
		return status;
	}
	return STATUS_OK;
}

int get_chunk_checksum_tab(uint64_t chunkid, uint32_t version, uint8_t *checksum_tab) {
	int status;
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version != version && version>0) {
		return ERROR_WRONGVERSION;
	}
	status = chunk_before_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		masterconn_send_chunk_damaged(chunkid);
		return status;
	}
	memcpy(checksum_tab,c->crc,4096);
	status = chunk_after_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		masterconn_send_chunk_damaged(chunkid);
		return status;
	}
	return STATUS_OK;
}

uint64_t get_msectime() {
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return ((uint64_t)(tv.tv_sec))*1000000+tv.tv_usec;
}

int read_block_from_chunk(uint64_t chunkid, uint32_t version,uint16_t blocknum, uint8_t *buffer, uint32_t offset,uint32_t size,uint32_t *crc) {
	chunk *c;
	int ret;
	uint8_t *crcptr;
	uint32_t bcrc;
	uint64_t ts,te;
	*crc = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version != version && version>0) {
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=0x400) {
		return ERROR_BNUMTOOBIG;
	}
	if (size>0x10000) {
		return ERROR_WRONGSIZE;
	}
	if ((offset>=0x10000) || (offset+size>0x10000)) {
		return ERROR_WRONGOFFSET;
	}
	if (blocknum>=c->blocks) {
		memset(buffer,0,size);
		if (size==0x10000) {
			*crc = emptyblockcrc;
		} else {
			*crc = crc32(0,buffer,size);
		}
		return STATUS_OK;
	}
	if (offset==0 && size==0x10000) {
		ts = get_msectime();
#ifdef USE_PIO
		ret = pread(c->fd,buffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
		ret = read(c->fd,buffer,0x10000);
#endif
		te = get_msectime();
		stats_dataopr++;
		stats_databytesr+=0x10000;
		stats_rtime+=(te-ts);
		*crc = crc32(0,buffer,0x10000);
		crcptr = (c->crc)+(4*blocknum);
		GET32BIT(bcrc,crcptr);
		if (bcrc!=*crc) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_CRC;
		}
		if (ret!=0x10000) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_IO;
		}
	} else {
		ts = get_msectime();
#ifdef USE_PIO
		ret = pread(c->fd,blockbuffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
		ret = read(c->fd,blockbuffer,0x10000);
#endif
		te = get_msectime();
		stats_dataopr++;
		stats_databytesr+=0x10000;
		stats_rtime+=(te-ts);
		*crc = crc32(0,blockbuffer+offset,size);	// first calc crc for piece
		crcptr = (c->crc)+(4*blocknum);
		GET32BIT(bcrc,crcptr);
		if (bcrc!=crc32(0,blockbuffer,0x10000)) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_CRC;
		}
		if (ret!=0x10000) {
			syslog(LOG_WARNING,"read_block_from_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_IO;
		}
		memcpy(buffer,blockbuffer+offset,size);
	}
	return STATUS_OK;
}

int write_block_to_chunk(uint64_t chunkid, uint32_t version,uint16_t blocknum, uint8_t *buffer, uint32_t offset,uint32_t size,uint32_t crc) {
	chunk *c;
	int ret;
	uint8_t *crcptr;
	uint32_t bcrc;
	uint32_t i;
	uint64_t ts,te;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version != version && version>0) {
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=0x400) {
		return ERROR_BNUMTOOBIG;
	}
	if (size>0x10000) {
		return ERROR_WRONGSIZE;
	}
	if ((offset>=0x10000) || (offset+size>0x10000)) {
		return ERROR_WRONGOFFSET;
	}
	if (crc!=crc32(0,buffer,size)) {
		return ERROR_CRC;
	}
	if (offset==0 && size==0x10000) {
		if (blocknum>=c->blocks) {
			crcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				PUT32BIT(emptyblockcrc,crcptr);
			}
			c->blocks=blocknum+1;
		}
		ts = get_msectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,buffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
		ret = write(c->fd,buffer,0x10000);
#endif
		te = get_msectime();
		stats_dataopw++;
		stats_databytesw+=0x10000;
		stats_wtime+=(te-ts);
		if (crc!=crc32(0,buffer,0x10000)) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_CRC;
		}
		crcptr = (c->crc)+(4*blocknum);
		PUT32BIT(crc,crcptr);
		c->crcchanged=1;
		if (ret!=0x10000) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_IO;
		}
	} else {
		if (blocknum<c->blocks) {
			ts = get_msectime();
#ifdef USE_PIO
			ret = pread(c->fd,blockbuffer,0x10000,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16));
#else
			lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16),SEEK_SET);
			ret = read(c->fd,blockbuffer,0x10000);
#endif
			te = get_msectime();
			stats_dataopr++;
			stats_databytesr+=0x10000;
			stats_rtime+=(te-ts);
			if (ret!=0x10000) {
				syslog(LOG_WARNING,"write_block_to_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_error_occured(c);
				masterconn_send_chunk_damaged(chunkid);
				return ERROR_IO;
			}
			crcptr = (c->crc)+(4*blocknum);
			GET32BIT(bcrc,crcptr);
			if (bcrc!=crc32(0,blockbuffer,0x10000)) {
				syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
				hdd_error_occured(c);
				masterconn_send_chunk_damaged(chunkid);
				return ERROR_CRC;
			}
		} else {
			if (ftruncate(c->fd,CHUNKHDRSIZE+(((uint32_t)(blocknum+1))<<16))<0) {
				syslog(LOG_WARNING,"write_block_to_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
				hdd_error_occured(c);
				masterconn_send_chunk_damaged(chunkid);
				return ERROR_IO;
			}
			crcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				PUT32BIT(emptyblockcrc,crcptr);
			}
			c->blocks=blocknum+1;
			memset(blockbuffer,0,0x10000);
		}
		memcpy(blockbuffer+offset,buffer,size);
		ts = get_msectime();
#ifdef USE_PIO
		ret = pwrite(c->fd,blockbuffer+offset,size,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset);
#else
		lseek(c->fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset,SEEK_SET);
		ret = write(c->fd,blockbuffer+offset,size);
#endif
		te = get_msectime();
		stats_dataopw++;
		stats_databytesw+=size;
		stats_wtime+=(te-ts);
		crcptr = (c->crc)+(4*blocknum);
		bcrc = crc32(0,blockbuffer,0x10000);
		PUT32BIT(bcrc,crcptr);
		c->crcchanged=1;
		if (crc!=crc32(0,blockbuffer+offset,size)) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - crc error",c->filename);
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_CRC;
		}
		if (ret!=(int)size) {
			syslog(LOG_WARNING,"write_block_to_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_IO;
		}
	}
	return STATUS_OK;
}



// OLD VERSION - WITHOUT CRC
/*
int read_block_from_chunk(uint64_t chunkid, uint32_t version,uint16_t blocknum, uint8_t *buffer, uint32_t offset,uint32_t size,uint32_t *crc) {
	chunk *c;
	int fd,ret;
	*crc = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version != version && version>0) {
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=0x400) {
		return ERROR_BNUMTOOBIG;
	}
	if (size>0x10000) {
		return ERROR_WRONGSIZE;
	}
	if ((offset>=0x10000) || (offset+size>0x10000)) {
		return ERROR_WRONGOFFSET;
	}
	if (blocknum>=c->blocks) {
		memset(buffer,0,size);
		return STATUS_OK;
	}
	fd = open(c->filename,O_RDONLY);
	if (fd<0) {
		masterconn_send_chunk_damaged(chunkid);
		return ERROR_IO;	//read error - mark chunk as bad
	}
#ifdef USE_PIO
	ret = pread(fd,buffer,size,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset);
#else
	lseek(fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset,SEEK_SET);
	ret = read(fd,buffer,size);
#endif
	close(fd);
	if (ret!=(int)size) {
		masterconn_send_chunk_damaged(chunkid);
		return ERROR_IO;	//read error - mark chunk as bad
	}
	*crc = crc32(0,buffer,size);
	return STATUS_OK;
}

int write_block_to_chunk(uint64_t chunkid, uint32_t version,uint16_t blocknum, uint8_t *buffer, uint32_t offset,uint32_t size,uint32_t crc) {
	chunk *c;
	int fd,ret;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version != version && version>0) {
		return ERROR_WRONGVERSION;
	}
	if (blocknum>=0x400) {
		return ERROR_BNUMTOOBIG;
	}
	if (size>0x10000) {
		return ERROR_WRONGSIZE;
	}
	if ((offset>=0x10000) || (offset+size>0x10000)) {
		return ERROR_WRONGOFFSET;
	}
	if (crc!=crc32(0,buffer,size)) {
		return ERROR_CRC;
	}
	fd = open(c->filename,O_WRONLY);
	if (fd<0) {
		masterconn_send_chunk_damaged(chunkid);
		return ERROR_IO;	//write error
	}
#ifdef USE_PIO
	ret = pwrite(fd,buffer,size,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset);
#else
	lseek(fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+offset,SEEK_SET);
	ret = write(fd,buffer,size);
#endif
	if (size+offset<0x10000) {	// increase file size up to 64k boundary
#ifdef USE_PIO
		if (pwrite(fd,"\0",1,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+0xFFFF)!=1) {
#else
		lseek(fd,CHUNKHDRSIZE+(((uint32_t)blocknum)<<16)+0xFFFF,SEEK_SET);
		if (write(fd,"\0",1)!=1) {
#endif
			close(fd);
			masterconn_send_chunk_damaged(chunkid);
			return ERROR_IO;
		}
	}
	close(fd);
	if (ret!=(int)size) {
		masterconn_send_chunk_damaged(chunkid);
		return ERROR_IO;	//write error
	}
	if (blocknum>=c->blocks) {
		c->owner->usedspace+=(1+blocknum-c->blocks)*0x10000;
		c->owner->freespace-=(1+blocknum-c->blocks)*0x10000;
		c->blocks=blocknum+1;
	}
	return STATUS_OK;
}
*/

int duplicate_chunk(uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion) {
	folder *bestfolder;
	uint8_t *ptr;
	uint16_t block;
	int32_t retsize;
	int ofd,fd;
	chunk *c,*oc;
	oc = chunk_find(oldchunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->version!=oldversion && oldversion>0) {
		return ERROR_WRONGVERSION;
	}
	if (version==0) {
		version=oc->version;
	}
	c = chunk_find(chunkid);
	if (c!=NULL) {
		return ERROR_CHUNKEXIST;
	}
	bestfolder = find_best_folder();
	if (bestfolder==NULL) {
		return ERROR_NOSPACE;
	}
	c = chunk_new(bestfolder,chunkid,version);
	fd = open(c->filename,O_CREAT | O_TRUNC | O_WRONLY,0666);
	if (fd<0) {
		syslog(LOG_WARNING,"duplicate_chunk: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	ofd = open(oc->filename,O_RDONLY);
	if (ofd<0) {
		syslog(LOG_WARNING,"duplicate_chunk: file:%s - open error (%d:%s)",oc->filename,errno,strerror(errno));
		close(fd);
		unlink(c->filename);
		hdd_error_occured(oc);
		masterconn_send_chunk_damaged(oldchunkid);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	//copy header
	if (read(ofd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"duplicate_chunk: file:%s - hdr read error (%d:%s)",oc->filename,errno,strerror(errno));
		close(ofd);
		close(fd);
		unlink(c->filename);
		chunk_delete(chunkid);
		hdd_error_occured(oc);
		masterconn_send_chunk_damaged(oldchunkid);
		// in the future - change status of oc to 'bad'
		return ERROR_IO;
	}
	stats_opr++;
	stats_bytesr+=CHUNKHDRSIZE;
	ptr = hdrbuffer+8;
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	if (write(fd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"duplicate_chunk: file:%s - hdr write error (%d:%s)",c->filename,errno,strerror(errno));
		close(ofd);
		close(fd);
		unlink(c->filename);
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	stats_opw++;
	stats_bytesw+=CHUNKHDRSIZE;
	for (block=0 ; block<oc->blocks ; block++) {
		retsize = read(ofd,blockbuffer,0x10000);
		if (retsize!=0x10000) {
			syslog(LOG_WARNING,"duplicate_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
			close(fd);
			close(ofd);
			unlink(c->filename);
			hdd_error_occured(oc);
			masterconn_send_chunk_damaged(oldchunkid);
			chunk_delete(chunkid);
			return ERROR_IO;	//read error - in the future - change status of oc to 'bad'
		}
		stats_opr++;
		stats_bytesr+=0x10000;
		retsize = write(fd,blockbuffer,0x10000);
		if (retsize!=0x10000) {
			syslog(LOG_WARNING,"duplicate_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
			close(fd);
			close(ofd);
			unlink(c->filename);
			hdd_error_occured(c);
			chunk_delete(chunkid);
			return ERROR_IO;	//write error
		}
		stats_opw++;
		stats_bytesw+=0x10000;
	}
	if (close(ofd)<0) {
		syslog(LOG_WARNING,"duplicate_chunk: file:%s - close error (%d:%s)",oc->filename,errno,strerror(errno));
		close(fd);
		unlink(c->filename);
		hdd_error_occured(oc);
		masterconn_send_chunk_damaged(oldchunkid);
		chunk_delete(chunkid);
		return ERROR_IO;	//read error - in the future - change status of oc to 'bad'
	}
	if (close(fd)<0) {
		syslog(LOG_WARNING,"duplicate_chunk: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
		unlink(c->filename);
		hdd_error_occured(c);
		masterconn_send_chunk_damaged(oldchunkid);
		chunk_delete(chunkid);
		return ERROR_IO;	//read error - in the future - change status of oc to 'bad'
	}
	c->blocks = oc->blocks;
	//c->lengthchanged = oc->lengthchanged;
	return STATUS_OK;
}

int set_chunk_version(uint64_t chunkid,uint32_t version,uint32_t oldversion) {
	int fd;
	uint32_t filenameleng;
	char *newfilename;
	uint8_t *ptr,vbuff[4];
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=oldversion && oldversion>0) {
		return ERROR_WRONGVERSION;
	}
	filenameleng = strlen(c->filename);
	if (c->filename[filenameleng-13]=='_') {	// new file name format
		newfilename = malloc(filenameleng+1);
		if (newfilename==NULL) {
			return ERROR_OUTOFMEMORY;
		}
		memcpy(newfilename,c->filename,filenameleng+1);
		sprintf(newfilename+filenameleng-12,"%08"PRIX32".mfs",version);
		if (rename(c->filename,newfilename)<0) {
			syslog(LOG_WARNING,"set_chunk_version: file:%s - rename error (%d:%s)",c->filename,errno,strerror(errno));
			hdd_error_occured(c);
			free(newfilename);
			return ERROR_IO;
		}
		free(c->filename);
		c->filename = newfilename;
	}
	c->version = version;
	fd = open(c->filename,O_WRONLY);
	if (fd<0) {
		syslog(LOG_WARNING,"set_chunk_version: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		return ERROR_IO;	//can't change file version
	}
	ptr = vbuff;
	PUT32BIT(version,ptr);
#ifdef USE_PIO
	if (pwrite(fd,vbuff,4,16)!=4) {
#else
	lseek(fd,16,SEEK_SET);
	if (write(fd,vbuff,4)!=4) {
#endif
		syslog(LOG_WARNING,"set_chunk_version: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
		close(fd);
		hdd_error_occured(c);
		return ERROR_IO;	//can't change file version
	}
	stats_opw++;
	stats_bytesw+=4;
	if (close(fd)<0) {
		syslog(LOG_WARNING,"set_chunk_version: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		return ERROR_IO;	//can't change file version
	}
	return STATUS_OK;
}

int truncate_chunk(uint64_t chunkid,uint32_t length,uint32_t version,uint32_t oldversion) {
	int fd;
	uint32_t filenameleng;
	char *newfilename;
	uint8_t *ptr,vbuff[4];
	chunk *c;
	uint32_t blocks;
	uint32_t i;
	int status;
	c = chunk_find(chunkid);
	if (length>0x4000000) {
		return ERROR_WRONGSIZE;
	}
	// step 1 - change version
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=oldversion && oldversion>0) {
		return ERROR_WRONGVERSION;
	}
	filenameleng = strlen(c->filename);
	if (c->filename[filenameleng-13]=='_') {	// new file name format
		newfilename = malloc(filenameleng+1);
		if (newfilename==NULL) {
			return ERROR_OUTOFMEMORY;
		}
		memcpy(newfilename,c->filename,filenameleng+1);
		sprintf(newfilename+filenameleng-12,"%08"PRIX32".mfs",version);
		if (rename(c->filename,newfilename)<0) {
			syslog(LOG_WARNING,"truncate_chunk: file:%s - rename error (%d:%s)",c->filename,errno,strerror(errno));
			free(newfilename);
			hdd_error_occured(c);
			return ERROR_IO;
		}
		free(c->filename);
		c->filename = newfilename;
	}
	c->version = version;
	fd = open(c->filename,O_WRONLY);
	if (fd<0) {
		syslog(LOG_WARNING,"truncate_chunk: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		return ERROR_IO;	//can't change file version
	}
	ptr = vbuff;
	PUT32BIT(version,ptr);
#ifdef USE_PIO
	if (pwrite(fd,vbuff,4,16)!=4) {
#else
	lseek(fd,16,SEEK_SET);
	if (write(fd,vbuff,4)!=4) {
#endif
		syslog(LOG_WARNING,"truncate_chunk: file:%s - write error (%d:%s)",c->filename,errno,strerror(errno));
		close(fd);
	}
	stats_opw++;
	stats_bytesw+=4;
	if (close(fd)<0) {
		syslog(LOG_WARNING,"truncate_chunk: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		return ERROR_IO;	//can't change file version
	}
	// step 2. truncate
	status = chunk_before_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
		return status;
	}
	blocks = ((length+0xFFFF)>>16);
	if (blocks>c->blocks) {
		if (ftruncate(c->fd,CHUNKHDRSIZE+(blocks<<16))<0) {
			syslog(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
			chunk_after_io_int(c);
			hdd_error_occured(c);
			return ERROR_IO;
		}
		ptr = (c->crc)+(4*(c->blocks));
		for (i=c->blocks ; i<blocks ; i++) {
			PUT32BIT(emptyblockcrc,ptr);
		}
		c->crcchanged=1;
	} else {
		uint32_t blocknum = length>>16;
		uint32_t blockpos = length&0x3FF0000;
		uint32_t blocksize = length&0xFFFF;
		if (ftruncate(c->fd,CHUNKHDRSIZE+length)<0) {
			syslog(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
			chunk_after_io_int(c);
			hdd_error_occured(c);
			return ERROR_IO;
		}
		if (blocksize>0) {
			if (ftruncate(c->fd,CHUNKHDRSIZE+(blocks<<16))<0) {
				syslog(LOG_WARNING,"truncate_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
				chunk_after_io_int(c);
				hdd_error_occured(c);
				return ERROR_IO;
			}
#ifdef USE_PIO
			if (pread(c->fd,blockbuffer,blocksize,CHUNKHDRSIZE+blockpos)!=(signed)blocksize) {
#else
			lseek(c->fd,CHUNKHDRSIZE+blockpos,SEEK_SET);
			if (read(c->fd,blockbuffer,blocksize)!=(signed)blocksize) {
#endif
				syslog(LOG_WARNING,"truncate_chunk: file:%s - read error (%d:%s)",c->filename,errno,strerror(errno));
				chunk_after_io_int(c);
				hdd_error_occured(c);
				return ERROR_IO;
			}
			stats_opr++;
			stats_bytesr+=blocksize;
			memset(blockbuffer+blocksize,0,0x10000-blocksize);

			ptr = (c->crc)+(4*blocknum);
			i = crc32(0,blockbuffer,0x10000);
			PUT32BIT(i,ptr);
			c->crcchanged=1;
		}
	}
	if (c->blocks != blocks) {
		c->owner->needrefresh = 1;
	}
	c->blocks=blocks;
	status = chunk_after_io_int(c);
	if (status!=STATUS_OK) {
		hdd_error_occured(c);
	}
	return status;
}

int duptrunc_chunk(uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion,uint32_t length) {
	folder *bestfolder;
	uint8_t *ptr;
	uint16_t block;
	uint16_t blocks; 
	int32_t retsize;
	uint32_t crc;
	int ofd,fd;
	chunk *c,*oc;
	if (length>0x4000000) {
		return ERROR_WRONGSIZE;
	}
	oc = chunk_find(oldchunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->version!=oldversion && oldversion>0) {
		return ERROR_WRONGVERSION;
	}
	if (version==0) {
		version=oc->version;
	}
	c = chunk_find(chunkid);
	if (c!=NULL) {
		return ERROR_CHUNKEXIST;
	}
	bestfolder = find_best_folder();
	if (bestfolder==NULL) {
		return ERROR_NOSPACE;
	}
	c = chunk_new(bestfolder,chunkid,version);
	fd = open(c->filename,O_CREAT | O_TRUNC | O_WRONLY,0666);
	if (fd<0) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - open error (%d:%s)",c->filename,errno,strerror(errno));
		chunk_delete(chunkid);
		hdd_error_occured(oc);
		return ERROR_IO;
	}
	ofd = open(oc->filename,O_RDONLY);
	if (ofd<0) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - open error (%d:%s)",oc->filename,errno,strerror(errno));
		close(fd);
		unlink(c->filename);
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	blocks = ((length+0xFFFF)>>16);
	//copy header
	if (read(ofd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - hdr read error (%d:%s)",oc->filename,errno,strerror(errno));
		close(ofd);
		close(fd);
		unlink(c->filename);
		chunk_delete(chunkid);
		hdd_error_occured(oc);
		masterconn_send_chunk_damaged(oldchunkid);
		return ERROR_IO;
	}
	stats_opr++;
	stats_bytesr+=CHUNKHDRSIZE;
	ptr = hdrbuffer+8;
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	lseek(fd,CHUNKHDRSIZE,SEEK_SET);
	if (blocks>oc->blocks) { // expanding
		for (block=0 ; block<oc->blocks ; block++) {
			retsize = read(ofd,blockbuffer,0x10000);
			if (retsize!=0x10000) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
				close(fd);
				close(ofd);
				unlink(c->filename);
				hdd_error_occured(oc);
				masterconn_send_chunk_damaged(oldchunkid);
				chunk_delete(chunkid);
				return ERROR_IO;	//read error - in the future - change status of oc to 'bad'
			}
			stats_opr++;
			stats_bytesr+=0x10000;
			retsize = write(fd,blockbuffer,0x10000);
			if (retsize!=0x10000) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
				close(fd);
				close(ofd);
				unlink(c->filename);
				hdd_error_occured(c);
				chunk_delete(chunkid);
				return ERROR_IO;	//write error
			}
			stats_opw++;
			stats_bytesw+=0x10000;
		}
		if (ftruncate(fd,CHUNKHDRSIZE+(((uint32_t)blocks)<<16))<0) {
			syslog(LOG_WARNING,"duptrunc_chunk: file:%s - ftruncate error (%d:%s)",c->filename,errno,strerror(errno));
			close(fd);
			close(ofd);
			unlink(c->filename);
			hdd_error_occured(c);
			chunk_delete(chunkid);
			return ERROR_IO;	//write error
		}
		ptr = hdrbuffer+CHUNKHDRCRC+4*(oc->blocks);
		for (block=oc->blocks ; block<blocks ; block++) {
			PUT32BIT(emptyblockcrc,ptr);
		}
	} else { // shrinking
		uint32_t blocksize = (length&0xFFFF);
		if (blocksize==0) { // aligned shring
			for (block=0 ; block<blocks ; block++) {
				retsize = read(ofd,blockbuffer,0x10000);
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
					close(fd);
					close(ofd);
					unlink(c->filename);
					hdd_error_occured(oc);
					masterconn_send_chunk_damaged(oldchunkid);
					chunk_delete(chunkid);
					return ERROR_IO;	//read error - in the future - change status of oc to 'bad'
				}
				stats_opr++;
				stats_bytesr+=0x10000;
				retsize = write(fd,blockbuffer,0x10000);
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
					close(fd);
					close(ofd);
					unlink(c->filename);
					hdd_error_occured(c);
					chunk_delete(chunkid);
					return ERROR_IO;	//write error
				}
				stats_opw++;
				stats_bytesw+=0x10000;
			}
		} else { // misaligned shrink
			for (block=0 ; block<blocks-1 ; block++) {
				retsize = read(ofd,blockbuffer,0x10000);
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
					close(fd);
					close(ofd);
					unlink(c->filename);
					hdd_error_occured(oc);
					masterconn_send_chunk_damaged(oldchunkid);
					chunk_delete(chunkid);
					return ERROR_IO;	//read error - in the future - change status of oc to 'bad'
				}
				stats_opr++;
				stats_bytesr+=0x10000;
				retsize = write(fd,blockbuffer,0x10000);
				if (retsize!=0x10000) {
					syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
					close(fd);
					close(ofd);
					unlink(c->filename);
					hdd_error_occured(c);
					chunk_delete(chunkid);
					return ERROR_IO;	//write error
				}
				stats_opw++;
				stats_bytesw+=0x10000;
			}
			retsize = read(ofd,blockbuffer,blocksize);
			if (retsize!=(signed)blocksize) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data read error (%d:%s)",oc->filename,errno,strerror(errno));
				close(fd);
				close(ofd);
				unlink(c->filename);
				hdd_error_occured(oc);
				masterconn_send_chunk_damaged(oldchunkid);
				chunk_delete(chunkid);
				return ERROR_IO;
			}
			stats_opr++;
			stats_bytesr+=blocksize;
			memset(blockbuffer+blocksize,0,0x10000-blocksize);
			retsize = write(fd,blockbuffer,0x10000);
			if (retsize!=0x10000) {
				syslog(LOG_WARNING,"duptrunc_chunk: file:%s - data write error (%d:%s)",c->filename,errno,strerror(errno));
				close(fd);
				close(ofd);
				unlink(c->filename);
				hdd_error_occured(c);
				chunk_delete(chunkid);
				return ERROR_IO;	//write error
			}
			stats_opw++;
			stats_bytesw+=0x10000;
			ptr = hdrbuffer+CHUNKHDRCRC+4*(blocks-1);
			crc = crc32(0,blockbuffer,0x10000);
			PUT32BIT(crc,ptr);
		}
	}
	lseek(fd,0,SEEK_SET);
	if (write(fd,hdrbuffer,CHUNKHDRSIZE)!=CHUNKHDRSIZE) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - hdr write error (%d:%s)",c->filename,errno,strerror(errno));
		close(ofd);
		close(fd);
		unlink(c->filename);
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	stats_opw++;
	stats_bytesw+=CHUNKHDRSIZE;
	if (close(ofd)<0) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - close error (%d:%s)",oc->filename,errno,strerror(errno));
		close(fd);
		unlink(c->filename);
		hdd_error_occured(oc);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	if (close(fd)<0) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - close error (%d:%s)",c->filename,errno,strerror(errno));
		unlink(c->filename);
		hdd_error_occured(c);
		chunk_delete(chunkid);
		return ERROR_IO;
	}
	c->blocks = blocks;
	return STATUS_OK;
}

int delete_chunk(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		return ERROR_WRONGVERSION;
	}
	if (unlink(c->filename)<0) {
		syslog(LOG_WARNING,"duptrunc_chunk: file:%s - unlink error (%d:%s)",c->filename,errno,strerror(errno));
		hdd_error_occured(c);
		return ERROR_IO;
	}
	chunk_delete(chunkid);
	return STATUS_OK;
}


static void hdd_folder_scan(folder *f) {
	DIR *dd;
	struct dirent *de;
	struct stat sb;
	int fd;
	uint8_t subf;
	chunk *c;
	char *fullname,*oldfullname;
	int nameok;
	char ch;
	uint32_t i;
	uint8_t plen;
	uint32_t hashpos;
//	uint8_t *ptr,buff[1024];
	uint64_t namechunkid;
	uint32_t nameversion;

	plen = strlen(f->path);
	fullname = malloc(plen+38);
	oldfullname = malloc(plen+29);

	memcpy(fullname,f->path,plen);
	fullname[plen]='\0';
	mkdir(fullname,0755);

	fullname[plen++]='_';
	fullname[plen++]='/';
	fullname[plen]='\0';
	for (subf=0 ; subf<16 ; subf++) {
		if (subf<10) {
			fullname[plen-2]='0'+subf;
		} else {
			fullname[plen-2]='A'+(subf-10);
		}
		fullname[plen]='\0';
		mkdir(fullname,0755);
		dd = opendir(fullname);
		if (dd==NULL) {
			continue;
		}
		while ((de = readdir(dd)) != NULL) {
#ifdef HAVE_STRUCT_DIRENT_D_TYPE
			if (de->d_type != DT_REG) {
				continue;
			}
#endif
			// old name format: "chunk_XXXXXXXXXXXXXXXX.mfs"
			// new name format: "chunk_XXXXXXXXXXXXXXXX_YYYYYYYY.mfs" (X - chunkid ; Y - version)
			if (strncmp(de->d_name,"chunk_",6)!=0) {
				continue;
			}
			namechunkid = 0;
			nameversion = 0;
			nameok=1;
			for (i=6 ; i<22 && nameok==1 ; i++) {
				ch = de->d_name[i];
				if (ch>='0' && ch<='9') {
					ch-='0';
				} else if (ch>='A' && ch<='F') {
					ch-='A'-10;
				} else {
					ch=0;
					nameok=0;
				}
				namechunkid*=16;
				namechunkid+=ch;
			}
			if (nameok==0) {
				continue;
			}
			if (de->d_name[22]=='_') {	// new name
				for (i=23 ; i<31 && nameok==1 ; i++) {
					ch = de->d_name[i];
					if (ch>='0' && ch<='9') {
						ch-='0';
					} else if (ch>='A' && ch<='F') {
						ch-='A'-10;
					} else {
						ch=0;
						nameok=0;
					}
					nameversion*=16;
					nameversion+=ch;
				}
				if (nameok==0) {
					continue;
				}
				if (strcmp(de->d_name+31,".mfs")!=0) {
					continue;
				}
				nameok = 2;
			} else if (de->d_name[22]=='.') {	// old name
				if (strcmp(de->d_name+23,"mfs")!=0) {
					continue;
				}
			} else {
				continue;
			}
			if (nameok==2) {
				memcpy(fullname+plen,de->d_name,36);
			} else {
				uint8_t *ptr,hdr[20];
				uint64_t t64;
				memcpy(oldfullname,fullname,plen);
				memcpy(oldfullname+plen,de->d_name,27);
				fd = open(oldfullname,O_RDONLY);
				if (fd<0) {
					continue;
				}
				if (read(fd,hdr,20)!=20) {
					close(fd);
					unlink(oldfullname);
					continue;
				}
				close(fd);
				if (memcmp(hdr,"MFSC 1.0",8)!=0) {
					unlink(oldfullname);
					continue;
				}
				ptr = hdr+8;
				GET64BIT(t64,ptr);
				GET32BIT(nameversion,ptr);
				if (t64!=namechunkid) {
					unlink(fullname);
					continue;
				}
				sprintf(fullname+plen,"chunk_%016"PRIX64"_%08"PRIX32".mfs",namechunkid,nameversion);
				if (rename(oldfullname,fullname)<0) {
					syslog(LOG_WARNING,"can't rename %s to %s: %m",oldfullname,fullname);
					memcpy(fullname+plen,oldfullname+plen,27);
				}
			}
			if (stat(fullname,&sb)<0) {
				unlink(fullname);
				continue;
			}
			if (access(fullname,R_OK | W_OK)<0) {
				syslog(LOG_WARNING,"access to file: %s: %m",fullname);
				continue;
			}
			if (sb.st_size<CHUNKHDRSIZE || sb.st_size>(CHUNKHDRSIZE+0x4000000) || ((sb.st_size-CHUNKHDRSIZE)&0xFFFF)!=0) {
				unlink(fullname);	// remove wrong chunk
				continue;
			}
			hashpos = HASHPOS(namechunkid);
			c=NULL;
			for (c=hashtab[hashpos] ; c && c->chunkid != namechunkid ; c=c->hashnext) {}
			if (c!=NULL) {
				syslog(LOG_WARNING,"repeated chunk: %016"PRIX64,namechunkid);
				if (nameversion < c->chunkid) {
					unlink(fullname);
					continue;
				} else {
					unlink(c->filename);
					free(c->filename);
				}
			} else {
				c = (chunk*)malloc(sizeof(chunk));
				c->chunkid = namechunkid;
				c->crcrefcount = 0;
				c->crcchanged = 0;
				c->crc = NULL;
				c->fd = -1;
				c->hashnext = hashtab[hashpos];
				hashtab[hashpos] = c;
			}
			c->filename = strdup(fullname);
			c->version = nameversion;
			c->blocks = (sb.st_size - CHUNKHDRSIZE) / 0x10000;
			c->owner = f;
			f->chunkcount++;
		}
		closedir(dd);
	}
	free(fullname);
	free(oldfullname);
}

int hdd_init(void) {
	uint32_t l;
	int lfp,td;
	uint32_t hp;
	FILE *fd;
	char buff[1000];
	char *pptr;
	char *lockfname;
	char *hddfname;
	struct stat sb;
	folder *f,*sf;

	for (hp=0 ; hp<HASHSIZE ; hp++) {
		hashtab[hp]=NULL;
	}
	memset(blockbuffer,0,0x10000);
	emptyblockcrc = crc32(0,blockbuffer,0x10000);

	config_getnewstr("HDD_CONF_FILENAME",ETC_PATH "/mfshdd.cfg",&hddfname);

	fd = fopen(hddfname,"r");
	free(hddfname);
	if (!fd) {
		return -1;
	}
	while (fgets(buff,999,fd)) {
		buff[999]=0;
		l = strlen(buff);
		while (l>0 && (buff[l-1]=='\r' || buff[l-1]=='\n' || buff[l-1]==' ' || buff[l-1]=='\t')) {
			l--;
		}
		if (l>0) {
			if (buff[l-1]!='/') {
				buff[l]='/';
				buff[l+1]='\0';
				l++;
			} else {
				buff[l]='\0';
			}
			if (buff[0]=='*') {
				td = 1;
				pptr = buff+1;
				l--;
			} else {
				td = 0;
				pptr = buff;
			}
			lockfname = (char*)malloc(l+6);
			memcpy(lockfname,pptr,l);
			memcpy(lockfname+l,".lock",6);
			lfp=open(lockfname,O_RDWR|O_CREAT|O_TRUNC,0640);
			if (lfp<0) {
				syslog(LOG_ERR,"can't create lock file '%s': %m",lockfname);
				return -1;
			}
			if (lockf(lfp,F_TLOCK,0)<0) {
				if (errno==EAGAIN) {
					syslog(LOG_ERR,"data folder '%s' already locked (used by another process)",pptr);
				} else {
					syslog(LOG_NOTICE,"lockf '%s' error: %m",lockfname);
				}
				return -1;
			}
			if (fstat(lfp,&sb)<0) {
				syslog(LOG_NOTICE,"fstat '%s' error: %m",lockfname);
				return -1;
			}
			for (sf=folderhead ; sf ; sf=sf->next) {
				if (sf->devid==sb.st_dev) {
					if (sf->lockinode==sb.st_ino) {
						syslog(LOG_ERR,"data folder '%s' already locked (used by this process)",pptr);
						return -1;
					} else {
						syslog(LOG_WARNING,"data folders '%s' and '%s' are on the same physical device (could lead to unexpected behaviours)",pptr,sf->path);
					}
				}
			}
			f = (folder*)malloc(sizeof(folder));
			f->todel = td;
			f->path = strdup(pptr);
			f->leavefree = 0x10000000; // about 256MB  -  future: (uint64_t)as*0x40000000;
			f->avail = 0ULL;
			f->total = 0ULL;
			f->chunkcount = 0;
			for (l=0 ; l<LASTERRSIZE ; l++) {
				f->lasterrtab[l].chunkid = 0ULL;
				f->lasterrtab[l].timestamp = 0;
			}
			f->lasterrindx=0;
			f->needrefresh = 1;
			f->devid = sb.st_dev;
			f->lockinode = sb.st_ino;
			f->next = folderhead;
			hdd_folder_scan(f);
			hdd_refresh_usage(f);
			f->needrefresh = 0;
			folderhead = f;
		}
	}
	if (folderhead==NULL) {
		syslog(LOG_ERR,"no hdd space !!!");
		return -1;
	}
	main_timeregister(TIMEMODE_RUNONCE,1,0,hdd_send_space);
	main_timeregister(TIMEMODE_RUNONCE,10,0,hdd_check_crc);
	main_timeregister(TIMEMODE_RUNONCE,60,0,hdd_time_refresh);
	main_destructregister(hdd_flush_crc);
	return 0;
}
