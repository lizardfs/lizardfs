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

#include <inttypes.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef METARESTORE
#include <time.h>
#endif

#include "MFSCommunication.h"

#ifndef METARESTORE
#include "main.h"
#include "cfg.h"
#include "matocsserv.h"
#include "matocuserv.h"
#include "random.h"

#include "config.h"
#endif

#include "chunks.h"
#include "filesystem.h"
#include "datapack.h"

#define HASHSIZE 65536
#define HASHPOS(chunkid) (((uint32_t)chunkid)&0xFFFF)

#ifndef METARESTORE
/* chunk.operation */
enum {NONE,CREATE,SET_VERSION,DUPLICATE,TRUNCATE,DUPTRUNC};
/* slist.valid */
/* INVALID - wrong version / or got info from chunkserver (IO error etc.)  ->  to delete */
/* DEL - deletion in progress */
/* BUSY - operation in progress */
/* VALID - ok */
/* TDBUSY - to delete + BUSY */
/* TDVALID - want to be deleted */
enum {INVALID,DEL,BUSY,VALID,TDBUSY,TDVALID};

typedef struct _slist {
	void *ptr;
	uint8_t valid;
//	uint8_t sectionid; - idea - Split machines into sctions. Try to place each copy of particular chunk in different section.
//	uint16_t machineid; - idea - If there are many different processes on the same physical computer then place there only one copy of chunk.
	struct _slist *next;
} slist;
#endif

typedef struct _flist {
	uint32_t inode;
	uint16_t indx;
	uint8_t goal;
	struct _flist *next;
} flist;

typedef struct chunk {
	uint64_t chunkid;
	uint32_t version;
	uint8_t goal;
#ifndef METARESTORE
	uint8_t interrupted:1;
	uint8_t operation:4;
#endif
	uint32_t lockedto;
#ifndef METARESTORE
	void *replserv;
	slist *slisthead;
#endif
	flist *flisthead;
	struct chunk *next;
} chunk;

static chunk *chunkhash[HASHSIZE];
static uint64_t nextchunkid=1;
#define LOCKTIMEOUT 120

#ifndef METARESTORE

static uint32_t ReplicationsDelayDisconnect=3600;
static uint32_t ReplicationsDelayInit=300;

// CONFIG
// D:(MAX DELETES PER SECOND)
// R:(MAX REPLICATION PER SECOND)
// L:(LOOP TIME)

// static char* CfgFileName;
// static uint32_t MaxRepl=10;
// static uint32_t MaxDel=30;
// static uint32_t LoopTime=3600;
// static uint32_t HashSteps=1+((HASHSIZE)/3600);
static uint32_t MaxRepl;
static uint32_t MaxDel;
static uint32_t LoopTime;
static uint32_t HashSteps;

//#define MAXCOPY 2
//#define MAXDEL 6
//#define LOOPTIME 3600
//#define HASHSTEPS (1+((HASHSIZE)/(LOOPTIME)))

#define MAXDIFFERENCE 0.02
#define MINMAXRND 0.01

static uint32_t jobshpos;
//static chunk **jobscptr;
static uint32_t jobsrebalancecount;
static uint32_t jobscopycount;
static uint32_t jobsdelcount;
//static uint32_t jobsloopstart;
static uint32_t jobsnorepbefore;

typedef struct _job_info {
	uint32_t del_invalid;
	uint32_t del_unused;
	uint32_t del_diskclean;
	uint32_t del_overgoal;
	uint32_t copy_undergoal;
} job_info;

typedef struct _loop_info {
	job_info done,notdone;
	uint32_t copy_rebalance;
} loop_info;

static loop_info chunksinfo = {{0,0,0,0,0},{0,0,0,0,0},0};
static uint32_t chunksinfo_loopstart=0,chunksinfo_loopend=0;

#endif

static uint64_t lastchunkid=0;
static chunk* lastchunkptr=NULL;

static uint32_t chunks;
#ifndef METARESTORE
static uint32_t todelchunks;
#endif

#ifndef METARESTORE
static uint32_t stats_deletions=0;
static uint32_t stats_replications=0;

void chunk_stats(uint32_t *del,uint32_t *repl) {
	*del = stats_deletions;
	*repl = stats_replications;
	stats_deletions = 0;
	stats_replications = 0;
}

#endif

/*
#ifndef METARESTORE
int chunk_cfg_load() {
	char buff[100];
	FILE *fd;
	fd = fopen(CfgFileName,"r");
	if (!fd) {
		return -1;
	}
	while (fgets(buff,99,fd)) {
		switch(buff[0]) {
		case 'D':
		case 'd':
			MaxDel = strtoul(buff+2,NULL,0);
			break;
		case 'R':
		case 'r':
			MaxRepl = strtoul(buff+2,NULL,0);
			break;
		case 'L':
		case 'l':
			LoopTime = strtoul(buff+2,NULL,0);
			if (LoopTime<60) {
				LoopTime=60;
			}
			HashSteps = 1+((HASHSIZE)/LoopTime);
		}
	}
	fclose(fd);
	syslog(LOG_NOTICE,"chunks: new cfg loaded - (%u,%u,%u,%u)",MaxDel,MaxRepl,LoopTime,HashSteps);
	return 0;
}

void chunk_cfg_check() {
	struct stat sb;
	static int filesize=0,filemtime=0;
	if (stat(CfgFileName,&sb)!=0) {
		filesize = 0;
		filemtime = 0;
	} else {
		if (filesize != sb.st_size || filemtime != sb.st_mtime) {
			filesize = sb.st_size;
			filemtime = sb.st_mtime;
			chunk_cfg_load();
		}
	}
}
#endif
*/
chunk* chunk_new(uint64_t chunkid) {
	uint32_t chunkpos = HASHPOS(chunkid);
	chunk *newchunk;
	newchunk = malloc(sizeof(chunk));
#ifndef METARESTORE
	chunks++;
#endif
	newchunk->next = chunkhash[chunkpos];
	chunkhash[chunkpos] = newchunk;
	newchunk->chunkid = chunkid;
	newchunk->version = 0;
	newchunk->goal = 0;
	newchunk->lockedto = 0;
#ifndef METARESTORE
	newchunk->replserv = NULL;
	newchunk->interrupted = 0;
	newchunk->operation = NONE;
	newchunk->slisthead = NULL;
#endif
	newchunk->flisthead = NULL;
	lastchunkid = chunkid;
	lastchunkptr = newchunk;
	return newchunk;
}

chunk* chunk_find(uint64_t chunkid) {
	uint32_t chunkpos = HASHPOS(chunkid);
	chunk *chunkit;
	if (lastchunkid==chunkid) {
		return lastchunkptr;
	}
	for (chunkit = chunkhash[chunkpos] ; chunkit ; chunkit = chunkit->next ) {
		if (chunkit->chunkid == chunkid) {
			lastchunkid = chunkid;
			lastchunkptr = chunkit;
			return chunkit;
		}
	}
	return NULL;
}

#ifndef METARESTORE
void chunk_delete(chunk* c) {
	slist *s;
	if (lastchunkptr==c) {
		lastchunkid=0;
		lastchunkptr=NULL;
	}
	while ((s=c->slisthead)) {
		s = c->slisthead;
		c->slisthead = s->next;
		free(s);
	}
	free(c);
	chunks--;
}

void chunk_refresh_goal(chunk* c) {
	flist *f;
	c->goal = 0;
	for (f=c->flisthead ; f ; f=f->next) {
		if (f->goal > c->goal) {
			c->goal = f->goal;
		}
	}
}
#endif
/* --- */




/*
int chunk_create(uint64_t *chunkid,uint8_t goal) {
	void* ptrs[65536];
	uint8_t i,g;
	uint16_t servcount;
	chunk *c;
	slist *s;
	if (goal>15) {
		goal=15;
	}
//	servcount = matocsserv_getservers_ordered(ptrs,MINMAXRND,NULL,NULL);
	servcount = matocsserv_getservers_wrandom(ptrs,goal);
	if (servcount==0) {
		return ERROR_NOCHUNKSERVERS;
	}
	c = chunk_new(nextchunkid++);
	c->version = 1;
	c->refcount = 1;
	c->goal = goal;
	c->tgoal = goal;
	c->operation = CREATE;
	if (servcount<goal) {
		g = servcount;
	} else {
		g = goal;
	}
	for (i=0 ; i<g ; i++) {
		s = (slist*)malloc(sizeof(slist));
		s->ptr = ptrs[i];
		s->valid = BUSY;
		s->next = c->slisthead;
		c->slisthead = s;
		matocsserv_send_createchunk(s->ptr,c->chunkid,c->version);
	}
	*chunkid = c->chunkid;
	return STATUS_OK;
}

int chunk_reinitialize(uint64_t chunkid) {
	void* ptrs[65536];
	uint8_t i,g;
	uint16_t servcount;
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->lockedto>=(uint32_t)main_time()) {
		return ERROR_LOCKED;
	}
	if (c->slisthead!=NULL) {
		return ERROR_CHUNKEXIST;
	}
	servcount = matocsserv_getservers_wrandom(ptrs,c->goal);
	if (servcount==0) {
		return ERROR_NOCHUNKSERVERS;
	}
	c->operation = CREATE;
	if (servcount<c->goal) {
		g = servcount;
	} else {
		g = c->goal;
	}
	c->version++;
	for (i=0 ; i<g ; i++) {
		s = (slist*)malloc(sizeof(slist));
		s->ptr = ptrs[i];
		s->valid = BUSY;
		s->next = c->slisthead;
		c->slisthead = s;
		matocsserv_send_createchunk(s->ptr,c->chunkid,c->version);
	}
	return STATUS_OK;
}


int chunk_duplicate(uint64_t *chunkid,uint64_t oldchunkid,uint8_t goal) {
	chunk *c,*oc;
	slist *s,*os;
	uint32_t i;
	oc = chunk_find(oldchunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->lockedto>=(uint32_t)main_time()) {
		return ERROR_LOCKED;
	}
	if (goal>15) {
		goal=15;
	}
	c=NULL;
	i=0;
	for (os=oc->slisthead ;os ; os=os->next) {
		if (os->valid!=INVALID && os->valid!=DEL) {
			if (c==NULL) {
				c = chunk_new(nextchunkid++);
				c->version = 1;
				c->refcount = 1;
				c->goal = goal;
				c->tgoal = goal;
				c->operation = DUPLICATE;
			}
			s = (slist*)malloc(sizeof(slist));
			s->ptr = os->ptr;
			s->valid = BUSY;
			s->next = c->slisthead;
			c->slisthead = s;
			matocsserv_send_duplicatechunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version);
			i++;
		}
	}
	if (i>0) {
		*chunkid = c->chunkid;
		return STATUS_OK;
	} else {
		return ERROR_CHUNKLOST;
	}
}

int chunk_increase_version(uint64_t chunkid) {
	chunk *c;
	slist *s;
	uint32_t i;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->operation!=NONE) {
		return ERROR_CHUNKBUSY;
	}
	i=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL) {
			s->valid = BUSY;
			matocsserv_send_setchunkversion(s->ptr,chunkid,c->version+1,c->version);
			i++;
		}
	}
	if (i>0) {
		c->operation = SET_VERSION;
		c->version++;
		return STATUS_OK;
	} else {
		return ERROR_CHUNKLOST;
	}
}

int chunk_truncate(uint64_t chunkid,uint32_t length) {
	chunk *c;
	slist *s;
	uint32_t i;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->operation!=NONE) {
		return ERROR_CHUNKBUSY;
	}
	i=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL) {
			s->valid = BUSY;
			matocsserv_send_truncatechunk(s->ptr,chunkid,length,c->version+1,c->version);
			i++;
		}
	}
	if (i>0) {
		c->operation = TRUNCATE;
		c->version++;
		return STATUS_OK;
	} else {
		return ERROR_CHUNKLOST;
	}
}

int chunk_duptrunc(uint64_t *chunkid,uint64_t oldchunkid,uint32_t length,uint8_t goal) {
	chunk *c,*oc;
	slist *s,*os;
	uint32_t i;
	oc = chunk_find(oldchunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (oc->lockedto>=(uint32_t)main_time()) {
		return ERROR_LOCKED;
	}
	if (goal>15) {
		goal=15;
	}
	c = NULL;
	i=0;
	for (os=oc->slisthead ;os ; os=os->next) {
		if (os->valid!=INVALID && os->valid!=DEL) {
			if (c==NULL) {
				c = chunk_new(nextchunkid++);
				c->version = 1;
				c->refcount = 1;
				c->goal = goal;
				c->tgoal = goal;
				c->operation = DUPTRUNC;
			}
			s = (slist*)malloc(sizeof(slist));
			s->ptr = os->ptr;
			s->valid = BUSY;
			s->next = c->slisthead;
			c->slisthead = s;
			matocsserv_send_duptruncchunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version,length);
			i++;
		}
	}
	if (i>0) {
		*chunkid = c->chunkid;
		return STATUS_OK;
	} else {
		return ERROR_CHUNKLOST;
	}
}


void chunk_load_goal(void) {
	uint32_t i;
	chunk *c;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next) {
			c->goal = c->tgoal;
			c->tgoal = 0;
		}
	}
}
*/

int chunk_set_file_goal(uint64_t chunkid,uint32_t inode,uint16_t indx,uint8_t goal) {
	chunk *c;
	flist *f;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->goal = 0;
	for (f=c->flisthead ; f ; f=f->next) {
		if (f->inode == inode && f->indx == indx) {
			f->goal = goal;
		}
		if (f->goal > c->goal) {
			c->goal = f->goal;
		}
	}
	return STATUS_OK;
}

int chunk_delete_file(uint64_t chunkid,uint32_t inode,uint16_t indx) {
	chunk *c;
	flist *f,**fp;
	uint32_t i;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	i=0;
	c->goal = 0;
	fp = &(c->flisthead);
	while ((f=*fp)) {
		if (f->inode == inode && f->indx == indx) {
			*fp = f->next;
			free(f);
			i=1;
		} else {
			if (f->goal > c->goal) {
				c->goal = f->goal;
			}
			fp = &(f->next);
		}
	}
	if (i==0) {
		syslog(LOG_WARNING,"(delete file) serious structure inconsistency: (chunkid:%llu ; inode:%u ; index:%u)",chunkid,inode,indx);
	}
	return STATUS_OK;
}

int chunk_add_file(uint64_t chunkid,uint32_t inode,uint16_t indx,uint8_t goal) {
	chunk *c;
	flist *f;
	uint32_t i;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	i=0;
	c->goal = 0;
	for (f=c->flisthead ; f ; f=f->next) {
		if (f->inode == inode && f->indx == indx) {
			f->goal = goal;
			i=1;
		}
		if (f->goal > c->goal) {
			c->goal = f->goal;
		}
	}
	if (i==0) {
		f = (flist*)malloc(sizeof(flist*));
		f->inode = inode;
		f->indx = indx;
		f->goal = goal;
		f->next = c->flisthead;
		c->flisthead = f;
		if (goal > c->goal) {
			c->goal = goal;
		}
	} else {
		syslog(LOG_WARNING,"(add file) serious structure inconsistency: (chunkid:%llu ; inode:%u ; index:%u)",chunkid,inode,indx);
	}
	return STATUS_OK;
}

/*
int chunk_get_refcount(uint64_t chunkid,uint16_t *refcount) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	*refcount = c->refcount;
	return STATUS_OK;
//	return c->refcount;
}

int chunk_locked(uint64_t chunkid,uint8_t *l) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->lockedto>=(uint32_t)main_time()) {
		*l = 1;
	} else {
		*l = 0;
	}
	return STATUS_OK;
//	return c->locked;
}

int chunk_writelock(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
	return STATUS_OK;
}
*/

int chunk_unlock(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->lockedto=0;
	return STATUS_OK;
}

#ifndef METARESTORE

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies) {
	chunk *c;
	slist *s;
	uint8_t vc;
	*vcopies = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	vc=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && vc<255) {
			vc++;
		}
	}
	*vcopies = vc;
	return STATUS_OK;
}
#endif


#ifndef METARESTORE
int chunk_multi_modify(uint64_t *nchunkid,uint64_t ochunkid,uint32_t inode,uint16_t indx,uint8_t goal) {
	void* ptrs[65536];
	uint16_t servcount;
	slist *os,*s;
	uint8_t g;
#else
int chunk_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint32_t inode,uint16_t indx,uint8_t goal) {
#endif
	uint32_t i;
	chunk *oc,*c;
	flist *f,**fp;

	if (ochunkid==0) {	// new chunk
//		servcount = matocsserv_getservers_ordered(ptrs,MINMAXRND,NULL,NULL);
#ifndef METARESTORE
		servcount = matocsserv_getservers_wrandom(ptrs,goal);
		if (servcount==0) {
			return ERROR_NOCHUNKSERVERS;
		}
#endif
		c = chunk_new(nextchunkid++);
		c->version = 1;
		c->goal = goal;
#ifndef METARESTORE
		c->interrupted = 0;
		c->operation = CREATE;
#endif
		c->flisthead = (flist*)malloc(sizeof(flist));
		c->flisthead->inode = inode;
		c->flisthead->indx = indx;
		c->flisthead->goal = goal;
		c->flisthead->next = NULL;
#ifndef METARESTORE
		if (servcount<goal) {
			g = servcount;
		} else {
			g = goal;
		}
		for (i=0 ; i<g ; i++) {
			s = (slist*)malloc(sizeof(slist));
			s->ptr = ptrs[i];
			s->valid = BUSY;
			s->next = c->slisthead;
			c->slisthead = s;
			matocsserv_send_createchunk(s->ptr,c->chunkid,c->version);
		}
#endif
		*nchunkid = c->chunkid;
	} else {
		c = NULL;
		oc = chunk_find(ochunkid);
		if (oc==NULL) {
			return ERROR_NOCHUNK;
		}
#ifndef METARESTORE
		if (oc->lockedto>=(uint32_t)main_time()) {
			return ERROR_LOCKED;
		}
#endif
		fp = &(oc->flisthead);
		i=1;
		while ((f=*fp)) {
			if (f->inode==inode && f->indx==indx) {
				f->goal = goal;	// not needed.
				break;
			}
			fp = &(f->next);
			i=0;
		}
		if (i && f && f->next==NULL) {	// refcount==1
			*nchunkid = ochunkid;
			c = oc;
/*zapis bez zwiekszania wersji
#ifndef METARESTORE
	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
#else
	c->lockedto=ts+LOCKTIMEOUT;
#endif
	return 255;
*/
#ifndef METARESTORE

			if (c->operation!=NONE) {
				return ERROR_CHUNKBUSY;
			}
			i=0;
			for (s=c->slisthead ;s ; s=s->next) {
				if (s->valid!=INVALID && s->valid!=DEL) {
					if (s->valid==TDVALID || s->valid==TDBUSY) {
						s->valid = TDBUSY;
					} else {
						s->valid = BUSY;
					}
					matocsserv_send_setchunkversion(s->ptr,ochunkid,c->version+1,c->version);
					i++;
				}
			}
			if (i>0) {
				c->interrupted = 0;
				c->operation = SET_VERSION;
				c->version++;
			} else {
				return ERROR_CHUNKLOST;
			}

#else
			c->version++;
#endif
		} else {
			if (f==NULL) {	// it's serious structure error
#ifndef METARESTORE
				syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%llu ; inode:%u ; index:%u)",ochunkid,inode,indx);
#else
				printf("serious structure inconsistency: (chunkid:%llu ; inode:%u ; index:%u)\n",ochunkid,inode,indx);
#endif
				return ERROR_CHUNKLOST;	// ERROR_STRUCTURE
			}
#ifndef METARESTORE
			i=0;
			for (os=oc->slisthead ;os ; os=os->next) {
				if (os->valid!=INVALID && os->valid!=DEL) {
					if (c==NULL) {
#endif
						c = chunk_new(nextchunkid++);
						c->version = 1;
						c->goal = goal;
#ifndef METARESTORE
						c->interrupted = 0;
						c->operation = DUPLICATE;
#endif
						// move f to new chunk
						c->flisthead = f;
						*fp = f->next;
						f->next = NULL;
#ifndef METARESTORE
					}
					s = (slist*)malloc(sizeof(slist));
					s->ptr = os->ptr;
					s->valid = BUSY;
					s->next = c->slisthead;
					c->slisthead = s;
					matocsserv_send_duplicatechunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version);
					i++;
				}
			}
			if (i>0) {
#endif
				*nchunkid = c->chunkid;
				oc->goal = 0;
				for (f=oc->flisthead ; f ; f=f->next) {
					if (f->goal > oc->goal) {
						oc->goal = f->goal;
					}
				}
#ifndef METARESTORE
			} else {
				return ERROR_CHUNKLOST;
			}
#endif
		}
	}

#ifndef METARESTORE
	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
#else
	c->lockedto=ts+LOCKTIMEOUT;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint32_t inode,uint16_t indx,uint8_t goal) {
	slist *os,*s;
#else
int chunk_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint32_t inode,uint16_t indx,uint8_t goal) {
#endif
	chunk *oc,*c;
	flist *f,**fp;
	uint32_t i;

	c=NULL;
	oc = chunk_find(ochunkid);
	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
#ifndef METARESTORE
	if (oc->lockedto>=(uint32_t)main_time()) {
		return ERROR_LOCKED;
	}
#endif
	fp = &(oc->flisthead);
	i=1;
	while ((f=*fp)) {
		if (f->inode==inode && f->indx==indx) {
			f->goal = goal;	// not needed.
			break;
		}
		fp = &(f->next);
		i=0;
	}
	if (i && f && f->next==NULL) {	// refcount==1
		*nchunkid = ochunkid;
		c = oc;
#ifndef METARESTORE
		if (c->operation!=NONE) {
			return ERROR_CHUNKBUSY;
		}
		i=0;
		for (s=c->slisthead ;s ; s=s->next) {
			if (s->valid!=INVALID && s->valid!=DEL) {
				if (s->valid==TDVALID || s->valid==TDBUSY) {
					s->valid = TDBUSY;
				} else {
					s->valid = BUSY;
				}
				matocsserv_send_truncatechunk(s->ptr,ochunkid,length,c->version+1,c->version);
				i++;
			}
		}
		if (i>0) {
			c->interrupted = 0;
			c->operation = TRUNCATE;
			c->version++;
		} else {
			return ERROR_CHUNKLOST;
		}
#else
		c->version++;
#endif
	} else {
		if (f==NULL) {	// it's serious structure error
#ifndef METARESTORE
				syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%llu ; inode:%u ; index:%u)",ochunkid,inode,indx);
#else
				printf("serious structure inconsistency: (chunkid:%llu ; inode:%u ; index:%u)\n",ochunkid,inode,indx);
#endif
			return ERROR_CHUNKLOST;	// ERROR_STRUCTURE
		}
#ifndef METARESTORE
		i=0;
		for (os=oc->slisthead ;os ; os=os->next) {
			if (os->valid!=INVALID && os->valid!=DEL) {
				if (c==NULL) {
#endif
					c = chunk_new(nextchunkid++);
					c->version = 1;
					c->goal = goal;
#ifndef METARESTORE
					c->interrupted = 0;
					c->operation = DUPTRUNC;
#endif
					// move f to new chunk
					c->flisthead = f;
					*fp = f->next;
					f->next = NULL;
#ifndef METARESTORE
				}
				s = (slist*)malloc(sizeof(slist));
				s->ptr = os->ptr;
				s->valid = BUSY;
				s->next = c->slisthead;
				c->slisthead = s;
				matocsserv_send_duptruncchunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version,length);
				i++;
			}
		}
		if (i>0) {
#endif
			*nchunkid = c->chunkid;
			oc->goal = 0;
			for (f=oc->flisthead ; f ; f=f->next) {
				if (f->goal > oc->goal) {
					oc->goal = f->goal;
				}
			}
#ifndef METARESTORE
		} else {
			return ERROR_CHUNKLOST;
		}
#endif
	}

#ifndef METARESTORE
	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
#else
	c->lockedto=ts+LOCKTIMEOUT;
#endif
	return STATUS_OK;
}


#ifndef METARESTORE
int chunk_multi_reinitialize(uint64_t chunkid) {
	void* ptrs[65536];
	uint8_t i,g;
	uint16_t servcount;
	chunk *c;
	slist *s;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->lockedto>=(uint32_t)main_time()) {
		return ERROR_LOCKED;
	}

	if (c->slisthead!=NULL) {
		return ERROR_CHUNKEXIST;
	}
	servcount = matocsserv_getservers_wrandom(ptrs,c->goal);
	if (servcount==0) {
		return ERROR_NOCHUNKSERVERS;
	}
	c->interrupted = 0;
	c->operation = CREATE;
	if (servcount<c->goal) {
		g = servcount;
	} else {
		g = c->goal;
	}
	c->version++;
	for (i=0 ; i<g ; i++) {
		s = (slist*)malloc(sizeof(slist));
		s->ptr = ptrs[i];
		s->valid = BUSY;
		s->next = c->slisthead;
		c->slisthead = s;
		matocsserv_send_createchunk(s->ptr,c->chunkid,c->version);
	}

	c->lockedto=(uint32_t)main_time()+LOCKTIMEOUT;
	return STATUS_OK;
}
#else
int chunk_multi_reinitialize(uint32_t ts,uint64_t chunkid) {
	chunk *c;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->version++;
	c->lockedto=ts+LOCKTIMEOUT;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
void chunk_emergency_increase_version(chunk *c) {
	slist *s;
	uint32_t i;
	i=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL) {
			if (s->valid==TDVALID || s->valid==TDBUSY) {
				s->valid = TDBUSY;
			} else {
				s->valid = BUSY;
			}
			matocsserv_send_setchunkversion(s->ptr,c->chunkid,c->version+1,c->version);
			i++;
		}
	}
	if (i>0) {	// should always be true !!!
		c->interrupted = 0;
		c->operation = SET_VERSION;
		c->version++;
	} else {
		matocuserv_chunk_status(c->chunkid,ERROR_CHUNKLOST);
	}
	fs_incversion(c->chunkid);
}
#else
int chunk_increase_version(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->version++;
	return STATUS_OK;
}
#endif

/* ---- */

#ifndef METARESTORE

int chunk_getversionandlocations(uint64_t chunkid,uint32_t *version,uint8_t *count,void *sptr[256]) {
	chunk *c;
	slist *s;
	uint8_t i,k,cnt;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	*version = c->version;
	cnt=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL) {
			sptr[cnt++]=s->ptr;
		}
	}
	// make random permutation
	for (i=0 ; i<cnt ; i++) {
		// k = random <i,j)
		k = i+(rndu32()%(cnt-i));
		// swap (i,k)
		if (i!=k) {
			void* p = sptr[i];
			sptr[i] = sptr[k];
			sptr[k] = p;
		}
	}
	*count = cnt;
	return STATUS_OK;
}

/* ---- */

void chunk_server_has_chunk(void *ptr,uint64_t chunkid,uint32_t version) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		syslog(LOG_WARNING,"chunkserver has nonexistent chunk, so create it for future deletion");
		c = chunk_new(chunkid);
		c->version = version;
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr==ptr) {
			return;
		}
	}
	s = (slist*)malloc(sizeof(slist));
	s->ptr = ptr;
	if (c->version!=(version&0x7FFFFFFF)) {
		s->valid=INVALID;
	} else {
		if (version&0x80000000) {
			s->valid=TDVALID;
			todelchunks++;
		} else {
			s->valid=VALID;
		}
	}
	s->next = c->slisthead;
	c->slisthead = s;
}

void chunk_damaged(void *ptr,uint64_t chunkid) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		syslog(LOG_WARNING,"chunkserver has nonexistent chunk, so create it for future deletion");
		c = chunk_new(chunkid);
		c->version = 0;
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr==ptr) {
			if (s->valid==TDBUSY || s->valid==TDVALID) {
				todelchunks--;
			}
			s->valid = INVALID;
			return;
		}
	}
	s = (slist*)malloc(sizeof(slist));
	s->ptr = ptr;
	s->valid = INVALID;
	s->next = c->slisthead;
	c->slisthead = s;
}

void chunk_lost(void *ptr,uint64_t chunkid) {
	chunk *c;
	slist **sptr,*s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return;
	}
	sptr=&(c->slisthead);
	while ((s=*sptr)) {
		if (s->ptr==ptr) {
			*sptr = s->next;
			free(s);
		} else {
			sptr = &(s->next);
		}
	}
}

void chunk_server_disconnected(void *ptr) {
	chunk *c;
	slist *s,**st;
	uint32_t i;
	uint8_t valid,vs;
	//jobsnorepbefore = main_time()+ReplicationsDelayDisconnect;
	//jobslastdisconnect = main_time();
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next ) {
			if (ptr==c->replserv) {
				jobscopycount--;
				c->replserv=NULL;
			}
			st = &(c->slisthead);
			while (*st) {
				s = *st;
				if (s->ptr == ptr) {
					if (s->valid == DEL) {
						jobsdelcount--;
					}
					if (s->valid==TDBUSY || s->valid==TDVALID) {
						todelchunks--;
					}
					*st = s->next;
					free(s);
				} else {
					st = &(s->next);
				}
			}
			vs=0;
			valid=1;
			if (c->operation!=NONE) {
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->valid==BUSY || s->valid==TDBUSY) {
						valid=0;
					}
					if (s->valid==VALID || s->valid==TDVALID) {
						vs++;
					}
				}
				if (valid) {
					if (vs>0) {
						chunk_emergency_increase_version(c);
//						matocuserv_chunk_status(c->chunkid,STATUS_OK);
					} else {
						matocuserv_chunk_status(c->chunkid,ERROR_NOTDONE);
						c->operation=NONE;
					}
				} else {
					c->interrupted = 1;
				}
			}
		}
	}
}

void chunk_got_delete_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	slist *s,**st;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (s->ptr == ptr) {
			if (s->valid==DEL) {
				jobsdelcount--;
			} else {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					todelchunks--;
				}
				syslog(LOG_WARNING,"got unexpected delete status");
			}
			*st = s->next;
			free(s);
		} else {
			st = &(s->next);
		}
	}
	if (status!=0) {
		return ;
	}
}

void chunk_got_replicate_status(void *ptr,uint64_t chunkid,uint32_t version,uint8_t status) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	if (c->replserv!=ptr) {
		syslog(LOG_WARNING,"got unexpected replicate status");
		return ;
	}
	c->replserv = NULL;
	jobscopycount--;
	if (status!=0) {
		return ;
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr == ptr) {
			return;
		}
	}
	s = (slist*)malloc(sizeof(slist));
	s->ptr = ptr;
	if (c->lockedto>=(uint32_t)main_time() || version!=c->version) {
		s->valid = INVALID;
	} else {
		s->valid = VALID;
	}
	s->next = c->slisthead;
	c->slisthead = s;
}


void chunk_operation_status(chunk *c,uint8_t status,void *ptr) {
	uint8_t valid,vs;
	slist *s;
/*
	slist *s,**st;
	if (status!=0) {
		st = &(c->slisthead);
		while (*st) {
			s = *st;
			if (s->ptr == ptr) {
				*st = s->next;
				free(s);
			} else {
				st = &(s->next);
			}
		}
	}
*/
	vs=0;
	valid=1;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr == ptr) {
			if (status!=0) {
				c->interrupted = 1;	// na wszelki wypadek zwiêksz wersjê po zakoñczeniu
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					todelchunks--;
				}
				s->valid=INVALID;
			} else {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					s->valid=TDVALID;
				} else {
					s->valid=VALID;
				}
			}
		}
		if (s->valid==BUSY || s->valid==TDBUSY) {
			valid=0;
		}
		if (s->valid==VALID || s->valid==TDVALID) {
			vs++;
		}
	}
	if (valid) {
		if (vs>0) {
			if (c->interrupted) {
				chunk_emergency_increase_version(c);
			} else {
				matocuserv_chunk_status(c->chunkid,STATUS_OK);
				c->operation=NONE;
			}
		} else {
			matocuserv_chunk_status(c->chunkid,ERROR_NOTDONE);
			c->operation=NONE;
		}
	}
}


void chunk_got_create_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_duplicate_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_setversion_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_truncate_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

void chunk_got_duptrunc_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
}

/* ----------------------- */
/* JOBS (DELETE/REPLICATE) */
/* ----------------------- */

void chunk_store_info(uint8_t *buff) {
	PUT32BIT(chunksinfo_loopstart,buff);
	PUT32BIT(chunksinfo_loopend,buff);
	PUT32BIT(chunksinfo.done.del_invalid,buff);
	PUT32BIT(chunksinfo.notdone.del_invalid,buff);
	PUT32BIT(chunksinfo.done.del_unused,buff);
	PUT32BIT(chunksinfo.notdone.del_unused,buff);
	PUT32BIT(chunksinfo.done.del_diskclean,buff);
	PUT32BIT(chunksinfo.notdone.del_diskclean,buff);
	PUT32BIT(chunksinfo.done.del_overgoal,buff);
	PUT32BIT(chunksinfo.notdone.del_overgoal,buff);
	PUT32BIT(chunksinfo.done.copy_undergoal,buff);
	PUT32BIT(chunksinfo.notdone.copy_undergoal,buff);
	PUT32BIT(chunksinfo.copy_rebalance,buff);
}

//jobs state: jobshpos, jobscptr, jobsdelcount, jobscopycount 

void chunk_do_jobs(chunk *c,uint16_t scount,double minusage,double maxusage) {
	slist *s;
	static void* ptrs[65535];
	static uint16_t servcount;
	static uint32_t min,max;
	void *srcptr;
	uint16_t i;
	uint32_t vc,tdc,ivc,bc,dc;
	static loop_info inforec;

	if (c==NULL) {
		if (scount==0) {
			servcount=0;
		} else if (scount==1) {
			chunksinfo = inforec;
			memset(&inforec,0,sizeof(inforec));
			chunksinfo_loopstart = chunksinfo_loopend;
			chunksinfo_loopend = main_time();
		}
		return;
	}
// step 1. calculate number of valid and invalid copies
	vc=tdc=ivc=bc=dc=0;
	for (s=c->slisthead ; s ; s=s->next) {
		switch (s->valid) {
		case INVALID:
			ivc++;
			break;
		case TDVALID:
			tdc++;
			break;
		case VALID:
			vc++;
			break;
		case TDBUSY:
		case BUSY:
			bc++;
			break;
		case DEL:
			dc++;
			break;
		}
	}

//	syslog(LOG_WARNING,"chunk %016llX: ivc=%u , tdc=%u , vc=%u , bc=%u , dc=%u , goal=%u",c->chunkid,ivc,tdc,vc,bc,dc,c->goal); 

// step 2. check number of copies
	if (tdc+vc+bc==0 && ivc>0 && c->flisthead) {
		syslog(LOG_WARNING,"chunk %llu has only invalid copies (%u) - please repair it manually\n",c->chunkid,ivc);
		for (s=c->slisthead ; s ; s=s->next) {
			syslog(LOG_NOTICE,"chunk %llu (%016llX:%08X) - invalid copy on (%s)",c->chunkid,c->chunkid,c->version,matocsserv_getstrip(s->ptr));
		}
		return ;
	}

// step 3. delete invalid copies
	if (jobsdelcount<MaxDel) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==INVALID) { 
				s->valid = DEL;
				stats_deletions++;
				matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
				jobsdelcount++;
				inforec.done.del_invalid++;
				dc++;
				ivc--;
			}
		}
	} else {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==INVALID) {
				inforec.notdone.del_invalid++;
			}
		}
	}

// step 4. return if chunk is during some operation
	if (c->operation!=NONE || (c->lockedto>=(uint32_t)main_time())) {
		return ;
	}

// step 5. check busy count
	if (bc>0) {
		syslog(LOG_WARNING,"chunk %llu has unexpected BUSY copies",c->chunkid);
		return ;
	}

// step 6. delete unused chunk
	if (c->flisthead==NULL) {
		if (jobsdelcount<MaxDel) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID || s->valid==TDVALID) {
					if (s->valid==TDVALID) {
						todelchunks--;
					}
					s->valid=DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,c->version);
					jobsdelcount++;
					inforec.done.del_unused++;
				}
			}
		} else {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID || s->valid==TDVALID) {
					inforec.notdone.del_unused++;
				}
			}
		}
		return ;
	}

// step 7a. if chunk has too many copies and some of them have status TODEL then delete them
	if (vc+tdc>c->goal && tdc>0) {
		if (jobsdelcount<MaxDel) {
			for (s=c->slisthead ; s && vc+tdc>c->goal && tdc>0 ; s=s->next) {
				if (s->valid==TDVALID) {
					todelchunks--;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					jobsdelcount++;
					inforec.done.del_diskclean++;
					tdc--;
					dc++;
				}
			}
		} else {
			if (vc>=c->goal) {
				inforec.notdone.del_diskclean+=tdc;
			} else {
				inforec.notdone.del_diskclean+=((vc+tdc)-(c->goal));
			}
		}
		return;
	}

// step 7b. if chunk has too many copies then delete some of them
	if (vc > c->goal) {
		if (jobsdelcount<MaxDel) {
			if (servcount==0) {
				servcount = matocsserv_getservers_ordered(ptrs,MINMAXRND,&min,&max);
			}
			for (i=0 ; i<servcount && vc>c->goal ; i++) {
				for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next) {}
				if (s && s->valid==VALID) {
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					jobsdelcount++;
					inforec.done.del_overgoal++;
					vc--;
					dc++;
				}
			}
		} else {
			inforec.notdone.del_overgoal+=(vc-(c->goal));
		}
		return;
	}

// step 7c. if chunk has one copy on each server and some of them have status TODEL then delete one of it
	if (vc+tdc>=scount && tdc>0 && vc+tdc>1) {
		if (jobsdelcount<MaxDel) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==TDVALID) {
					todelchunks--;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					jobsdelcount++;
					inforec.done.del_diskclean++;
					tdc--;
					dc++;
					return;
				}
			}
		} else {
			inforec.notdone.del_diskclean++;
		}
		return;
	}

// copies can make only if maximum not excided
//	if (jobscopycount>=MaxRepl) {
//		return ;
//	}

// step 8. if chunk has number of copies less than goal then make another copy of this chunk
	if (c->goal > vc && vc+tdc > 0) {
		if (jobscopycount<MaxRepl && c->replserv==NULL && maxusage<=0.99 && jobsnorepbefore<(uint32_t)main_time()) {
			if (servcount==0) {
				servcount = matocsserv_getservers_ordered(ptrs,MINMAXRND,&min,&max);
			}
			for (i=0 ; i<servcount ; i++) {
				for (s=c->slisthead ; s && s->ptr!=ptrs[i] ; s=s->next) {}
				if (!s) {
					uint32_t r;
					if (vc>0) {	// if there are VALID copies then make copy of one VALID chunk
						r = 1+(rndu32()%vc);
						srcptr = NULL;
						for (s=c->slisthead ; s && r>0 ; s=s->next) {
							if (s->valid==VALID) {
								r--;
								srcptr = s->ptr;
							}
						}
					} else {	// if not then use TDVALID chunks.
						r = 1+(rndu32()%tdc);
						srcptr = NULL;
						for (s=c->slisthead ; s && r>0 ; s=s->next) {
							if (s->valid==TDVALID) {
								r--;
								srcptr = s->ptr;
							}
						}
					}
					if (srcptr) {
						stats_replications++;
						matocsserv_send_replicatechunk(ptrs[i],c->chunkid,c->version,srcptr);
						jobscopycount++;
						inforec.done.copy_undergoal++;
						c->replserv=ptrs[i];
					}
					return;
				}
			}
		} else {
			inforec.notdone.copy_undergoal++;
		}
	}

	if (chunksinfo.notdone.copy_undergoal>0) {
		return;
	}

// step 9. if there is too big difference between chunkservers then make copy on server with lowest disk usage
	if (jobscopycount<MaxRepl && c->replserv==NULL && c->goal == vc && vc+tdc>0 && (maxusage-minusage)>MAXDIFFERENCE) {
		if (servcount==0) {
			servcount = matocsserv_getservers_ordered(ptrs,MINMAXRND,&min,&max);
		}
		if (min>0 && max>0) {
			void *srcserv=NULL;
			void *dstserv=NULL;
			for (i=0 ; i<max && srcserv==NULL ; i++) {
				for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next ) {}
				if (s && (s->valid==VALID || s->valid==TDVALID)) {
					srcserv=s->ptr;
				}
			}
			if (srcserv!=NULL) {
				for (i=0 ; i<min && dstserv==NULL ; i++) {
					for (s=c->slisthead ; s && s->ptr!=ptrs[i] ; s=s->next ) {}
					if (s==NULL) {
						dstserv=ptrs[i];
					}
				}
				if (dstserv!=NULL) {
					stats_replications++;
					matocsserv_send_replicatechunk(dstserv,c->chunkid,c->version,srcserv);
					jobscopycount++;
					inforec.copy_rebalance++;
					c->replserv=dstserv;
				}
			}
		}
	}
}

void chunk_jobs_main(void) {
	uint32_t i,l,r;
	uint16_t uscount,tscount;
	static uint16_t lasttscount=0;
	static uint16_t maxtscount=0;
	double minusage,maxusage;
	chunk *c,**cp;
	matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);

	if (tscount<lasttscount) {		// servers disconnected
		jobsnorepbefore = main_time()+ReplicationsDelayDisconnect;
	} else if (tscount>lasttscount) {	// servers connected
		if (tscount>=maxtscount) {
			maxtscount = tscount;
			jobsnorepbefore = main_time();
		}
	} else if (tscount<maxtscount && (uint32_t)main_time()>jobsnorepbefore) {
		maxtscount = tscount;
	}
	lasttscount = tscount;

	if (minusage>maxusage) {
		return;
	}

	chunk_do_jobs(NULL,0,0.0,0.0);	// clear servercount
	for (i=0 ; i<HashSteps ; i++) {
		if (jobshpos==0) {
			chunk_do_jobs(NULL,1,0.0,0.0);	// copy loop info
		}
		// delete unused chunks from structures
		l=0;
		cp = &(chunkhash[jobshpos]);
		while ((c=*cp)!=NULL) {
			if (c->flisthead==NULL && c->slisthead==NULL) {
				*cp = (c->next);
				chunk_delete(c);
			} else {
				cp = &(c->next);
				l++;
			}
		}
		if (l>0) {
			r = rndu32()%l;
			l=0;
		// do jobs on rest of them
			for (c=chunkhash[jobshpos] ; c ; c=c->next) {
				if (l>=r) {
					chunk_do_jobs(c,uscount,minusage,maxusage);
				}
				l++;
			}
			l=0;
			for (c=chunkhash[jobshpos] ; l<r && c ; c=c->next) {
				chunk_do_jobs(c,uscount,minusage,maxusage);
				l++;
			}
		}
//		for (c=chunkhash[jobshpos] ; c ; c=c->next) {
//			chunk_do_jobs(c,uscount,minusage,maxusage);
//		}
		jobshpos+=123;	// if HASHSIZE is any power of 2 then any odd number is good here
		jobshpos%=HASHSIZE;
	}
}

#endif

/* ---- */

// #define CHUNKFSIZE11 12
#define CHUNKFSIZE 16
#define CHUNKCNT 1000

/*
int chunk_load_1_1(FILE *fd) {
	uint8_t hdr[8];
	uint8_t loadbuff[CHUNKFSIZE11*CHUNKCNT];
	uint8_t *ptr;
	int32_t r;
	uint32_t i,j;
	chunk *c;
	int readlast;
// chunkdata
	uint64_t chunkid;
	uint32_t version;

	fread(hdr,1,8,fd);
	ptr = hdr;
	GET64BIT(nextchunkid,ptr);
	readlast = 0;
	for (;;) {
		r = fread(loadbuff,1,CHUNKFSIZE11*CHUNKCNT,fd);
		if (r<0) {
			return -1;
		}
		if ((r%CHUNKFSIZE11)!=0) {
			return -1;
		}
		i = r/CHUNKFSIZE11;
		ptr = loadbuff;
		for (j=0 ; j<i ; j++) {
			GET64BIT(chunkid,ptr);
			if (chunkid>0) {
				if (readlast==1) {
					return -1;
				}
				c = chunk_new(chunkid);
				GET32BIT(version,ptr);
				c->version = version;
			} else {
				readlast = 1;
				GET32BIT(version,ptr);
			}
		}
		if (i<CHUNKCNT) {
			break;
		}
	}
	if (readlast==0) {
		return -1;
	}
	return 0;
}
*/

#ifdef METARESTORE

void chunk_dump(void) {
	chunk *c;
	uint32_t i,lockedto,now;
	now = time(NULL);

	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next) {
			lockedto = c->lockedto;
			if (lockedto<now) {
				lockedto = 0;
			}
			printf("*|i:%016llX|v:%08X|g:%u|t:%10u\n",c->chunkid,c->version,c->goal,c->lockedto);
		}
	}
}

#endif

int chunk_load(FILE *fd) {
	uint8_t hdr[8];
	uint8_t loadbuff[CHUNKFSIZE*CHUNKCNT];
	uint8_t *ptr;
	int32_t r;
	uint32_t i,j;
	chunk *c;
	int readlast;
// chunkdata
	uint64_t chunkid;
	uint32_t version,lockedto;

	chunks=0;
	fread(hdr,1,8,fd);
	ptr = hdr;
	GET64BIT(nextchunkid,ptr);
	readlast = 0;
	for (;;) {
		r = fread(loadbuff,1,CHUNKFSIZE*CHUNKCNT,fd);
		if (r<0) {
			return -1;
		}
		if ((r%CHUNKFSIZE)!=0) {
			return -1;
		}
		i = r/CHUNKFSIZE;
		ptr = loadbuff;
		for (j=0 ; j<i ; j++) {
			GET64BIT(chunkid,ptr);
			if (chunkid>0) {
				if (readlast==1) {
					return -1;
				}
				c = chunk_new(chunkid);
				GET32BIT(version,ptr);
				c->version = version;
				GET32BIT(lockedto,ptr);
				c->lockedto = lockedto;
			} else {
				readlast = 1;
				GET32BIT(version,ptr);
				GET32BIT(lockedto,ptr);
			}
		}
		if (i<CHUNKCNT) {
			break;
		}
	}
	if (readlast==0) {
		return -1;
	}
	return 0;
}

void chunk_store(FILE *fd) {
	uint8_t hdr[8];
	uint8_t storebuff[CHUNKFSIZE*CHUNKCNT];
	uint8_t *ptr;
	uint32_t i,j;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version;
	uint32_t lockedto,now;
#ifndef METARESTORE
	now = main_time();
#else
	now = time(NULL);
#endif
	ptr = hdr;
	PUT64BIT(nextchunkid,ptr);
	fwrite(hdr,1,8,fd);
	j=0;
	ptr = storebuff;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next) {
			chunkid = c->chunkid;
			PUT64BIT(chunkid,ptr);
			version = c->version;
			PUT32BIT(version,ptr);
			lockedto = c->lockedto;
			if (lockedto<now) {
				lockedto = 0;
			}
			PUT32BIT(lockedto,ptr);
			j++;
			if (j==CHUNKCNT) {
				fwrite(storebuff,1,CHUNKFSIZE*CHUNKCNT,fd);
				j=0;
				ptr = storebuff;
			}
		}
	}
	memset(ptr,0,CHUNKFSIZE);
	j++;
	fwrite(storebuff,1,CHUNKFSIZE*j,fd);
}

void chunk_newfs(void) {
	chunks=0;
	nextchunkid = 1;
}

#ifndef METARESTORE
uint32_t chunk_count(void) {
	return chunks;
}

uint32_t chunk_todel_count(void) {
	return todelchunks;
}
#endif

void chunk_strinit(void) {
	uint32_t i;
#ifndef METARESTORE
	config_getuint32("REPLICATIONS_DELAY_INIT",300,&ReplicationsDelayInit);
	config_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600,&ReplicationsDelayDisconnect);
	config_getuint32("CHUNKS_DEL_LIMIT",100,&MaxDel);
	config_getuint32("CHUNKS_REP_LIMIT",15,&MaxRepl);
	config_getuint32("CHUNKS_LOOP_TIME",300,&LoopTime);
	HashSteps = 1+((HASHSIZE)/LoopTime);
//	config_getnewstr("CHUNKS_CONFIG",ETC_PATH "/mfschunks.cfg",&CfgFileName);
#endif
	for (i=0 ; i<HASHSIZE ; i++) {
		chunkhash[i]=NULL;
	}
#ifndef METARESTORE
	jobshpos = 0;
	jobsrebalancecount = 0;
	jobscopycount = 0;
	jobsdelcount = 0;
	jobsnorepbefore = main_time()+ReplicationsDelayInit;
	//jobslastdisconnect = 0;
/*
	chunk_cfg_check();
	main_timeregister(30,0,chunk_cfg_check);
*/
	main_timeregister(1,0,chunk_jobs_main);
#endif
}
