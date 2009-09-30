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

#include "config.h"

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
#endif

#include "chunks.h"
#include "filesystem.h"
#include "datapack.h"

#define USE_SLIST_BUCKETS 1
#define USE_FLIST_BUCKETS 1
#define USE_CHUNK_BUCKETS 1

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

/*
typedef struct _bcdata {
	void *ptr;
	uint32_t version;
} bcdata;
*/

typedef struct _slist {
	void *ptr;
	uint8_t valid;
	uint32_t version;
//	uint8_t sectionid; - idea - Split machines into sctions. Try to place each copy of particular chunk in different section.
//	uint16_t machineid; - idea - If there are many different processes on the same physical computer then place there only one copy of chunk.
	struct _slist *next;
} slist;

#ifdef USE_SLIST_BUCKETS
#define SLIST_BUCKET_SIZE 5000

typedef struct _slist_bucket {
	slist bucket[SLIST_BUCKET_SIZE];
	uint32_t firstfree;
	struct _slist_bucket *next;
} slist_bucket;

static slist_bucket *sbhead = NULL;
static slist *slfreehead = NULL;
#endif /* USE_SLIST_BUCKET */

#endif /* METARESTORE */

typedef struct _flist {
	uint32_t inode;
	uint16_t indx;
	uint8_t goal;
	struct _flist *next;
} flist;

#ifdef USE_FLIST_BUCKETS
#define FLIST_BUCKET_SIZE 5000
typedef struct _flist_bucket {
	flist bucket[FLIST_BUCKET_SIZE];
	uint32_t firstfree;
	struct _flist_bucket *next;
} flist_bucket;

static flist_bucket *fbhead = NULL;
static flist *flfreehead = NULL;
#endif /* USE_FLIST_BUCKET */

typedef struct chunk {
	uint64_t chunkid;
	uint32_t version;
	uint8_t goal;
#ifndef METARESTORE
	uint8_t validcopies;
	uint8_t needverincrease:1;
	uint8_t interrupted:1;
	uint8_t operation:4;
#endif
	uint32_t lockedto;
#ifndef METARESTORE
//	uint32_t lockedby;
	slist *slisthead;
//	bcdata *bestchunk;
#endif
	flist *flisthead;
	struct chunk *next;
} chunk;

#ifdef USE_CHUNK_BUCKETS
#define CHUNK_BUCKET_SIZE 20000
typedef struct _chunk_bucket {
	chunk bucket[CHUNK_BUCKET_SIZE];
	uint32_t firstfree;
	struct _chunk_bucket *next;
} chunk_bucket;

static chunk_bucket *cbhead = NULL;
static chunk *chfreehead = NULL;
#endif /* USE_CHUNK_BUCKETS */

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
// static uint32_t MaxRepl=1;
// static uint32_t MaxDel=100;
// static uint32_t LoopTime=300;
// static uint32_t HashSteps=1+((HASHSIZE)/3600);
static uint32_t MaxWriteRepl;
static uint32_t MaxReadRepl;
static uint32_t MaxDel;
static double TmpMaxDelFrac;
static uint32_t TmpMaxDel;
static uint32_t LoopTime;
static uint32_t HashSteps;

//#define MAXCOPY 2
//#define MAXDEL 6
//#define LOOPTIME 3600
//#define HASHSTEPS (1+((HASHSIZE)/(LOOPTIME)))

#define ACCEPTABLE_DIFFERENCE 0.01

static uint32_t jobshpos;
static uint32_t jobsrebalancecount;
static uint32_t jobsnorepbefore;

static uint32_t starttime;

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

#ifndef METARESTORE
static uint32_t chunks;
static uint32_t todelchunks;
#endif

#ifndef METARESTORE
uint32_t chunkcounts[11][11];
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

#ifndef METARESTORE
#ifdef USE_SLIST_BUCKETS
static inline slist* slist_malloc() {
	slist_bucket *sb;
	slist *ret;
	if (slfreehead) {
		ret = slfreehead;
		slfreehead = ret->next;
		return ret;
	}
	if (sbhead==NULL || sbhead->firstfree==SLIST_BUCKET_SIZE) {
		sb = (slist_bucket*)malloc(sizeof(slist_bucket));
		sb->next = sbhead;
		sb->firstfree = 0;
		sbhead = sb;
	}
	ret = (sbhead->bucket)+(sbhead->firstfree);
	sbhead->firstfree++;
	return ret;
}

static inline void slist_free(slist *p) {
	p->next = slfreehead;
	slfreehead = p;
}
#else /* USE_SLIST_BUCKETS */

static inline slist* slist_malloc() {
	return (slist*)malloc(sizeof(slist));
}

static inline void slist_free(slist* p) {
	free(p);
}

#endif /* USE_SLIST_BUCKETS */
#endif /* !METARESTORE */

#ifdef USE_FLIST_BUCKETS
static inline flist* flist_malloc() {
	flist_bucket *fb;
	flist *ret;
	if (flfreehead) {
		ret = flfreehead;
		flfreehead = ret->next;
		return ret;
	}
	if (fbhead==NULL || fbhead->firstfree==FLIST_BUCKET_SIZE) {
		fb = (flist_bucket*)malloc(sizeof(flist_bucket));
		fb->next = fbhead;
		fb->firstfree = 0;
		fbhead = fb;
	}
	ret = (fbhead->bucket)+(fbhead->firstfree);
	fbhead->firstfree++;
	return ret;
}

static inline void flist_free(flist *p) {
	p->next = flfreehead;
	flfreehead = p;
}
#else /* USE_FLIST_BUCKETS */

static inline flist* flist_malloc() {
	return (flist*)malloc(sizeof(flist));
}

static inline void flist_free(flist* p) {
	free(p);
}

#endif /* USE_FLIST_BUCKETS */

#ifdef USE_CHUNK_BUCKETS
static inline chunk* chunk_malloc() {
	chunk_bucket *cb;
	chunk *ret;
	if (chfreehead) {
		ret = chfreehead;
		chfreehead = ret->next;
		return ret;
	}
	if (cbhead==NULL || cbhead->firstfree==CHUNK_BUCKET_SIZE) {
		cb = (chunk_bucket*)malloc(sizeof(chunk_bucket));
		cb->next = cbhead;
		cb->firstfree = 0;
		cbhead = cb;
	}
	ret = (cbhead->bucket)+(cbhead->firstfree);
	cbhead->firstfree++;
	return ret;
}

static inline void chunk_free(chunk *p) {
	p->next = chfreehead;
	chfreehead = p;
}
#else /* USE_CHUNK_BUCKETS */

static inline chunk* chunk_malloc() {
	return (chunk*)malloc(sizeof(chunk));
}

static inline void chunk_free(chunk* p) {
	free(p);
}

#endif /* USE_CHUNK_BUCKETS */

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
	syslog(LOG_NOTICE,"chunks: new cfg loaded - (%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32")",MaxDel,MaxRepl,LoopTime,HashSteps);
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
	newchunk = chunk_malloc();
#ifndef METARESTORE
	chunks++;
	chunkcounts[0][0]++;
#endif
	newchunk->next = chunkhash[chunkpos];
	chunkhash[chunkpos] = newchunk;
	newchunk->chunkid = chunkid;
	newchunk->version = 0;
	newchunk->goal = 0;
	newchunk->lockedto = 0;
#ifndef METARESTORE
	newchunk->validcopies = 0;
	newchunk->needverincrease = 1;
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
//	slist *s;
//	flist *f;
	if (lastchunkptr==c) {
		lastchunkid=0;
		lastchunkptr=NULL;
	}
/* not needed - function called only if slisthead==NULL and flisthead==NULL
	while ((s=c->slisthead)) {
		s = c->slisthead;
		c->slisthead = s->next;
		slist_free(s);
	}
	while ((f=c->flisthead)) {
		f = c->flisthead;
		c->flisthead = f->next;
		flist_free(f);
	}
*/
	chunks--;
	chunkcounts[c->goal][0]--;
	chunk_free(c);
}

void chunk_state_change(uint8_t oldgoal,uint8_t newgoal,uint8_t oldvc,uint8_t newvc) {
	if (oldgoal>9) {
		oldgoal=10;
	}
	if (newgoal>9) {
		newgoal=10;
	}
	if (oldvc>9) {
		oldvc=10;
	}
	if (newvc>9) {
		newvc=10;
	}
	chunkcounts[oldgoal][oldvc]--;
	chunkcounts[newgoal][newvc]++;
}

uint32_t chunk_count(void) {
	return chunks;
}

uint32_t chunk_todel_count(void) {
	return todelchunks;
}

void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *tdcopies) {
	uint32_t i,j,ag;
	*allchunks = chunks;
	*allcopies = 0;
	for (i=1 ; i<=10 ; i++) {
		ag=0;
		for (j=0 ; j<=10 ; j++) {
			ag += chunkcounts[j][i];
		}
		*allcopies += ag*i;
	}
	*tdcopies = todelchunks;
}

void chunk_store_chunkcounters(uint8_t *buff) {
	uint8_t i,j;
	for (i=0 ; i<=10 ; i++) {
		for (j=0 ; j<=10 ; j++) {
			put32bit(&buff,chunkcounts[i][j]);
		}
	}
}

void chunk_refresh_goal(chunk* c) {
	flist *f;
	uint8_t oldgoal = c->goal;
	c->goal = 0;
	for (f=c->flisthead ; f ; f=f->next) {
		if (f->goal > c->goal) {
			c->goal = f->goal;
		}
	}
	if (c->goal!=oldgoal) {
		chunk_state_change(oldgoal,c->goal,c->validcopies,c->validcopies);
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
		s = slist_malloc();
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
		s = slist_malloc();
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
			s = slist_malloc();
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
			s = slist_malloc();
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
#ifndef METARESTORE
	uint8_t oldgoal;
#endif
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
#ifndef METARESTORE
	oldgoal = c->goal;
#endif
	c->goal = 0;
	for (f=c->flisthead ; f ; f=f->next) {
		if (f->inode == inode && f->indx == indx) {
			f->goal = goal;
		}
		if (f->goal > c->goal) {
			c->goal = f->goal;
		}
	}
#ifndef METARESTORE
	if (oldgoal!=c->goal) {
		chunk_state_change(oldgoal,c->goal,c->validcopies,c->validcopies);
	}
#endif
	return STATUS_OK;
}

int chunk_delete_file(uint64_t chunkid,uint32_t inode,uint16_t indx) {
	chunk *c;
	flist *f,**fp;
#ifndef METARESTORE
	uint8_t oldgoal;
#endif
	uint32_t i;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	i=0;
#ifndef METARESTORE
	oldgoal = c->goal;
#endif
	c->goal = 0;
	fp = &(c->flisthead);
	while ((f=*fp)) {
		if (f->inode == inode && f->indx == indx) {
			*fp = f->next;
			flist_free(f);
			i=1;
		} else {
			if (f->goal > c->goal) {
				c->goal = f->goal;
			}
			fp = &(f->next);
		}
	}
	if (i==0) {
		syslog(LOG_WARNING,"(delete file) serious structure inconsistency: (chunkid:%016"PRIX64" ; inode:%"PRIu32" ; index:%"PRIu16")",chunkid,inode,indx);
	}
#ifndef METARESTORE
	if (oldgoal!=c->goal) {
		chunk_state_change(oldgoal,c->goal,c->validcopies,c->validcopies);
	}
#endif
	return STATUS_OK;
}

int chunk_add_file(uint64_t chunkid,uint32_t inode,uint16_t indx,uint8_t goal) {
	chunk *c;
	flist *f;
#ifndef METARESTORE
	uint8_t oldgoal;
#endif
	uint32_t i;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	i=0;
#ifndef METARESTORE
	oldgoal = c->goal;
#endif
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
		f = flist_malloc();
		f->inode = inode;
		f->indx = indx;
		f->goal = goal;
		f->next = c->flisthead;
		c->flisthead = f;
		if (goal > c->goal) {
			c->goal = goal;
		}
	} else {
		syslog(LOG_WARNING,"(add file) serious structure inconsistency: (chunkid:%016"PRIX64" ; inode:%"PRIu32" ; index:%"PRIu16")",chunkid,inode,indx);
	}
#ifndef METARESTORE
	if (oldgoal!=c->goal) {
		chunk_state_change(oldgoal,c->goal,c->validcopies,c->validcopies);
	}
#endif
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
//	slist *s;
//	uint8_t vc;
	*vcopies = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
/*
	vc=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && vc<255) {
			vc++;
		}
	}
*/
	*vcopies = c->validcopies;
	return STATUS_OK;
}
#endif


#ifndef METARESTORE
int chunk_multi_modify(uint64_t *nchunkid,uint64_t ochunkid,uint32_t inode,uint16_t indx,uint8_t goal,uint8_t *opflag) {
	void* ptrs[65536];
	uint16_t servcount;
	slist *os,*s;
	uint8_t oldgoal;
#else
int chunk_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint32_t inode,uint16_t indx,uint8_t goal,uint8_t opflag) {
#endif
	uint32_t i;
	chunk *oc,*c;
	flist *f,**fp;

	if (ochunkid==0) {	// new chunk
//		servcount = matocsserv_getservers_ordered(ptrs,MINMAXRND,NULL,NULL);
#ifndef METARESTORE
		servcount = matocsserv_getservers_wrandom(ptrs,goal);
		if (servcount==0) {
			uint16_t uscount,tscount;
			double minusage,maxusage;
			matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);
			if (uscount>0 && (uint32_t)(main_time())>(starttime+60)) {	// if there are chunkservers and it's at least one minute after start then it means that there is no space left
				return ERROR_NOSPACE;
			} else {
				return ERROR_NOCHUNKSERVERS;
			}
		}
#endif
		c = chunk_new(nextchunkid++);
		c->version = 1;
		c->goal = goal;
#ifndef METARESTORE
		c->interrupted = 0;
		c->operation = CREATE;
#endif
		c->flisthead = flist_malloc();
		c->flisthead->inode = inode;
		c->flisthead->indx = indx;
		c->flisthead->goal = goal;
		c->flisthead->next = NULL;
#ifndef METARESTORE
		if (servcount<goal) {
			c->validcopies = servcount;
		} else {
			c->validcopies = goal;
		}
		for (i=0 ; i<c->validcopies ; i++) {
			s = slist_malloc();
			s->ptr = ptrs[i];
			s->valid = BUSY;
			s->version = c->version;
			s->next = c->slisthead;
			c->slisthead = s;
			matocsserv_send_createchunk(s->ptr,c->chunkid,c->version);
		}
		chunk_state_change(0,c->goal,0,c->validcopies);
		*opflag=1;
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
#ifndef METARESTORE

			if (c->operation!=NONE) {
				return ERROR_CHUNKBUSY;
			}
			if (c->needverincrease) {
				i=0;
				for (s=c->slisthead ;s ; s=s->next) {
					if (s->valid!=INVALID && s->valid!=DEL) {
						if (s->valid==TDVALID || s->valid==TDBUSY) {
							s->valid = TDBUSY;
						} else {
							s->valid = BUSY;
						}
						s->version = c->version+1;
						matocsserv_send_setchunkversion(s->ptr,ochunkid,c->version+1,c->version);
						i++;
					}
				}
				if (i>0) {
					c->interrupted = 0;
					c->operation = SET_VERSION;
					c->version++;
					*opflag=1;
				} else {
					return ERROR_CHUNKLOST;
				}
			} else {
				*opflag=0;
			}
#else
			if (opflag) {
				c->version++;
			}
#endif
		} else {
			if (f==NULL) {	// it's serious structure error
#ifndef METARESTORE
				syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64" ; inode:%"PRIu32" ; index:%"PRIu16")",ochunkid,inode,indx);
#else
				printf("serious structure inconsistency: (chunkid:%016"PRIX64" ; inode:%"PRIu32" ; index:%"PRIu16")\n",ochunkid,inode,indx);
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
					s = slist_malloc();
					s->ptr = os->ptr;
					s->valid = BUSY;
					s->version = c->version;
					s->next = c->slisthead;
					c->slisthead = s;
					c->validcopies++;
					matocsserv_send_duplicatechunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version);
					i++;
				}
			}
			if (c!=NULL) {
				chunk_state_change(0,c->goal,0,c->validcopies);
			}
			if (i>0) {
#endif
				*nchunkid = c->chunkid;
#ifndef METARESTORE
				oldgoal = oc->goal;
#endif
				oc->goal = 0;
				for (f=oc->flisthead ; f ; f=f->next) {
					if (f->goal > oc->goal) {
						oc->goal = f->goal;
					}
				}
#ifndef METARESTORE
				if (oldgoal!=oc->goal) {
					chunk_state_change(oldgoal,oc->goal,oc->validcopies,oc->validcopies);
				}
				*opflag=1;
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
	uint8_t oldgoal;
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
				s->version = c->version+1;
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
				syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64" ; inode:%"PRIu32" ; index:%"PRIu16")",ochunkid,inode,indx);
#else
				printf("serious structure inconsistency: (chunkid:%016"PRIX64" ; inode:%"PRIu32" ; index:%"PRIu16")\n",ochunkid,inode,indx);
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
				s = slist_malloc();
				s->ptr = os->ptr;
				s->valid = BUSY;
				s->version = c->version;
				s->next = c->slisthead;
				c->slisthead = s;
				c->validcopies++;
				matocsserv_send_duptruncchunk(s->ptr,c->chunkid,c->version,oc->chunkid,oc->version,length);
				i++;
			}
		}
		if (c!=NULL) {
			chunk_state_change(0,c->goal,0,c->validcopies);
		}
		if (i>0) {
#endif
			*nchunkid = c->chunkid;
#ifndef METARESTORE
			oldgoal = oc->goal;
#endif
			oc->goal = 0;
			for (f=oc->flisthead ; f ; f=f->next) {
				if (f->goal > oc->goal) {
					oc->goal = f->goal;
				}
			}
#ifndef METARESTORE
			if (oldgoal!=oc->goal) {
				chunk_state_change(oldgoal,oc->goal,oc->validcopies,oc->validcopies);
			}
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

/*
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
		s = slist_malloc();
		s->ptr = ptrs[i];
		s->valid = BUSY;
		s->version = c->version;
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
*/
#ifndef METARESTORE
int chunk_repair(uint32_t inode,uint16_t indx,uint64_t ochunkid,uint32_t *nversion) {
	uint32_t bestversion;
	uint8_t oldgoal;
	uint8_t oldvc;
	chunk *c;
	flist *f,**fp;
	slist *s;

	*nversion=0;
	if (ochunkid==0) {
		return 0;	// not changed
	}

	c = chunk_find(ochunkid);
	if (c==NULL) {	// no such chunk - erase (nchunkid already is 0 - so just return with "changed" status)
		return 1;
	}
	if (c->lockedto>=(uint32_t)main_time()) { // can't repair locked chunks - but if it's locked, then likely it doesn't need to be repaired
		return 0;
	}
	bestversion = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid == VALID || s->valid == TDVALID || s->valid == BUSY || s->valid == TDBUSY) {	// found chunk that is ok - so return
			return 0;
		}
		if (s->valid == INVALID) {
			if (s->version>=bestversion) {
				bestversion = s->version;
			}
		}
	}
	if (bestversion==0) {	// didn't find sensible chunk - so erase it
		oldgoal = c->goal;
		c->goal = 0;
		fp = &(c->flisthead);
		while ((f=*fp)) {
			if (f->inode == inode && f->indx == indx) {
				*fp = f->next;
				flist_free(f);
			} else {
				if (f->goal > c->goal) {
					c->goal = f->goal;
				}
				fp = &(f->next);
			}
		}
		if (oldgoal!=c->goal) {
			chunk_state_change(oldgoal,c->goal,c->validcopies,c->validcopies);
		}
		return 1;
	}
	oldvc = c->validcopies;	// should be 0
	c->version = bestversion;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid == INVALID && s->version==bestversion) {
			s->valid = VALID;
			c->validcopies++;
		}
	}
	*nversion = bestversion;
	if (oldvc!=c->validcopies) {	// should be true
		chunk_state_change(c->goal,c->goal,oldvc,c->validcopies);
	}
	c->needverincrease=1;
	return 1;
}
#else
int chunk_set_version(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->version = version;
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
			s->version = c->version+1;
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

typedef struct locsort {
	uint32_t ip;
	uint16_t port;
	uint32_t dist;
	uint32_t rnd;
} locsort;

int chunk_locsort_cmp(const void *aa,const void *bb) {
	const locsort *a = (const locsort*)aa;
	const locsort *b = (const locsort*)bb;
	if (a->dist<b->dist) {
		return -1;
	} else if (a->dist>b->dist) {
		return 1;
	} else if (a->rnd<b->rnd) {
		return -1;
	} else if (a->rnd>b->rnd) {
		return 1;
	}
	return 0;
}

int chunk_getversionandlocations(uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t loc[100*6]) {
	chunk *c;
	slist *s;
	uint8_t i;
	uint8_t cnt;
	uint8_t *wptr;
	locsort lstab[100];

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	*version = c->version;
	cnt=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL) {
			if (cnt<100 && matocsserv_getlocation(s->ptr,&(lstab[cnt].ip),&(lstab[cnt].port))==0) {
				lstab[cnt].dist = (lstab[cnt].ip==cuip)?0:1;	// in the future prepare more sofisticated distance function
				lstab[cnt].rnd = rndu32();
				cnt++;
			}
//			sptr[cnt++]=s->ptr;
		}
	}
	qsort(lstab,cnt,sizeof(locsort),chunk_locsort_cmp);
	wptr = loc;
	for (i=0 ; i<cnt ; i++) {
		put32bit(&wptr,lstab[i].ip);
		put16bit(&wptr,lstab[i].port);
	}
//	// make random permutation
//	for (i=0 ; i<cnt ; i++) {
//		// k = random <i,j)
//		k = i+(rndu32()%(cnt-i));
//		// swap (i,k)
//		if (i!=k) {
//			void* p = sptr[i];
//			sptr[i] = sptr[k];
//			sptr[k] = p;
//		}
//	}
	*count = cnt;
	return STATUS_OK;
}

/* ---- */

void chunk_server_has_chunk(void *ptr,uint64_t chunkid,uint32_t version) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"_%08"PRIX32"), so create it for future deletion",chunkid,version);
		c = chunk_new(chunkid);
		c->version = version;
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr==ptr) {
			return;
		}
	}
	s = slist_malloc();
	s->ptr = ptr;
	if (c->version!=(version&0x7FFFFFFF)) {
		s->valid = INVALID;
		s->version = version&0x7FFFFFFF;
	} else {
		if (version&0x80000000) {
			s->valid=TDVALID;
			s->version = c->version;
			todelchunks++;
		} else {
			s->valid=VALID;
			s->version = c->version;
		}
		chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies+1);
		c->validcopies++;
	}
	s->next = c->slisthead;
	c->slisthead = s;
}

void chunk_damaged(void *ptr,uint64_t chunkid) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"), so create it for future deletion",chunkid);
		c = chunk_new(chunkid);
		c->version = 0;
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr==ptr) {
			if (s->valid==TDBUSY || s->valid==TDVALID) {
				todelchunks--;
			}
			if (s->valid!=INVALID && s->valid!=DEL) {
				chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
				c->validcopies--;
			}
			s->valid = INVALID;
			s->version = 0;
			c->needverincrease=1;
			return;
		}
	}
	s = slist_malloc();
	s->ptr = ptr;
	s->valid = INVALID;
	s->version = 0;
	s->next = c->slisthead;
	c->needverincrease=1;
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
			if (s->valid==TDBUSY || s->valid==TDVALID) {
				todelchunks--;
			}
			if (s->valid!=INVALID && s->valid!=DEL) {
				chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
				c->validcopies--;
			}
			c->needverincrease=1;
			*sptr = s->next;
			slist_free(s);
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
			st = &(c->slisthead);
			while (*st) {
				s = *st;
				if (s->ptr == ptr) {
					if (s->valid==TDBUSY || s->valid==TDVALID) {
						todelchunks--;
					}
					if (s->valid!=INVALID && s->valid!=DEL) {
						chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
						c->validcopies--;
					}
					c->needverincrease=1;
					*st = s->next;
					slist_free(s);
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
			if (s->valid!=DEL) {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					todelchunks--;
				}
				if (s->valid!=INVALID && s->valid!=DEL) {
					chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
					c->validcopies--;
				}
				syslog(LOG_WARNING,"got unexpected delete status");
			}
			*st = s->next;
			slist_free(s);
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
	if (status!=0) {
		return ;
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ptr == ptr) {
			syslog(LOG_WARNING,"got replication status from server which had had that chunk before (chunk:%016"PRIX64"_%08"PRIX32")",chunkid,version);
			if (s->valid==VALID && version!=c->version) {
				chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
				c->validcopies--;
				s->valid = INVALID;
				s->version = version;
			}
			return;
		}
	}
	s = slist_malloc();
	s->ptr = ptr;
	if (c->lockedto>=(uint32_t)main_time() || version!=c->version) {
		s->valid = INVALID;
	} else {
		chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies+1);
		c->validcopies++;
		s->valid = VALID;
	}
	s->version = version;
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
				slist_free(s);
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
				c->interrupted = 1;	// increase version after finish, just in case
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					todelchunks--;
				}
				if (s->valid!=INVALID && s->valid!=DEL) {
					chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
					c->validcopies--;
				}
				s->valid=INVALID;
				s->version = 0;	// after unfinished operation can't be shure what version chunk has
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
				c->needverincrease = 0;
			}
		} else {
			matocuserv_chunk_status(c->chunkid,ERROR_NOTDONE);
			c->operation=NONE;
		}
	}
}

void chunk_got_chunkop_status(void *ptr,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,ptr);
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
	put32bit(&buff,chunksinfo_loopstart);
	put32bit(&buff,chunksinfo_loopend);
	put32bit(&buff,chunksinfo.done.del_invalid);
	put32bit(&buff,chunksinfo.notdone.del_invalid);
	put32bit(&buff,chunksinfo.done.del_unused);
	put32bit(&buff,chunksinfo.notdone.del_unused);
	put32bit(&buff,chunksinfo.done.del_diskclean);
	put32bit(&buff,chunksinfo.notdone.del_diskclean);
	put32bit(&buff,chunksinfo.done.del_overgoal);
	put32bit(&buff,chunksinfo.notdone.del_overgoal);
	put32bit(&buff,chunksinfo.done.copy_undergoal);
	put32bit(&buff,chunksinfo.notdone.copy_undergoal);
	put32bit(&buff,chunksinfo.copy_rebalance);
}

//jobs state: jobshpos

void chunk_do_jobs(chunk *c,uint16_t scount,double minusage,double maxusage) {
	slist *s;
	static void* ptrs[65535];
	static uint16_t servcount;
	static uint32_t min,max;
	void* rptrs[65536];
	uint16_t rservcount;
	void *srcptr;
//	uint32_t ip;
//	uint16_t port;
	uint16_t i;
	uint32_t vc,tdc,ivc,bc,dc;
	static loop_info inforec;
	static uint32_t delcount;

	if (c==NULL) {
		if (scount==0) {
			servcount=0;
			delcount=0;
		} else if (scount==1) {
			uint32_t delnotdone,deldone,prevdelnotdone;
			delnotdone = inforec.notdone.del_invalid + inforec.notdone.del_unused + inforec.notdone.del_diskclean + inforec.notdone.del_overgoal;
			deldone = inforec.done.del_invalid + inforec.done.del_unused + inforec.done.del_diskclean + inforec.done.del_overgoal;
			prevdelnotdone = chunksinfo.notdone.del_invalid + chunksinfo.notdone.del_unused + chunksinfo.notdone.del_diskclean + chunksinfo.notdone.del_overgoal;
			if (delnotdone > deldone && delnotdone > prevdelnotdone) {
				TmpMaxDelFrac *= 1.3;
				TmpMaxDel = TmpMaxDelFrac;
				syslog(LOG_NOTICE,"DEL_LIMIT temporary increased to: %u/s",TmpMaxDel);
			}
			if (delnotdone < deldone && TmpMaxDelFrac > MaxDel) {
				TmpMaxDelFrac /= 1.3;
				if (TmpMaxDelFrac<MaxDel) {
					TmpMaxDelFrac = MaxDel;
				}
				TmpMaxDel = TmpMaxDelFrac;
				syslog(LOG_NOTICE,"DEL_LIMIT decreased back to: %u/s",TmpMaxDel);
			}
//			prevdeldone = chunksinfo.done.del_invalid + chunksinfo.done.del_unused + chunksinfo.done.del_diskclean + chunksinfo.done.del_overgoal;
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
	if (c->validcopies!=vc+tdc+bc) {
		syslog(LOG_WARNING,"wrong validcopies counter - (counter value: %u, should be: %u) - fixed",c->validcopies,vc+tdc+bc);
		chunk_state_change(c->goal,c->goal,c->validcopies,vc+tdc+bc);
		c->validcopies = vc+tdc+bc;
	}

//	syslog(LOG_WARNING,"chunk %016"PRIX64": ivc=%"PRIu32" , tdc=%"PRIu32" , vc=%"PRIu32" , bc=%"PRIu32" , dc=%"PRIu32" , goal=%"PRIu8,c->chunkid,ivc,tdc,vc,bc,dc,c->goal); 

// step 2. check number of copies
	if (tdc+vc+bc==0 && ivc>0 && c->flisthead) {
		syslog(LOG_WARNING,"chunk %016"PRIX64" has only invalid copies (%"PRIu32") - please repair it manually\n",c->chunkid,ivc);
		for (s=c->slisthead ; s ; s=s->next) {
			syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - invalid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(s->ptr),s->version);
		}
		return ;
	}

// step 3. delete invalid copies
	if (delcount<TmpMaxDel) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==INVALID || s->valid==DEL) {
				if (s->valid==DEL) {
					syslog(LOG_WARNING,"chunk hasn't been deleted since previous loop - retry");
				}
				s->valid = DEL;
				stats_deletions++;
				matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
				delcount++;
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
		syslog(LOG_WARNING,"chunk %016"PRIX64" has unexpected BUSY copies",c->chunkid);
		return ;
	}

// step 6. delete unused chunk
	if (c->flisthead==NULL) {
		if (delcount<TmpMaxDel) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID || s->valid==TDVALID) {
					if (s->valid==TDVALID) {
						todelchunks--;
					}
					chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
					c->validcopies--;
					c->needverincrease=1;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,c->version);
					delcount++;
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
		if (delcount<TmpMaxDel) {
			for (s=c->slisthead ; s && vc+tdc>c->goal && tdc>0 ; s=s->next) {
				if (s->valid==TDVALID) {
					todelchunks--;
					chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
					c->validcopies--;
					c->needverincrease=1;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					delcount++;
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
		if (delcount<TmpMaxDel) {
			if (servcount==0) {
				servcount = matocsserv_getservers_ordered(ptrs,ACCEPTABLE_DIFFERENCE/2.0,&min,&max);
			}
			for (i=0 ; i<servcount && vc>c->goal ; i++) {
				for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next) {}
				if (s && s->valid==VALID) {
					chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
					c->validcopies--;
					c->needverincrease=1;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					delcount++;
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
		if (delcount<TmpMaxDel) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==TDVALID) {
					todelchunks--;
					chunk_state_change(c->goal,c->goal,c->validcopies,c->validcopies-1);
					c->validcopies--;
					c->needverincrease=1;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(s->ptr,c->chunkid,0);
					delcount++;
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

//step 8. if chunk has number of copies less than goal then make another copy of this chunk
	if (c->goal > vc && vc+tdc > 0) {
		if (jobsnorepbefore<(uint32_t)main_time()) {
			uint32_t rgvc,rgtdc;
			rservcount = matocsserv_getservers_lessrepl(rptrs,MaxWriteRepl);
			rgvc=0;
			rgtdc=0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (matocsserv_replication_read_counter(s->ptr)<MaxReadRepl) {
					if (s->valid==VALID) {
						rgvc++;
					} else if (s->valid==TDVALID) {
						rgtdc++;
					}
				}
			}
			if (rgvc+rgtdc>0 && rservcount>0) { // have at least one server to read from and at least one to write to
				for (i=0 ; i<rservcount ; i++) {
					for (s=c->slisthead ; s && s->ptr!=rptrs[i] ; s=s->next) {}
					if (!s) {
						uint32_t r;
						if (rgvc>0) {	// if there are VALID copies then make copy of one VALID chunk
							r = 1+(rndu32()%rgvc);
							srcptr = NULL;
							for (s=c->slisthead ; s && r>0 ; s=s->next) {
								if (matocsserv_replication_read_counter(s->ptr)<MaxReadRepl && s->valid==VALID) {
									r--;
									srcptr = s->ptr;
								}
							}
						} else {	// if not then use TDVALID chunks.
							r = 1+(rndu32()%rgtdc);
							srcptr = NULL;
							for (s=c->slisthead ; s && r>0 ; s=s->next) {
								if (matocsserv_replication_read_counter(s->ptr)<MaxReadRepl && s->valid==TDVALID) {
									r--;
									srcptr = s->ptr;
								}
							}
						}
						if (srcptr) {
							stats_replications++;
	//						matocsserv_getlocation(srcptr,&ip,&port);
							matocsserv_send_replicatechunk(rptrs[i],c->chunkid,c->version,srcptr);
							c->needverincrease=1;
							inforec.done.copy_undergoal++;
						}
						return;
					}
				}
			}
		}
		inforec.notdone.copy_undergoal++;
	}

// step 8. if chunk has number of copies less than goal then make another copy of this chunk
/*
	if (c->goal > vc && vc+tdc > 0) {
		if (jobscopycount<MaxRepl && maxusage<=0.99 && jobsnorepbefore<(uint32_t)main_time()) {
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
						matocsserv_getlocation(srcptr,&ip,&port);
						matocsserv_send_replicatechunk(ptrs[i],c->chunkid,c->version,ip,port);
						inforec.done.copy_undergoal++;
					}
					return;
				}
			}
		} else {
			inforec.notdone.copy_undergoal++;
		}
	}
*/
	if (chunksinfo.notdone.copy_undergoal>0) {
		return;
	}

// step 9. if there is too big difference between chunkservers then make copy of chunk from server with biggest disk usage on server with lowest disk usage
	if (c->goal >= vc && vc+tdc>0 && (maxusage-minusage)>ACCEPTABLE_DIFFERENCE) {
		if (servcount==0) {
			servcount = matocsserv_getservers_ordered(ptrs,ACCEPTABLE_DIFFERENCE/2.0,&min,&max);
		}
		if (min>0 || max>0) {
			void *srcserv=NULL;
			void *dstserv=NULL;
			if (max>0) {
				for (i=0 ; i<max && srcserv==NULL ; i++) {
					if (matocsserv_replication_read_counter(ptrs[servcount-1-i])<MaxReadRepl) {
						for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next ) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srcserv=s->ptr;
						}
					}
				}
			} else {
				for (i=0 ; i<(servcount-min) && srcserv==NULL ; i++) {
					if (matocsserv_replication_read_counter(ptrs[servcount-1-i])<MaxReadRepl) {
						for (s=c->slisthead ; s && s->ptr!=ptrs[servcount-1-i] ; s=s->next ) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srcserv=s->ptr;
						}
					}
				}
			}
			if (srcserv!=NULL) {
				if (min>0) {
					for (i=0 ; i<min && dstserv==NULL ; i++) {
						if (matocsserv_replication_write_counter(ptrs[i])<MaxWriteRepl) {
							for (s=c->slisthead ; s && s->ptr!=ptrs[i] ; s=s->next ) {}
							if (s==NULL) {
								dstserv=ptrs[i];
							}
						}
					}
				} else {
					for (i=0 ; i<servcount-max && dstserv==NULL ; i++) {
						if (matocsserv_replication_write_counter(ptrs[i])<MaxWriteRepl) {
							for (s=c->slisthead ; s && s->ptr!=ptrs[i] ; s=s->next ) {}
							if (s==NULL) {
								dstserv=ptrs[i];
							}
						}
					}
				}
				if (dstserv!=NULL) {
					stats_replications++;
//					matocsserv_getlocation(srcserv,&ip,&port);
					matocsserv_send_replicatechunk(dstserv,c->chunkid,c->version,srcserv);
					c->needverincrease=1;
					inforec.copy_rebalance++;
				}
			}
		}
	}

// step 9. if there is too big difference between chunkservers then make copy on server with lowest disk usage
/*
	if (jobscopycount<MaxRepl && c->goal == vc && vc+tdc>0 && (maxusage-minusage)>ACCEPTABLE_DIFFERENCE) {
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
					matocsserv_getlocation(srcserv,&ip,&port);
					matocsserv_send_replicatechunk(dstserv,c->chunkid,c->version,ip,port);
					inforec.copy_rebalance++;
				}
			}
		}
	}
*/
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

	chunk_do_jobs(NULL,0,0.0,0.0);	// clear servercount and delcount
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
	nextchunkid = get64bit(&ptr);
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
			chunkid = get64bit(&ptr);
			if (chunkid>0) {
				if (readlast==1) {
					return -1;
				}
				c = chunk_new(chunkid);
				version = get32bit(&ptr);
				c->version = version;
			} else {
				readlast = 1;
				version = get32bit(&ptr);
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
			printf("*|i:%016"PRIX64"|v:%08"PRIX32"|g:%"PRIu8"|t:%10"PRIu32"\n",c->chunkid,c->version,c->goal,c->lockedto);
		}
	}
}

#endif

int chunk_load(FILE *fd) {
	uint8_t hdr[8];
	uint8_t loadbuff[CHUNKFSIZE*CHUNKCNT];
	const uint8_t *ptr;
	int32_t r;
	uint32_t i,j;
	chunk *c;
	int readlast;
// chunkdata
	uint64_t chunkid;
	uint32_t version,lockedto;

#ifndef METARESTORE
	chunks=0;
	todelchunks=0;
#endif
	if (fread(hdr,1,8,fd)!=8) {
		return -1;
	}
	ptr = hdr;
	nextchunkid = get64bit(&ptr);
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
			chunkid = get64bit(&ptr);
			if (chunkid>0) {
				if (readlast==1) {
					return -1;
				}
				c = chunk_new(chunkid);
				version = get32bit(&ptr);
				c->version = version;
				lockedto = get32bit(&ptr);
				c->lockedto = lockedto;
			} else {
				readlast = 1;
				version = get32bit(&ptr);
				lockedto = get32bit(&ptr);
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
	put64bit(&ptr,nextchunkid);
	fwrite(hdr,1,8,fd);
	j=0;
	ptr = storebuff;
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=chunkhash[i] ; c ; c=c->next) {
			chunkid = c->chunkid;
			put64bit(&ptr,chunkid);
			version = c->version;
			put32bit(&ptr,version);
			lockedto = c->lockedto;
			if (lockedto<now) {
				lockedto = 0;
			}
			put32bit(&ptr,lockedto);
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
#ifndef METARESTORE
	chunks = 0;
	todelchunks = 0;
#endif
	nextchunkid = 1;
}

void chunk_strinit(void) {
	uint32_t i;
#ifndef METARESTORE
	uint32_t j;
	config_getuint32("REPLICATIONS_DELAY_INIT",300,&ReplicationsDelayInit);
	config_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600,&ReplicationsDelayDisconnect);
	config_getuint32("CHUNKS_DEL_LIMIT",100,&MaxDel);
	TmpMaxDelFrac = MaxDel;
	TmpMaxDel = MaxDel;
	config_getuint32("CHUNKS_WRITE_REP_LIMIT",1,&MaxWriteRepl);
	config_getuint32("CHUNKS_READ_REP_LIMIT",5,&MaxReadRepl);
	config_getuint32("CHUNKS_LOOP_TIME",300,&LoopTime);
	HashSteps = 1+((HASHSIZE)/LoopTime);
//	config_getnewstr("CHUNKS_CONFIG",ETC_PATH "/mfschunks.cfg",&CfgFileName);
#endif
	for (i=0 ; i<HASHSIZE ; i++) {
		chunkhash[i]=NULL;
	}
#ifndef METARESTORE
	for (i=0 ; i<11 ; i++) {
		for (j=0 ; j<11 ; j++) {
			chunkcounts[i][j]=0;
		}
	}
	jobshpos = 0;
	jobsrebalancecount = 0;
	starttime = main_time();
	jobsnorepbefore = starttime+ReplicationsDelayInit;
	//jobslastdisconnect = 0;
/*
	chunk_cfg_check();
	main_timeregister(TIMEMODE_RUNONCE,30,0,chunk_cfg_check);
*/
	main_timeregister(TIMEMODE_RUNONCE,1,0,chunk_jobs_main);
#endif
}
