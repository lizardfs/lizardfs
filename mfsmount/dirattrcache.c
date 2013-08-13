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

#include <fuse_lowlevel.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "datapack.h"

#define DCACHE_HASH_SIZE 100003
#define INDEX_SLOT 8
#define INDEX_HASH_SIZE 100003

typedef struct _dircache {
	uint32_t parent;
	uint8_t *dbuff;
	uint32_t dsize;
	uint32_t hashsize;
	const uint8_t **namehashtab;
	const uint8_t **inodehashtab;
} dircache;

typedef struct _inode_parent {
    uint32_t inode;
    uint32_t parent;
} inode_parent;

static inode_parent parents[INDEX_HASH_SIZE][INDEX_SLOT];
static dircache *dcache[DCACHE_HASH_SIZE];
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

static void add_parent(uint32_t inode, uint32_t parent) {
    int i;
    inode_parent *ptr = parents[inode%INDEX_HASH_SIZE];
    for (i=0; i<INDEX_SLOT; i++) {
        if (ptr[i].inode == 0) {
            ptr[i].inode = inode;
            ptr[i].parent = parent;
            ptr[(i+1)%INDEX_SLOT].inode = 0;
            break;
        }
    }
}

static uint32_t get_parent(uint32_t inode) {
    int i;
    inode_parent *ptr = parents[inode%INDEX_HASH_SIZE];
    for (i=0; i<INDEX_SLOT; i++) {
        if (ptr[i].inode == inode) {
            return ptr[i].parent;
        }
    }
    return 0;
}

static inline uint32_t inode_hash(uint32_t inode) {
    return (inode*0x5F2318BD)%DCACHE_HASH_SIZE;
}

static inline uint32_t dcache_hash(const uint8_t *name,uint8_t nleng) {
	uint32_t hash=5381;
	while (nleng>0) {
		hash = ((hash<<5)+hash)^(*name);
		name++;
		nleng--;
	}
	return hash;
}

uint32_t dcache_elemcount(const uint8_t *dbuff,uint32_t dsize) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
	uint32_t ret;
	ptr = dbuff;
	eptr = dbuff+dsize;
	ret=0;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr) {
			ret++;
		}
		ptr+=enleng+40;
	}
	return ret;
}

static inline void dcache_calchashsize(dircache *d) {
	uint32_t cnt = dcache_elemcount(d->dbuff,d->dsize);
	d->hashsize = 1;
	cnt = (cnt*3)/2;
	while (cnt) {
		d->hashsize<<=1;
		cnt>>=1;
	}
}

void dcache_makenamehash(dircache *d) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
	uint32_t hash,disp;
	uint32_t hashmask;

	if (d->hashsize==0) {
		dcache_calchashsize(d);
	}
	hashmask = d->hashsize-1;
	d->namehashtab = malloc(sizeof(uint8_t*)*d->hashsize);
    if (!d->namehashtab) return;
	memset(d->namehashtab,0,sizeof(uint8_t*)*d->hashsize);

	ptr = d->dbuff;
	eptr = d->dbuff+d->dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr) {
			hash = dcache_hash(ptr+1,enleng);
			disp = ((hash*0x53B23891)&hashmask)|1;
			while (d->namehashtab[hash&hashmask]) {
				hash+=disp;
			}
			d->namehashtab[hash&hashmask]=ptr;
		}
		ptr+=enleng+40;
	}
}

void dcache_makeinodehash(dircache *d) {
	const uint8_t *iptr,*ptr,*eptr;
	uint8_t enleng;
	uint32_t hash,disp;
	uint32_t hashmask;

	if (d->hashsize==0) {
		dcache_calchashsize(d);
	}
	hashmask = d->hashsize-1;
	d->inodehashtab = malloc(sizeof(uint8_t*)*d->hashsize);
    if (!d->inodehashtab) return;
	memset(d->inodehashtab,0,sizeof(uint8_t*)*d->hashsize);

	ptr = d->dbuff;
	eptr = d->dbuff+d->dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr) {
			iptr = ptr+1+enleng;
			hash = get32bit(&iptr);
            add_parent(hash, d->parent);
			disp = ((hash*0x53B23891)&hashmask)|1;
			hash *= 0xB28E457D;
			while (d->inodehashtab[hash&hashmask]) {
				hash+=disp;
			}
			d->inodehashtab[hash&hashmask]=ptr+1+enleng;
		}
		ptr+=enleng+40;
	}
}

static void dcache_free(dircache *d) {
    if (d->namehashtab) {
        free(d->namehashtab);
    }
    if (d->inodehashtab) {
        free(d->inodehashtab);
    }
    free(d->dbuff);
    free(d);
}

void dcache_init() {
	pthread_mutex_lock(&glock);
    memset(dcache, 0, sizeof(dcache));
    memset(parents, 0, sizeof(parents));
	pthread_mutex_unlock(&glock);
}

uint32_t dcache_replace(uint32_t parent,const uint8_t *dbuff,uint32_t dsize) {
    uint32_t old=0;
    uint32_t hash=inode_hash(parent);
	dircache *d;
    
	pthread_mutex_lock(&glock);
    if (dcache[hash]&&dcache[hash]->parent==parent) {
        goto RET;
    }

	d = malloc(sizeof(dircache));
    if (!d) {
        goto RET;
    }
	d->parent = parent;
	d->dbuff = (uint8_t*)malloc(dsize);
    if (!d->dbuff) {
        free(d);
        goto RET;
    }
    memcpy(d->dbuff, dbuff, dsize);
	d->dsize = dsize;
	d->hashsize = 0;
	d->namehashtab = NULL;
    dcache_makeinodehash(d);
    dcache_makenamehash(d);
    if (!d->namehashtab || !d->inodehashtab) {
        dcache_free(d);
        goto RET;
    }

    if (dcache[hash]) {
        old = dcache[hash]->parent;
        dcache_free(dcache[hash]);
    }
	dcache[hash] = d;
RET:    
	pthread_mutex_unlock(&glock);
    return old;
}

uint8_t dcache_remove(uint32_t parent) {
    dircache *d;
    uint32_t hash=inode_hash(parent);
    pthread_mutex_lock(&glock);
    d = dcache[hash];
    if (d && d->parent == parent) {
        dcache_free(d);
        dcache[hash] = NULL;
        pthread_mutex_unlock(&glock);
        return 1;
    }
    pthread_mutex_unlock(&glock);
    return 0;
}

void dcache_remove_all() {
    uint32_t h;
    pthread_mutex_lock(&glock);
    for (h=0; h<DCACHE_HASH_SIZE; h++) {
        if (dcache[h]) {
            dcache_free(dcache[h]);
        }
    }
    memset(dcache,0,sizeof(dcache));
    memset(parents,0,sizeof(parents));
    pthread_mutex_unlock(&glock);
}

static inline uint8_t dcache_namehashsearch(dircache *d,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]) {
	uint32_t hash,disp,hashmask;
	const uint8_t *ptr;

	hashmask = d->hashsize-1;
	hash = dcache_hash(name,nleng);
	disp = ((hash*0x53B23891)&hashmask)|1;
    *inode = 0;
	while ((ptr=d->namehashtab[hash&hashmask])) {
		if (*ptr==nleng && memcmp(ptr+1,name,nleng)==0) {
			ptr+=1+nleng;
			*inode = get32bit(&ptr);
			memcpy(attr,ptr,35);
			return 1;
		}
		hash+=disp;
	}
	return 0;
}

static inline uint8_t dcache_inodehashsearch(dircache *d,uint32_t inode,uint8_t **attr) {
	uint32_t hash,disp,hashmask;
	const uint8_t *ptr;

	hashmask = d->hashsize-1;
	hash = inode*0xB28E457D;
	disp = ((inode*0x53B23891)&hashmask)|1;
	while ((ptr=d->inodehashtab[hash&hashmask])) {
		if (inode==get32bit(&ptr)) {
            *attr = ptr;
			return 1;
		}
		hash+=disp;
	}
	return 0;
}

uint8_t dcache_getdir(uint32_t parent,uint8_t **dbuff,uint32_t *dsize) {
	dircache *d;
    uint32_t hash=inode_hash(parent);
    uint8_t r=0;
	pthread_mutex_lock(&glock);
    d = dcache[hash];
	if (d && parent==d->parent) {
        *dsize=d->dsize;
        *dbuff=malloc(*dsize);
        if (*dbuff) {
            memcpy(*dbuff,d->dbuff,*dsize);
            r = 1;
        }
	}
	pthread_mutex_unlock(&glock);
	return r;
}

uint8_t dcache_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]) {
	dircache *d;
    uint32_t hash=inode_hash(parent);
    uint8_t r=0;
	pthread_mutex_lock(&glock);
    d = dcache[hash];
	if (d && parent==d->parent) {
	    dcache_namehashsearch(d,nleng,name,inode,attr);
        r = 1;
	}
	pthread_mutex_unlock(&glock);
	return r;
}

// miss when hash collipsion
uint8_t dcache_getattr(uint32_t inode,uint8_t attr[35]) {
	dircache *d;
    uint8_t *ptr;
    uint32_t hash,parent;
	pthread_mutex_lock(&glock);
    parent=get_parent(inode);
    if (parent==0) {
	    pthread_mutex_unlock(&glock);
        return 0;
    }
    hash=inode_hash(parent);
	d=dcache[hash];
	if (d && d->parent==parent) {
		if (dcache_inodehashsearch(d,inode,&ptr)) {
            memcpy(attr,ptr,35);
			pthread_mutex_unlock(&glock);
            return 1;
		}
	}
	pthread_mutex_unlock(&glock);
	return 0;
}

uint8_t dcache_setattr(uint32_t parent,uint32_t inode,uint8_t attr[35]) {
	dircache *d;
    uint8_t *ptr;
    uint32_t hash;
	pthread_mutex_lock(&glock);
    if (parent==0) {
        parent=get_parent(inode);
    }
    if (parent) {
        hash = inode_hash(parent);
        d = dcache[hash];
        if (d && d->parent==parent) {
            if (dcache_inodehashsearch(d,inode,&ptr)) {
                memcpy(ptr,attr,35);
            }
        }
    }
	pthread_mutex_unlock(&glock);
	return 0;
}
