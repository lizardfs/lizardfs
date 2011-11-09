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

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>

#include "stats.h"
#include "MFSCommunication.h"

#define HASH_FUNCTIONS 4
#define HASH_BUCKET_SIZE 16
#define HASH_BUCKETS 6257

// entries in cache = HASH_FUNCTIONS*HASH_BUCKET_SIZE*HASH_BUCKETS
// 4 * 16 * 6257 = 400448
// Symlink cache capacity can be easly changed by altering HASH_BUCKETS value.
// Any number should work but it is better to use prime numers here.

typedef struct _hashbucket {
	uint32_t inode[HASH_BUCKET_SIZE];
	uint32_t time[HASH_BUCKET_SIZE];
	uint8_t* path[HASH_BUCKET_SIZE];
//	uint16_t multihit;
} hashbucket;

static hashbucket *symlinkhash = NULL;
static pthread_mutex_t slcachelock = PTHREAD_MUTEX_INITIALIZER;

enum {
	INSERTS = 0,
	SEARCH_HITS,
	SEARCH_MISSES,
	LINKS,
	STATNODES
};

static uint64_t *statsptr[STATNODES];

static inline void symlink_cache_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"symlink_cache",0);
	statsptr[INSERTS] = stats_get_counterptr(stats_get_subnode(s,"inserts",0));
	statsptr[SEARCH_HITS] = stats_get_counterptr(stats_get_subnode(s,"search_hits",0));
	statsptr[SEARCH_MISSES] = stats_get_counterptr(stats_get_subnode(s,"search_misses",0));
	statsptr[LINKS] = stats_get_counterptr(stats_get_subnode(s,"#links",1));
}

static inline void symlink_cache_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_lock();
		(*statsptr[id])++;
		stats_unlock();
	}
}

static inline void symlink_cache_stats_dec(uint8_t id) {
	if (id<STATNODES) {
		stats_lock();
		(*statsptr[id])--;
		stats_unlock();
	}
}

void symlink_cache_insert(uint32_t inode,const uint8_t *path) {
	uint32_t primes[HASH_FUNCTIONS] = {1072573589U,3465827623U,2848548977U,748191707U};
	hashbucket *hb,*fhb;
	uint8_t h,i,fi;
	uint32_t now;
	uint32_t mints;

	now = time(NULL);
	mints = UINT32_MAX;
	fi = 0;
	fhb = NULL;

	symlink_cache_stats_inc(INSERTS);
	pthread_mutex_lock(&slcachelock);
	for (h=0 ; h<HASH_FUNCTIONS ; h++) {
		hb = symlinkhash + ((inode*primes[h])%HASH_BUCKETS);
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->inode[i]==inode) {
				if (hb->path[i]) {
					free(hb->path[i]);
				}
				hb->path[i]=(uint8_t*)strdup((const char *)path);
				hb->time[i]=now;
				pthread_mutex_unlock(&slcachelock);
				return;
			}
			if (hb->time[i]<mints) {
				fhb = hb;
				fi = i;
				mints = hb->time[i];
			}
		}
	}
	if (fhb) {	// just sanity check
		if (fhb->time[fi]==0) {
			symlink_cache_stats_inc(LINKS);
		}
		if (fhb->path[fi]) {
			free(fhb->path[fi]);
		}
		fhb->inode[fi]=inode;
		fhb->path[fi]=(uint8_t*)strdup((const char *)path);
		fhb->time[fi]=now;
	}
	pthread_mutex_unlock(&slcachelock);
}

int symlink_cache_search(uint32_t inode,const uint8_t **path) {
	uint32_t primes[HASH_FUNCTIONS] = {1072573589U,3465827623U,2848548977U,748191707U};
	hashbucket *hb;
	uint8_t h,i;
	uint32_t now;

	now = time(NULL);

	pthread_mutex_lock(&slcachelock);
	for (h=0 ; h<HASH_FUNCTIONS ; h++) {
		hb = symlinkhash + ((inode*primes[h])%HASH_BUCKETS);
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->inode[i]==inode) {
				if (hb->time[i]+MFS_INODE_REUSE_DELAY<now) {
					if (hb->path[i]) {
						free(hb->path[i]);
						hb->path[i]=NULL;
					}
					hb->time[i]=0;
					hb->inode[i]=0;
					pthread_mutex_unlock(&slcachelock);
					symlink_cache_stats_dec(LINKS);
					symlink_cache_stats_inc(SEARCH_MISSES);
					return 0;
				}
				*path = hb->path[i];
				pthread_mutex_unlock(&slcachelock);
				symlink_cache_stats_inc(SEARCH_HITS);
				return 1;
			}
		}
	}
	pthread_mutex_unlock(&slcachelock);
	symlink_cache_stats_inc(SEARCH_MISSES);
	return 0;
}

void symlink_cache_init(void) {
	symlinkhash = malloc(sizeof(hashbucket)*HASH_BUCKETS);
	memset(symlinkhash,0,sizeof(hashbucket)*HASH_BUCKETS);
	symlink_cache_statsptr_init();
}

void symlink_cache_term(void) {
	hashbucket *hb;
	uint8_t i;
	uint32_t hi;

	pthread_mutex_lock(&slcachelock);
	for (hi=0 ; hi<HASH_BUCKETS ; hi++) {
		hb = symlinkhash + hi;
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->path[i]) {
				free(hb->path[i]);
			}
		}
	}
	free(symlinkhash);
	pthread_mutex_unlock(&slcachelock);
}
