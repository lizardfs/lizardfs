#include "config.h"

#include <fuse_lowlevel.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "datapack.h"

typedef struct _dircache {
	struct fuse_ctx ctx;
	uint32_t parent;
	const uint8_t *dbuff;
	uint32_t dsize;
	struct _dircache *next,**prev;
} dircache;

static dircache *head;
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

void* dcache_new(const struct fuse_ctx *ctx,uint32_t parent,const uint8_t *dbuff,uint32_t dsize) {
	dircache *d;
	d = malloc(sizeof(dircache));
	d->ctx.pid = ctx->pid;
	d->ctx.uid = ctx->uid;
	d->ctx.gid = ctx->gid;
	d->parent = parent;
	d->dbuff = dbuff;
	d->dsize = dsize;
	pthread_mutex_lock(&glock);
	if (head) {
		head->prev = &(d->next);
	}
	d->next = head;
	d->prev = &head;
	head = d;
	pthread_mutex_unlock(&glock);
	return d;
}

void dcache_release(void *r) {
	dircache *d = (dircache*)r;
	pthread_mutex_lock(&glock);
	if (d->next) {
		d->next->prev = d->prev;
	}
	*(d->prev) = d->next;
	free(d);
	pthread_mutex_unlock(&glock);
}

static inline uint8_t dcache_namesearch(const uint8_t *dbuff,uint32_t dsize,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
	ptr = dbuff;
	eptr = dbuff+dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr && enleng==nleng && memcmp(ptr+1,name,enleng)==0) {
			ptr+=1+enleng;
			*inode = get32bit(&ptr);
			memcpy(attr,ptr,35);
			return 1;
		}
		ptr+=enleng+40;
	}
	return 0;
}

static inline uint8_t dcache_inodesearch(const uint8_t *dbuff,uint32_t dsize,uint32_t inode,uint8_t attr[35]) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
	ptr = dbuff;
	eptr = dbuff+dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr) {
			ptr+=1+enleng;
			if (inode==get32bit(&ptr)) {
				memcpy(attr,ptr,35);
				return 1;
			} else {
				ptr+=35;
			}
		} else {
			return 0;
		}
	}
	return 0;
}


uint8_t dcache_lookup(const struct fuse_ctx *ctx,uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]) {
	dircache *d;
	pthread_mutex_lock(&glock);
	for (d=head ; d ; d=d->next) {
		if (parent==d->parent && ctx->pid==d->ctx.pid && ctx->uid==d->ctx.uid && ctx->gid==d->ctx.gid) {
			if (dcache_namesearch(d->dbuff,d->dsize,nleng,name,inode,attr)) {
				pthread_mutex_unlock(&glock);
				return 1;
			}
		}
	}
	pthread_mutex_unlock(&glock);
	return 0;
}

uint8_t dcache_getattr(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35]) {
	dircache *d;
	pthread_mutex_lock(&glock);
	for (d=head ; d ; d=d->next) {
		if (ctx->pid==d->ctx.pid && ctx->uid==d->ctx.uid && ctx->gid==d->ctx.gid) {
			if (dcache_inodesearch(d->dbuff,d->dsize,inode,attr)) {
				pthread_mutex_unlock(&glock);
				return 1;
			}
		}
	}
	pthread_mutex_unlock(&glock);
	return 0;
}
