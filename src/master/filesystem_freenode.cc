/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2015 Skytechnology sp. z o.o..

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
#include "master/filesystem_freenode.h"

#include "common/main.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_operations.h"

sessionidrec *sessionidrec_malloc() {
	sessionidrec_bucket *crb;
	sessionidrec *ret;
	if (gMetadata->crfreehead) {
		ret = gMetadata->crfreehead;
		gMetadata->crfreehead = ret->next;
		return ret;
	}
	if (gMetadata->crbhead == NULL || gMetadata->crbhead->firstfree == CUIDREC_BUCKET_SIZE) {
		crb = (sessionidrec_bucket *)malloc(sizeof(sessionidrec_bucket));
		passert(crb);
		crb->next = gMetadata->crbhead;
		crb->firstfree = 0;
		gMetadata->crbhead = crb;
	}
	ret = (gMetadata->crbhead->bucket) + (gMetadata->crbhead->firstfree);
	gMetadata->crbhead->firstfree++;
	return ret;
}

void sessionidrec_free(sessionidrec *p) {
	p->next = gMetadata->crfreehead;
	gMetadata->crfreehead = p;
}

freenode *freenode_malloc() {
	freenode_bucket *fnb;
	freenode *ret;
	if (gMetadata->fnfreehead) {
		ret = gMetadata->fnfreehead;
		gMetadata->fnfreehead = ret->next;
		return ret;
	}
	if (gMetadata->fnbhead == NULL || gMetadata->fnbhead->firstfree == FREENODE_BUCKET_SIZE) {
		fnb = (freenode_bucket *)malloc(sizeof(freenode_bucket));
		passert(fnb);
		fnb->next = gMetadata->fnbhead;
		fnb->firstfree = 0;
		gMetadata->fnbhead = fnb;
	}
	ret = (gMetadata->fnbhead->bucket) + (gMetadata->fnbhead->firstfree);
	gMetadata->fnbhead->firstfree++;
	return ret;
}

static inline void freenode_free(freenode *p) {
	p->next = gMetadata->fnfreehead;
	gMetadata->fnfreehead = p;
}

uint32_t fsnodes_get_next_id() {
	uint32_t i, mask;
	while (gMetadata->searchpos < gMetadata->bitmasksize &&
	       gMetadata->freebitmask[gMetadata->searchpos] == 0xFFFFFFFF) {
		gMetadata->searchpos++;
	}
	if (gMetadata->searchpos == gMetadata->bitmasksize) {  // no more freeinodes
		uint32_t *tmpfbm;
		gMetadata->bitmasksize += 0x80;
		tmpfbm = gMetadata->freebitmask;
		gMetadata->freebitmask = (uint32_t *)realloc(
		        gMetadata->freebitmask, gMetadata->bitmasksize * sizeof(uint32_t));
		if (gMetadata->freebitmask == NULL) {
			free(tmpfbm);  // pro forma - satisfy cppcheck
		}
		passert(gMetadata->freebitmask);
		memset(gMetadata->freebitmask + gMetadata->searchpos, 0, 0x80 * sizeof(uint32_t));
	}
	mask = gMetadata->freebitmask[gMetadata->searchpos];
	i = 0;
	while (mask & 1) {
		i++;
		mask >>= 1;
	}
	mask = 1 << i;
	gMetadata->freebitmask[gMetadata->searchpos] |= mask;
	i += (gMetadata->searchpos << 5);
	if (i > gMetadata->maxnodeid) {
		gMetadata->maxnodeid = i;
	}
	return i;
}

void fsnodes_free_id(uint32_t id, uint32_t ts) {
	freenode *n;
	n = freenode_malloc();
	n->id = id;
	n->ftime = ts;
	n->next = NULL;
	*gMetadata->freetail = n;
	gMetadata->freetail = &(n->next);
}

static uint32_t fs_do_freeinodes(uint32_t ts) {
	uint32_t pos, mask;
	freenode *n, *an;
	uint32_t fi = 0;
	n = gMetadata->freelist;
	while (n && n->ftime + MFS_INODE_REUSE_DELAY < ts) {
		fi++;
		pos = (n->id >> 5);
		mask = 1 << (n->id & 0x1F);
		gMetadata->freebitmask[pos] &= ~mask;
		if (pos < gMetadata->searchpos) {
			gMetadata->searchpos = pos;
		}
		an = n->next;
		freenode_free(n);
		n = an;
	}
	if (n) {
		gMetadata->freelist = n;
	} else {
		gMetadata->freelist = NULL;
		gMetadata->freetail = &(gMetadata->freelist);
	}
	return fi;
}

uint8_t fs_apply_freeinodes(uint32_t ts, uint32_t freeinodes) {
	uint32_t fi = fs_do_freeinodes(ts);
	gMetadata->metaversion++;
	if (freeinodes != fi) {
		return LIZARDFS_ERROR_MISMATCH;
	}
	return 0;
}

#ifndef METARESTORE
void fs_periodic_freeinodes(void) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	uint32_t fi = fs_do_freeinodes(ts);
	if (fi > 0) {
		fs_changelog(ts, "FREEINODES():%" PRIu32, fi);
	}
}
#endif

void fsnodes_init_freebitmask(void) {
	gMetadata->bitmasksize = 0x100 + (((gMetadata->maxnodeid) >> 5) & 0xFFFFFF80U);
	gMetadata->freebitmask = (uint32_t *)malloc(gMetadata->bitmasksize * sizeof(uint32_t));
	passert(gMetadata->freebitmask);
	memset(gMetadata->freebitmask, 0, gMetadata->bitmasksize * sizeof(uint32_t));
	gMetadata->freebitmask[0] = 1;  // reserve inode 0
	gMetadata->searchpos = 0;
}

void fsnodes_used_inode(uint32_t id) {
	uint32_t pos, mask;
	pos = id >> 5;
	mask = 1 << (id & 0x1F);
	gMetadata->freebitmask[pos] |= mask;
}
