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

uint32_t fsnodes_get_next_id(uint32_t ts, uint32_t req_inode) {
	if(req_inode == 0 || !gMetadata->inode_pool.markAsAcquired(req_inode,ts)) {
		req_inode = gMetadata->inode_pool.acquire(ts);
	}
	if (req_inode == 0) {
		mabort("Out of free inode numbers");
	}
	if (req_inode > gMetadata->maxnodeid) {
		gMetadata->maxnodeid = req_inode;
	}

	return req_inode;
}

uint8_t fs_apply_freeinodes(uint32_t /*ts*/, uint32_t /*freeinodes*/) {
	// left for compatibility when reading from old metadata change log
	gMetadata->metaversion++;
	return 0;
}
