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

#pragma once

#include "common/platform.h"
#include "master/filesystem_node.h"

typedef struct _freenode {
	uint32_t id;
	uint32_t ftime;
	struct _freenode *next;
} freenode;

#define FREENODE_BUCKET_SIZE 5000
typedef struct _freenode_bucket {
	freenode bucket[FREENODE_BUCKET_SIZE];
	uint32_t firstfree;
	struct _freenode_bucket *next;
} freenode_bucket;

#define CUIDREC_BUCKET_SIZE 1000
typedef struct _sessionidrec_bucket {
	sessionidrec bucket[CUIDREC_BUCKET_SIZE];
	uint32_t firstfree;
	struct _sessionidrec_bucket *next;
} sessionidrec_bucket;

void fsnodes_free_id(uint32_t id, uint32_t ts);
void fsnodes_init_freebitmask(void);
void fsnodes_used_inode(uint32_t id);
void fs_periodic_freeinodes(void);
void sessionidrec_free(sessionidrec *p);

freenode *freenode_malloc();
sessionidrec *sessionidrec_malloc();
uint32_t fsnodes_get_next_id();
