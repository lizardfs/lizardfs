/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015
   Skytechnology sp. z o.o..

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

#include <cstdint>
#include <cstdlib>

#define XATTR_INODE_HASH_SIZE 65536
#define XATTR_DATA_HASH_SIZE 524288
#define XATTRCHECKSUMSEED 29857986791741783ULL

struct xattr_data_entry {
	uint32_t inode;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t *attrname;
	uint8_t *attrvalue;
	uint64_t checksum;
	struct xattr_data_entry **previnode, *nextinode;
	struct xattr_data_entry **prev, *next;

	xattr_data_entry() : attrname(nullptr), attrvalue(nullptr) {
	}

	~xattr_data_entry() {
		free(attrname);
		free(attrvalue);
	}
};
void free(xattr_data_entry *);  // disable freeing using free at link time :)

struct xattr_inode_entry {
	uint32_t inode;
	uint32_t anleng;
	uint32_t avleng;
	struct xattr_data_entry *data_head;
	struct xattr_inode_entry *next;
};

#ifndef METARESTORE
static inline int xattr_namecheck(uint8_t anleng, const uint8_t *attrname) {
	uint32_t i;
	for (i = 0; i < anleng; i++) {
		if (attrname[i] == '\0') {
			return -1;
		}
	}
	return 0;
}
#endif /* METARESTORE */

static inline uint32_t xattr_data_hash_fn(uint32_t inode, uint8_t anleng, const uint8_t *attrname) {
	uint32_t hash = inode * 5381U;
	while (anleng) {
		hash = (hash * 33U) + (*attrname);
		attrname++;
		anleng--;
	}
	return (hash & (XATTR_DATA_HASH_SIZE - 1));
}

static inline uint32_t xattr_inode_hash_fn(uint32_t inode) {
	return ((inode * 0x72B5F387U) & (XATTR_INODE_HASH_SIZE - 1));
}

void xattr_checksum_add_to_background(xattr_data_entry *xde);
void xattr_listattr_data(void *xanode, uint8_t *xabuff);
void xattr_recalculate_checksum();
void xattr_removeinode(uint32_t inode);

uint8_t xattr_getattr(uint32_t inode, uint8_t anleng, const uint8_t *attrname, uint32_t *avleng,
			uint8_t **attrvalue);
uint8_t xattr_listattr_leng(uint32_t inode, void **xanode, uint32_t *xasize);
uint8_t xattr_setattr(uint32_t inode, uint8_t anleng, const uint8_t *attrname, uint32_t avleng,
			const uint8_t *attrvalue, uint8_t mode);
