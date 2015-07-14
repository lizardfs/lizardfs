/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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

#include <cstdlib>

#include "common/datapack.h"

typedef struct _bstnode {
	uint32_t val, count;
	struct _bstnode *left, *right;
} bstnode;

inline void fsnodes_bst_add(bstnode **n, uint32_t val) {
	while (*n) {
		if (val < (*n)->val) {
			n = &((*n)->left);
		} else if (val > (*n)->val) {
			n = &((*n)->right);
		} else {
			(*n)->count++;
			return;
		}
	}
	(*n) = (bstnode *)malloc(sizeof(bstnode));
	passert(*n);
	(*n)->val = val;
	(*n)->count = 1;
	(*n)->left = NULL;
	(*n)->right = NULL;
}

inline uint32_t fsnodes_bst_nodes(bstnode *n) {
	if (n) {
		return 1 + fsnodes_bst_nodes(n->left) + fsnodes_bst_nodes(n->right);
	} else {
		return 0;
	}
}

inline void fsnodes_bst_storedata(bstnode *n, uint8_t **ptr) {
	if (n) {
		fsnodes_bst_storedata(n->left, ptr);
		put32bit(&*ptr, n->val);
		put32bit(&*ptr, n->count);
		fsnodes_bst_storedata(n->right, ptr);
	}
}

inline void fsnodes_bst_free(bstnode *n) {
	if (n) {
		fsnodes_bst_free(n->left);
		fsnodes_bst_free(n->right);
		free(n);
	}
}
