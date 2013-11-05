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

#ifndef _SYMLINKCACHE_H_
#define _SYMLINKCACHE_H_

#include <inttypes.h>

void symlink_cache_insert(uint32_t inode,const uint8_t *path);
int symlink_cache_search(uint32_t inode,const uint8_t **path);
void symlink_cache_init(void);
void symlink_cache_term(void);

#endif
