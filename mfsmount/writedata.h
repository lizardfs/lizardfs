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

#ifndef _WRITEDATAALT_H_
#define _WRITEDATAALT_H_

#include <inttypes.h>

void write_data_init(uint32_t cachesize,uint32_t retries);
void write_data_term(void);
void* write_data_new(uint32_t inode);
int write_data_end(void *vid);
int write_data_flush(void *vid);
uint64_t write_data_getmaxfleng(uint32_t inode);
int write_data_flush_inode(uint32_t inode);
int write_data(void *vid,uint64_t offset,uint32_t size,const uint8_t *buff);
// uint64_t write_data_get_maxfleng(uint32_t inode);

#endif
