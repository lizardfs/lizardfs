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

#ifndef _READDATA_H_
#define _READDATA_H_

#include <inttypes.h>

void read_inode_ops(uint32_t inode);
void* read_data_new(uint32_t inode);
void read_data_end(void *rr);
int read_data(void *rr,uint64_t offset,uint32_t *size,uint8_t **buff);
void read_data_freebuff(void *rr);
void read_data_init(uint32_t retries);
void read_data_term(void);

#endif
