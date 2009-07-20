/*
   Copyright 2008 Gemius SA.

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

#ifndef _WRITEDATA_H_
#define _WRITEDATA_H_

#include <inttypes.h>

void write_data_init();
int write_data_flush_inode(uint32_t inode);
void* write_data_new(uint32_t inode);
void write_data_end(void *wr);
int write_data_flush(void *wr);
int write_data(void *wr,uint64_t offset,uint32_t size,const uint8_t *buff);

#endif
