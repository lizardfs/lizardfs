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

#ifndef _MATOCUSERV_H_
#define _MATOCUSERV_H_
#include <inttypes.h>

void matocuserv_chunk_status(uint64_t chunkid,uint8_t status);
void matocuserv_init_sessions(uint32_t sessionid,uint32_t inode);
int matocuserv_init(void);

#endif
