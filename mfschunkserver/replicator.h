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

#ifndef _REPLICATOR_H_
#define _REPLICATOR_H_
#include <inttypes.h>

void replicator_stats(uint32_t *repl);
void replicator_new(uint64_t chunkid,uint32_t version,uint32_t ip,uint16_t port);
void replicator_cstocs_gotdata(void *e,uint64_t chunkid,uint16_t blocknum,uint16_t offset,uint32_t size,uint32_t crc,uint8_t *ptr);
void replicator_cstocs_gotstatus(void *e,uint64_t chunkid,uint8_t s);
void replicator_cstocs_connected(void *e,void *cptr);
void replicator_cstocs_disconnected(void *e);

#endif
