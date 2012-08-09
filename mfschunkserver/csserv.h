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

#ifndef _CSSERV_H_
#define _CSSERV_H_

#include <inttypes.h>

void csserv_stats(uint64_t *bin,uint64_t *bout,uint32_t *hlopr,uint32_t *hlopw,uint32_t *maxjobscnt);
// void csserv_cstocs_connected(void *e,void *cptr);
// void csserv_cstocs_gotstatus(void *e,uint64_t chunkid,uint32_t writeid,uint8_t s);
// void csserv_cstocs_disconnected(void *e);
uint32_t csserv_getlistenip();
uint16_t csserv_getlistenport();
int csserv_init(void);

#endif
