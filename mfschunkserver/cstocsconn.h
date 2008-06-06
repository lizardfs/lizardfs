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

#ifndef _CSTOCSCONN_H_
#define _CSTOCSCONN_H_
#include <inttypes.h>

void cstocsconn_stats(uint32_t *bin,uint32_t *bout);
void cstocsconn_replinit(void *e,uint64_t chunkid, uint32_t version);
//void cstocsconn_readinit(void *e,uint64_t chunkid, uint32_t version/*, uint32_t offset, uint32_t size*/);
void cstocsconn_sendwrite(void *e,uint64_t chunkid,uint32_t version,uint8_t *chain,uint32_t chainleng);
void cstocsconn_sendwritedata(void *e,uint64_t chunkid,uint32_t writeid,uint16_t blocknum,uint16_t offset,uint32_t size,uint32_t crc,uint8_t *data);
//void cstocsconn_sendwritedone(void *e,uint64_t chunkid);
void cstocsconn_delete(void *e);
int cstocsconn_queueisfilled(void *e);
uint8_t cstocsconn_newservconnection(uint32_t ip,uint16_t port,void *p);
uint8_t cstocsconn_newreplconnection(uint32_t ip,uint16_t port,void *p);
int cstocsconn_init(void);

#endif
