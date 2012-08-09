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

#ifndef _MATOCSSERV_H_
#define _MATOCSSERV_H_

#include <inttypes.h>

int matocsserv_csdb_remove_server(uint32_t ip,uint16_t port);
void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount);
uint16_t matocsserv_getservers_ordered(void* ptrs[65535],double maxusagediff,uint32_t *min,uint32_t *max);
uint16_t matocsserv_getservers_wrandom(void* ptrs[65535],uint16_t demand);
uint16_t matocsserv_getservers_lessrepl(void* ptrs[65535],uint16_t replimit);
void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace);
char* matocsserv_getstrip(void *e);
int matocsserv_getlocation(void *e,uint32_t *servip,uint16_t *servport);
uint16_t matocsserv_replication_read_counter(void *e);
uint16_t matocsserv_replication_write_counter(void *e);
uint16_t matocsserv_deletion_counter(void *e);
uint32_t matocsserv_cservlist_size(void);
void matocsserv_cservlist_data(uint8_t *ptr);
int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint32_t version,void *src);
int matocsserv_send_replicatechunk_xor(void *e,uint64_t chunkid,uint32_t version,uint8_t cnt,void **src,uint64_t *srcchunkid,uint32_t *srcversion);
//int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint32_t version,uint32_t ip,uint16_t port);
// fromdata: cnt*(chunkid:64 version:32 ip:32 port:16)
//int matocsserv_send_replicatechunk_xor(void *e,uint64_t chunkid,uint32_t version,uint8_t cnt,uint8_t *fromdata);
int matocsserv_send_chunkop(void *e,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t leng);
int matocsserv_send_deletechunk(void *e,uint64_t chunkid,uint32_t version);
int matocsserv_send_createchunk(void *e,uint64_t chunkid,uint32_t version);
int matocsserv_send_setchunkversion(void *e,uint64_t chunkid,uint32_t version,uint32_t oldversion);
int matocsserv_send_duplicatechunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion);
int matocsserv_send_truncatechunk(void *e,uint64_t chunkid,uint32_t length,uint32_t version,uint32_t oldversion);
int matocsserv_send_duptruncchunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion,uint32_t length);
//void matocsserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize);
//void matocsserv_broadcast_logrotate();
int matocsserv_init(void);

#endif
