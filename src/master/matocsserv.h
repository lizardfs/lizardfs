/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

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

#include <inttypes.h>
#include <vector>

#include "common/chunk_type.h"
#include "common/goal.h"

struct matocsserventry;

int matocsserv_csdb_remove_server(uint32_t ip,uint16_t port);
void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount);
uint16_t matocsserv_getservers_ordered(void* ptrs[65535],double maxusagediff,uint32_t *min,uint32_t *max);
std::vector<std::pair<matocsserventry*, ChunkType>>
		matocsserv_getservers_for_new_chunk(uint8_t desiredGoal);
void matocsserv_getservers_lessrepl(std::vector<void*>& ptrs, uint16_t replimit);
void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace);
const char* matocsserv_getstrip(void *e);
int matocsserv_getlocation(void *e,uint32_t *servip,uint16_t *servport);
uint16_t matocsserv_replication_read_counter(void *e);
uint16_t matocsserv_replication_write_counter(void *e);
uint16_t matocsserv_deletion_counter(void *e);
uint32_t matocsserv_cservlist_size(void);
void matocsserv_cservlist_data(uint8_t *ptr);
int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint32_t version,void *src);
int matocsserv_send_liz_replicatechunk(void* e, uint64_t chunkid, uint32_t version, ChunkType type,
		const std::vector<void*> &sourcePointers, const std::vector<ChunkType> &sourceTypes);
int matocsserv_send_chunkop(void *e,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t leng);
int matocsserv_send_deletechunk(void *e, uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType);
int matocsserv_send_createchunk(void *e, uint64_t chunkid, ChunkType chunkType, uint32_t version);
int matocsserv_send_setchunkversion(void *e, uint64_t chunkId, uint32_t newVersion,
		uint32_t chunkVersion, ChunkType chunkType);
int matocsserv_send_duplicatechunk(void *e, uint64_t newChunkId, uint32_t newChunkVersion,
		ChunkType chunkType, uint64_t chunkId, uint32_t chunkVersion);
void matocsserv_send_truncatechunk(void *e, uint64_t chunkid, ChunkType chunkType, uint32_t length,
		uint32_t version, uint32_t oldversion);
int matocsserv_send_duptruncchunk(void *e, uint64_t newChunkId, uint32_t newChunkVersion,
		ChunkType chunkType, uint64_t chunkId, uint32_t chunkVersion, uint32_t length);
uint32_t matocsserv_get_version(void *e);
int matocsserv_init(void);
