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
#include <stdio.h>

#include "common/chunk_with_address_and_label.h"
#include "common/chunks_availability_state.h"
#include "master/checksum.h"

struct matocsserventry;

int chunk_increase_version(uint64_t chunkid);
int chunk_set_version(uint64_t chunkid,uint32_t version);
int chunk_change_file(uint64_t chunkid,uint8_t prevgoal,uint8_t newgoal);
int chunk_delete_file(uint64_t chunkid,uint8_t goal);
int chunk_add_file(uint64_t chunkid,uint8_t goal);
int chunk_unlock(uint64_t chunkid);
uint8_t chunk_apply_modification(uint32_t ts, uint64_t oldChunkId, uint8_t goal,
		bool doIncreaseVersion, uint64_t *newChunkId);

// Tries to set next chunk id to a passed value, returns status
uint8_t chunk_set_next_chunkid(uint64_t nextChunkIdToBeSet);

#ifdef METARESTORE
void chunk_dump(void);
#else
uint8_t chunk_multi_modify(uint64_t ochunkid, uint8_t goal, bool quota_exceeded,
		uint8_t *opflag, uint64_t *nchunkid);
uint8_t chunk_multi_truncate(uint64_t ochunkid, uint32_t length, uint8_t goal, bool quota_exceeded,
		uint64_t *nchunkid);
void chunk_stats(uint32_t *del,uint32_t *repl);
void chunk_store_info(uint8_t *buff);
uint32_t chunk_get_missing_count(void);
void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid);
uint32_t chunk_count(void);
const ChunksReplicationState& chunk_get_replication_state(bool regularChunksOnly);
const ChunksAvailabilityState& chunk_get_availability_state(bool regularChunksOnly);
void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regcopies);

/// Checks if the given chunk has only invalid copies (ie. needs to be repaired).
bool chunk_has_only_invalid_copies(uint64_t chunkid);

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies);
int chunk_repair(uint8_t goal,uint64_t ochunkid,uint32_t *nversion);

int chunk_getversionandlocations(uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t loc[256*6]);
int chunk_getversionandlocations(uint64_t chunkid, uint32_t currentIp, uint32_t& version,
		uint32_t maxNumberOfChunkCopies, std::vector<ChunkWithAddressAndLabel>& serversList);
void chunk_server_has_chunk(matocsserventry *ptr,uint64_t chunkid,uint32_t version);
void chunk_damaged(matocsserventry *ptr,uint64_t chunkid);
void chunk_lost(matocsserventry *ptr,uint64_t chunkid);
void chunk_server_disconnected(matocsserventry *ptr);

void chunk_got_delete_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_replicate_status(matocsserventry *ptr,uint64_t chunkid,uint32_t version,uint8_t status);

void chunk_got_chunkop_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status);

void chunk_got_create_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_duplicate_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_setversion_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_truncate_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_duptrunc_status(matocsserventry *ptr,uint64_t chunkid,uint8_t status);

#endif

int chunk_load(FILE *fd);
void chunk_store(FILE *fd);
void chunk_unload(void);
void chunk_newfs(void);
int chunk_strinit(void);
uint64_t chunk_checksum(ChecksumMode mode);
ChecksumRecalculationStatus chunks_update_checksum_a_bit(uint32_t speedLimit);
