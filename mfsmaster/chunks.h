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

#ifndef _CHUNKS_H_
#define _CHUNKS_H_
#include <stdio.h>
#include <inttypes.h>

/*
int chunk_create(uint64_t *chunkid,uint8_t goal);
int chunk_duplicate(uint64_t *chunkid,uint64_t oldchunkid,uint8_t goal);
int chunk_increase_version(uint64_t chunkid);
int chunk_truncate(uint64_t chunkid,uint32_t length);
int chunk_duptrunc(uint64_t *chunkid,uint64_t oldchunkid,uint32_t length,uint8_t goal);
int chunk_reinitialize(uint64_t chunkid);

void chunk_load_goal(void);
*/
// METARESTORE
int chunk_change_file(uint64_t chunkid,uint8_t prevgoal,uint8_t newgoal);
int chunk_delete_file(uint64_t chunkid,uint8_t goal);
int chunk_add_file(uint64_t chunkid,uint8_t goal);
int chunk_log_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t opflag);
int chunk_log_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal);
//int chunk_multi_reinitialize(uint32_t ts,uint64_t chunkid);
int chunk_unlock(uint64_t chunkid);
int chunk_increase_version(uint64_t chunkid);
int chunk_set_version(uint64_t chunkid,uint32_t version);

void chunk_dump(void);

#ifndef METARESTORE
void chunk_stats(uint32_t *del,uint32_t *repl);
void chunk_store_info(uint8_t *buff);
uint32_t chunk_get_missing_count(void);
void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid);
uint32_t chunk_count(void);
void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regcopies);

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies);

int chunk_multi_modify(uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t *opflag);
int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t goal);
//int chunk_multi_reinitialize(uint64_t chunkid);
int chunk_repair(uint8_t goal,uint64_t ochunkid,uint32_t *nversion);

/* ---- */
int chunk_getversionandlocations(uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t loc[256*6]);
/* ---- */
void chunk_server_has_chunk(void *ptr,uint64_t chunkid,uint32_t version);
void chunk_damaged(void *ptr,uint64_t chunkid);
void chunk_lost(void *ptr,uint64_t chunkid);
void chunk_server_disconnected(void *ptr);

void chunk_got_delete_status(void *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_replicate_status(void *ptr,uint64_t chunkid,uint32_t version,uint8_t status);

void chunk_got_chunkop_status(void *ptr,uint64_t chunkid,uint8_t status);

void chunk_got_create_status(void *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_duplicate_status(void *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_setversion_status(void *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_truncate_status(void *ptr,uint64_t chunkid,uint8_t status);
void chunk_got_duptrunc_status(void *ptr,uint64_t chunkid,uint8_t status);

#endif
/* ---- */

// int chunk_load_1_1(FILE *fd);
int chunk_load(FILE *fd);
void chunk_store(FILE *fd);
void chunk_term(void);
void chunk_newfs(void);
int chunk_strinit(void);

#endif
