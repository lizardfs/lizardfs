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

#ifndef _HDDSPACEMGR_H_
#define _HDDSPACEMGR_H_
#include <inttypes.h>

void hdd_stats(uint32_t *br,uint32_t *bw,uint32_t *opr,uint32_t *opw,uint32_t *dbr,uint32_t *dbw,uint32_t *dopr,uint32_t *dopw,uint64_t *rtime,uint64_t *wtime);
uint32_t hdd_diskinfo_size();
void hdd_diskinfo_data(uint8_t *buff);
uint32_t get_chunkscount();
void fill_chunksinfo(uint8_t *buff);
//uint32_t get_changedchunkscount();
//void fill_changedchunksinfo(uint8_t *buff);
void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount);
int check_chunk(uint64_t chunkid,uint32_t version);
int get_chunk_blocks(uint64_t chunkid,uint32_t version,uint16_t *blocks);
int create_newchunk(uint64_t chunkid,uint32_t version);
int chunk_before_io(uint64_t chunkid);
int chunk_after_io(uint64_t chunkid);
int get_chunk_checksum(uint64_t chunkid, uint32_t version, uint32_t *checksum);
int get_chunk_checksum_tab(uint64_t chunkid, uint32_t version, uint8_t *checksum_tab);
int read_block_from_chunk(uint64_t chunkid, uint32_t version,uint16_t blocknum, uint8_t *buffer, uint32_t offset,uint32_t size,uint32_t *crc);
int write_block_to_chunk(uint64_t chunkid, uint32_t version,uint16_t blocknum, uint8_t *buffer, uint32_t offset,uint32_t size,uint32_t crc);
int duplicate_chunk(uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion);
int set_chunk_version(uint64_t chunkid,uint32_t version,uint32_t oldversion);
int truncate_chunk(uint64_t chunkid,uint32_t length,uint32_t version,uint32_t oldversion);
int duptrunc_chunk(uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion,uint32_t length);
int delete_chunk(uint64_t chunkid,uint32_t version);
void correct_freespace();
int hdd_init(void);

#endif
