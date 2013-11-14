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

#ifndef _HDDSPACEMGR_H_
#define _HDDSPACEMGR_H_

#include <inttypes.h>
#include <vector>

#include "chunkserver/output_buffers.h"
#include "common/chunk_type.h"
#include "common/chunk_with_version.h"
#include "common/chunk_with_version_and_type.h"
#include "common/MFSCommunication.h"

void hdd_stats(uint64_t *br,uint64_t *bw,uint32_t *opr,uint32_t *opw,uint32_t *dbr,uint32_t *dbw,uint32_t *dopr,uint32_t *dopw,uint64_t *rtime,uint64_t *wtime);
void hdd_op_stats(uint32_t *op_create,uint32_t *op_delete,uint32_t *op_version,uint32_t *op_duplicate,uint32_t *op_truncate,uint32_t *op_duptrunc,uint32_t *op_test);
uint32_t hdd_errorcounter(void);

void hdd_get_damaged_chunks(std::vector<uint64_t>& chunks);
void hdd_get_lost_chunks(std::vector<uint64_t>& chunks, uint32_t limit);
void hdd_get_new_chunks(std::vector<ChunkWithVersionAndType>& chunks);

/* lock/unlock pair */
uint32_t hdd_diskinfo_v1_size();
void hdd_diskinfo_v1_data(uint8_t *buff);
uint32_t hdd_diskinfo_v2_size();
void hdd_diskinfo_v2_data(uint8_t *buff);
/* lock/unlock pair */
void hdd_get_chunks_begin();
void hdd_get_chunks_end();

void hdd_get_chunks_next_list_data(std::vector<ChunkWithVersionAndType>& chunks);

int hdd_spacechanged(void);
void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount);

/* I/O operations */
int hdd_open(uint64_t chunkid, ChunkType chunkType);
int hdd_close(uint64_t chunkid, ChunkType chunkType);
int hdd_read(uint64_t chunkid, uint32_t version, ChunkType chunkType,
		uint32_t offset, uint32_t size, OutputBuffer* outputBuffer);
int hdd_write(uint64_t chunkid, uint32_t version, ChunkType chunkType,
		uint16_t blocknum, uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer);

/* chunk info */
int hdd_check_version(uint64_t chunkid,uint32_t version);
int hdd_get_blocks(uint64_t chunkid,uint32_t version,uint16_t *blocks);
int hdd_get_checksum(uint64_t chunkid, uint32_t version, uint32_t *checksum);

/* chunk operations */

/* all chunk operations in one call */
// chunkNewVersion>0 && length==0xFFFFFFFF && chunkIdCopy==0    -> change version
// chunkNewVersion>0 && length==0xFFFFFFFF && chunkIdCopy>0     -> duplicate
// chunkNewVersion>0 && length<=MFSCHUNKSIZE && chunkIdCopy==0     -> truncate
// chunkNewVersion>0 && length<=MFSCHUNKSIZE && chunkIdCopy>0      -> duplicate and truncate
// chunkNewVersion==0 && length==0                              -> delete
// chunkNewVersion==0 && length==1                              -> create
// chunkNewVersion==0 && length==2                              -> test
int hdd_chunkop(uint64_t chunkId, uint32_t chunkVersion,  ChunkType chunkType,
		uint32_t chunkNewVersion, uint64_t chunkIdCopy, uint32_t chunkVersionCopy, uint32_t length);

#define hdd_delete(chunkId, chunkVersion) \
	hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), 0, 0, 0, 0)

#define hdd_create(chunkId, chunkVersion) \
	hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), 0, 0, 0, 1)

#define hdd_test(chunkId, chunkVersion) \
	hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), 0, 0, 0, 2)

#define hdd_version(chunkId, chunkVersion, chunkNewVersion) \
	(((chunkNewVersion) > 0) \
	? hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), chunkNewVersion, 0, 0, \
			0xFFFFFFFF) \
	: ERROR_EINVAL)

#define hdd_truncate(chunkId, chunkVersion, chunkNewVersion, length) \
	(((chunkNewVersion) > 0 && (length) != 0xFFFFFFFF) \
	? hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), chunkNewVersion, 0, 0, \
			length, ChunkType::getStandardChunkType()) \
	: ERROR_EINVAL)

#define hdd_duplicate(chunkId, chunkVersion, chunkNewVersion, chunkIdCopy, chunkVersionCopy) \
	(((chunkNewVersion > 0) && (chunkIdCopy) > 0) \
	? hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), chunkNewVersion, \
			chunkIdCopy, chunkVersionCopy, 0xFFFFFFFF) \
	: ERROR_EINVAL)

#define hdd_duptrunc(chunkId, chunkVersion, chunkNewVersion, chunkIdCopy, chunkVersionCopy, \
		length) \
	(((chunkNewVersion > 0) && (chunkIdCopy) > 0 && (length) != 0xFFFFFFFF) \
	? hdd_chunkop(chunkId, chunkVersion, , ChunkType::getStandardChunkType(), chunkNewVersion, \
			chunkIdCopy, chunkVersionCopy, length) \
	: ERROR_EINVAL)

/* initialization */
int hdd_late_init(void);
int hdd_init(void);

/* debug only */
void hdd_test_show_chunks(void);
void hdd_test_show_openedchunks(void);
#endif
