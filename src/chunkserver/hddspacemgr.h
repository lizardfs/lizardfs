/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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

#include "chunkserver/chunk_file_creator.h"
#include "chunkserver/output_buffer.h"
#include "common/chunk_part_type.h"
#include "common/chunk_with_version_and_type.h"
#include "protocol/chunks_with_type.h"
#include "protocol/MFSCommunication.h"

void hdd_stats(uint64_t *br,uint64_t *bw,uint32_t *opr,uint32_t *opw,uint64_t *dbr,uint64_t *dbw,uint32_t *dopr,uint32_t *dopw,uint64_t *rtime,uint64_t *wtime);
void hdd_op_stats(uint32_t *op_create,uint32_t *op_delete,uint32_t *op_version,uint32_t *op_duplicate,uint32_t *op_truncate,uint32_t *op_duptrunc,uint32_t *op_test);
uint32_t hdd_errorcounter(void);

void hdd_get_damaged_chunks(std::vector<ChunkWithType>& chunks, std::size_t limit);
void hdd_get_lost_chunks(std::vector<ChunkWithType>& chunks, std::size_t limit);
void hdd_get_new_chunks(std::vector<ChunkWithVersionAndType>& chunks, std::size_t limit);

/* lock/unlock pair */
uint32_t hdd_diskinfo_v1_size();
void hdd_diskinfo_v1_data(uint8_t *buff);
uint32_t hdd_diskinfo_v2_size();
void hdd_diskinfo_v2_data(uint8_t *buff);

/* lock/unlock pair */
void hdd_get_chunks_begin();
void hdd_get_chunks_end();
void hdd_get_chunks_next_list_data(std::vector<ChunkWithVersionAndType>& chunks, std::vector<ChunkWithType>& recheck_list);
void hdd_get_chunks_next_list_data_recheck(std::vector<ChunkWithVersionAndType>& chunks, std::vector<ChunkWithType>& recheck_list);

int hdd_spacechanged(void);
void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount);
int hdd_get_load_factor();

/* I/O operations */
void hdd_chunk_release(Chunk *c);
int hdd_open(uint64_t chunkid, ChunkPartType chunkType);
int hdd_close(uint64_t chunkid, ChunkPartType chunkType);
int hdd_prefetch_blocks(uint64_t chunkid, ChunkPartType chunkType, uint32_t firstBlock,
		uint16_t nrOfBlocks);
int hdd_read(uint64_t chunkid, uint32_t version, ChunkPartType chunkType,
		uint32_t offset, uint32_t size, uint32_t maxBlocksToBeReadBehind,
		uint32_t blocksToBeReadAhead, OutputBuffer* outputBuffer);
int hdd_write(Chunk* chunk, uint32_t version,
		uint16_t blocknum, uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer);
int hdd_write(uint64_t chunkid, uint32_t version, ChunkPartType chunkType,
		uint16_t blocknum, uint32_t offset, uint32_t size, uint32_t crc, const uint8_t* buffer);
int hdd_open(Chunk *chunk);
int hdd_close(Chunk *chunk);

/* chunk info */
int hdd_check_version(uint64_t chunkid,uint32_t version);
int hdd_get_blocks(uint64_t chunkid, ChunkPartType chunkType, uint32_t version, uint16_t *blocks);

bool hdd_scans_in_progress();
bool hdd_chunk_trylock(Chunk *c);

/* chunk operations */

/* all chunk operations in one call */
// chunkNewVersion>0 && length==0xFFFFFFFF && chunkIdCopy==0    -> change version
// chunkNewVersion>0 && length==0xFFFFFFFF && chunkIdCopy>0     -> duplicate
// chunkNewVersion>0 && length<=MFSCHUNKSIZE && chunkIdCopy==0     -> truncate
// chunkNewVersion>0 && length<=MFSCHUNKSIZE && chunkIdCopy>0      -> duplicate and truncate
// chunkNewVersion==0 && length==0                              -> delete
// chunkNewVersion==0 && length==1                              -> create
// chunkNewVersion==0 && length==2                              -> test
int hdd_chunkop(uint64_t chunkId, uint32_t chunkVersion,  ChunkPartType chunkType,
		uint32_t chunkNewVersion, uint64_t chunkIdCopy, uint32_t chunkVersionCopy, uint32_t length);

#define hdd_delete(chunkId, chunkVersion, chunkType) \
	hdd_chunkop(chunkId, chunkVersion, chunkType, 0, 0, 0, 0)

#define hdd_create(chunkId, chunkVersion, chunkType) \
	hdd_chunkop(chunkId, chunkVersion, chunkType, 0, 0, 0, 1)

#define hdd_test(chunkId, chunkVersion) \
	hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), 0, 0, 0, 2)

#define hdd_version(chunkId, chunkVersion, chunkType, chunkNewVersion) \
	(((chunkNewVersion) > 0) \
	? hdd_chunkop(chunkId, chunkVersion, chunkType, chunkNewVersion, 0, 0, \
			0xFFFFFFFF) \
	: LIZARDFS_ERROR_EINVAL)

#define hdd_truncate(chunkId, chunkVersion, chunkNewVersion, length) \
	(((chunkNewVersion) > 0 && (length) != 0xFFFFFFFF) \
	? hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), chunkNewVersion, 0, 0, \
			length, ChunkType::getStandardChunkType()) \
	: LIZARDFS_ERROR_EINVAL)

#define hdd_duplicate(chunkId, chunkVersion, chunkNewVersion, chunkIdCopy, chunkVersionCopy) \
	(((chunkNewVersion > 0) && (chunkIdCopy) > 0) \
	? hdd_chunkop(chunkId, chunkVersion, ChunkType::getStandardChunkType(), chunkNewVersion, \
			chunkIdCopy, chunkVersionCopy, 0xFFFFFFFF) \
	: LIZARDFS_ERROR_EINVAL)

#define hdd_duptrunc(chunkId, chunkVersion, chunkNewVersion, chunkIdCopy, chunkVersionCopy, \
		length) \
	(((chunkNewVersion > 0) && (chunkIdCopy) > 0 && (length) != 0xFFFFFFFF) \
	? hdd_chunkop(chunkId, chunkVersion, , ChunkType::getStandardChunkType(), chunkNewVersion, \
			chunkIdCopy, chunkVersionCopy, length) \
	: LIZARDFS_ERROR_EINVAL)

/* chunk testing */
void hdd_test_chunk(ChunkWithVersionAndType chunk);

/* initialization */
int hdd_late_init(void);
int hdd_init(void);

/**
 * Chunk low-level operations
 *
 * These functions shouldn't be used, unless for specific implementation
 * i.e.
 * \see ChunkFileCreator
 *
 * In most cases functions above are prefered.
*/

/**
 * \brief Create new chunk on disk
 *
 * \param chunkid - id of created chunk
 * \param version - version of created chunk
 * \param chunkType - type of created chunk
 * \return On success returns pair of LIZARDFS_STATUS_OK and created chunk in locked state.
 *         On failure returns pair of code of error and nullptr.
 */
std::pair<int, Chunk *> hdd_int_create_chunk(uint64_t chunkid, uint32_t version,
		ChunkPartType chunkType);
int hdd_int_create(uint64_t chunkid, uint32_t version, ChunkPartType chunkType);
int hdd_int_delete(Chunk *chunk, uint32_t version);
int hdd_int_delete(uint64_t chunkid, uint32_t version, ChunkPartType chunkType);
int hdd_int_version(Chunk *chunk, uint32_t version, uint32_t newversion);
int hdd_int_version(uint64_t chunkid, uint32_t version, uint32_t newversion,
		ChunkPartType chunkType);

void hdd_error_occured(Chunk *c);
void hdd_report_damaged_chunk(uint64_t chunkid, ChunkPartType chunk_type);
