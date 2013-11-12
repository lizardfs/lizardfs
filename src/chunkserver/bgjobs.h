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

#ifndef _BGJOBS_H_
#define _BGJOBS_H_

#include <inttypes.h>

#include "chunkserver/output_buffers.h"
#include "common/chunk_type.h"

void* job_pool_new(uint8_t workers,uint32_t jobs,int *wakeupdesc);
uint32_t job_pool_jobs_count(void *jpool);
void job_pool_disable_and_change_callback_all(void *jpool,void (*callback)(uint8_t status,void *extra));
void job_pool_disable_job(void *jpool,uint32_t jobid);
void job_pool_check_jobs(void *jpool);
void job_pool_change_callback(void *jpool,uint32_t jobid,void (*callback)(uint8_t status,void *extra),void *extra);
void job_pool_delete(void *jpool);


uint32_t job_inval(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra);
uint32_t job_chunkop(void *jpool, void (*callback)(uint8_t status, void *extra), void *extra,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType, uint32_t newChunkVersion,
		uint64_t copyChunkId, uint32_t copyChunkVersion, uint32_t length);

#define job_delete(jobPool, callback, extra, chunkId, chunkVersion, chunkType) \
	job_chunkop(jobPool, callback, extra, chunkId, chunkVersion, chunkType, 0, 0, 0, 0)

#define job_create(jobPool, callback, extra, chunkId, chunkVersion) job_chunkop(jobPool, callback, \
		extra, chunkId, chunkVersion, ChunkType::getStandardChunkType(), 0, 0, 0, 1)

#define job_test(jobPool, callback, extra, chunkId, chunkVersion) job_chunkop(jobPool, callback, \
		extra, chunkId, chunkVersion, ChunkType::getStandardChunkType(), 0, 0, 0, 2)

#define job_version(jobPool, callback, extra, chunkId, chunkVersion, chunkType, \
		newChunkVersion) \
	(((newChunkVersion)>0) \
	? job_chunkop(jobPool, callback, extra, chunkId, chunkVersion, chunkType, newChunkVersion, 0, \
			0, 0xFFFFFFFF) \
	: job_inval(jobPool, callback, extra))

#define job_truncate(jobPool, callback, extra, chunkId, chunkVersion, newChunkVersion, length) \
	(((newChunkVersion) > 0 && (length) != 0xFFFFFFFF) \
	? job_chunkop(jobPool, callback, extra, chunkId, chunkVersion, \
			ChunkType::getStandardChunkType(), newChunkVersion, 0, 0, length) \
	: job_inval(jobPool, callback, extra))

#define job_duplicate(jobPool, callback, extra, chunkId, chunkVersion, newChunkVersion, \
		chunkIdCopy, chunkVersionCopy) \
	(((newChunkVersion > 0) && (chunkIdCopy) > 0) \
	? job_chunkop(jobPool, callback, extra, chunkId, chunkVersion, \
			ChunkType::getStandardChunkType(), newChunkVersion, chunkIdCopy, chunkVersionCopy, \
			0xFFFFFFFF) \
	: job_inval(jobPool, callback, extra))

#define job_duptrunc(jobPool, callback, extra, chunkId, chunkVersion, newChunkVersion, \
		chunkIdCopy, chunkVersionCopy, length) \
	(((newChunkVersion > 0) && (chunkIdCopy) > 0 && (length) != 0xFFFFFFFF) \
	? job_chunkop(jobPool, callback, extra, chunkId, chunkVersion, \
			ChunkType::getStandardChunkType(), newChunkVersion, chunkIdCopy, chunkVersionCopy, \
			length) \
	: job_inval(jobPool, callback, extra))

uint32_t job_open(void *jpool, void (*callback)(uint8_t status, void *extra), void *extra,
		uint64_t chunkid, ChunkType chunkType);
uint32_t job_close(void *jpool, void (*callback)(uint8_t status, void *extra), void *extra,
		uint64_t chunkid, ChunkType chunkType);
uint32_t job_read(void *jpool, void (*callback)(uint8_t status,void *extra), void *extra,
		uint64_t chunkid, uint32_t chunkVersion, ChunkType chunkType, uint32_t offset, uint32_t size,
		OutputBuffer* outputBuffer, bool performHddOpen);
uint32_t job_write(void *jpool, void (*callback)(uint8_t status, void *extra), void *extra,
		uint64_t chunkid, uint32_t chunkVersion, uint16_t blocknum,
		const uint8_t *buffer, uint32_t offset, uint32_t size,
		const uint8_t *crcbuff);

/* srcs: srccnt * (chunkid:64 chunkVersion:32 ip:32 port:16) */
uint32_t job_replicate(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t chunkVersion,uint8_t srccnt,const uint8_t *srcs);
uint32_t job_replicate_simple(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t chunkVersion,uint32_t ip,uint16_t port);


#endif
