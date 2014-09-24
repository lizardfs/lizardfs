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

#include "mount/chunk_locator.h"

void read_inode_ops(uint32_t inode);
void* read_data_new(uint32_t inode);
void read_data_end(void *rr);
int read_data(void *rr,uint64_t offset,uint32_t *size,uint8_t **buff);
void read_data_freebuff(void *rr);
void read_data_init(uint32_t retries,
		uint32_t chunkserverRoundTripTime_ms,
		uint32_t chunkserverConnectTimeout_ms,
		uint32_t chunkServerBasicReadTimeout_ms,
		uint32_t chunkserverTotalReadTimeout_ms,
		bool prefetchXorStripes);
void read_data_term(void);
