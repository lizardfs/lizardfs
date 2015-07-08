/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <inttypes.h>
#include <atomic>

extern std::atomic<uint64_t> stats_bytesin;
extern std::atomic<uint64_t> stats_bytesout;
extern std::atomic<uint32_t> stats_hlopr;
extern std::atomic<uint32_t> stats_hlopw;
extern std::atomic<uint32_t> stats_maxjobscnt;

void networkStats(uint64_t *bin, uint64_t *bout, uint32_t *hlopr,
		uint32_t *hlopw, uint32_t *maxjobscnt);
