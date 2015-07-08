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

#include "common/platform.h"
#include "chunkserver/network_stats.h"

std::atomic<uint64_t> stats_bytesin(0);
std::atomic<uint64_t> stats_bytesout(0);
std::atomic<uint32_t> stats_hlopr(0);
std::atomic<uint32_t> stats_hlopw(0);
std::atomic<uint32_t> stats_maxjobscnt(0);

void networkStats(uint64_t *bin, uint64_t *bout, uint32_t *hlopr,
		uint32_t *hlopw, uint32_t *maxjobscnt) {
	*bin = stats_bytesin.exchange(0);
	*bout = stats_bytesout.exchange(0);
	*hlopr = stats_hlopr.exchange(0);
	*hlopw = stats_hlopw.exchange(0);
	*maxjobscnt = stats_maxjobscnt.exchange(0);
}
