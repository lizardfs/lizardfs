#pragma once

#include "config.h"

#include <inttypes.h>
#include <atomic>

extern std::atomic<uint64_t> stats_bytesin;
extern std::atomic<uint64_t> stats_bytesout;
extern std::atomic<uint32_t> stats_hlopr;
extern std::atomic<uint32_t> stats_hlopw;
extern std::atomic<uint32_t> stats_maxjobscnt;

void networkStats(uint64_t *bin, uint64_t *bout, uint32_t *hlopr,
		uint32_t *hlopw, uint32_t *maxjobscnt);
