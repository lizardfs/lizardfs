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
