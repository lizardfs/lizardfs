#include "config.h"
#include "chunkserver/g_limiters.h"

ReplicationBandwidthLimiter& replicationBandwidthLimiter() {
	static ReplicationBandwidthLimiter limiter;
	return limiter;
}
