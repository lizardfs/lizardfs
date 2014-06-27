#pragma once

#include "common/platform.h"

#include "chunkserver/replication_bandwidth_limiter.h"

/**
 * Function returning singleton object used for replication bandwidth limiting in chunkserver
 */
ReplicationBandwidthLimiter& replicationBandwidthLimiter();
