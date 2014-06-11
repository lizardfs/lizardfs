#include "config.h"
#include "chunkserver/replication_bandwidth_limiter.h"

using namespace ioLimiting;

std::mutex ReplicationBandwidthLimiter::mutex_;

// I/O limit group storing the replication limit.
// Group is needed just for technical purpose: IoLimiting mechanism operates on groups
// (since it can support multiple ones), but here we need just a single one.
constexpr const char* kReplicationGroupId = "replication";

ReplicationBandwidthLimiter::ReplicationBandwidthLimiter()
		: state_(limiter_, std::chrono::milliseconds(20)) {}

void ReplicationBandwidthLimiter::setLimit(uint64_t limit_kBps) {
	limiter_.setLimit(limit_kBps);
	if (group_) {
		return;
	}
	group_.reset(new Group(state_, kReplicationGroupId, clock_));
}

void ReplicationBandwidthLimiter::unsetLimit() {
	group_.reset();
	limiter_.unsetLimit();
}

uint8_t ReplicationBandwidthLimiter::wait(uint64_t requestedSize, const SteadyDuration timeout) {
	if (!group_) {
		// No limit set, request is instantly accepted
		return STATUS_OK;
	}
	std::unique_lock<std::mutex> lock(mutex_);
	int status = group_->wait(requestedSize, SteadyClock::now() + timeout, lock);
	// Convert errno-like code to MFS status
	if (status == ENOENT) {
		// This should never happen so return anything
		return ERROR_ENOTSUP;
	} else if (status == ETIMEDOUT) {
		return ERROR_TIMEOUT;
	}
	return STATUS_OK;
}

uint64_t ReplicationBandwidthLimiter::ReplicationLimiter::request(
		const IoLimitGroupId& groupId, uint64_t size) {
	return database_.request(SteadyClock::now(), groupId, size);
}

void ReplicationBandwidthLimiter::ReplicationLimiter::setLimit(uint64_t limit_kBps) {
	database_.setLimits(SteadyClock::now(), {{kReplicationGroupId, limit_kBps}}, 200);
}

void ReplicationBandwidthLimiter::ReplicationLimiter::unsetLimit() {
	database_.setLimits(SteadyClock::now(), {}, 200);
}
