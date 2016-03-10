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
		return LIZARDFS_STATUS_OK;
	}
	std::unique_lock<std::mutex> lock(mutex_);
	return group_->wait(requestedSize, SteadyClock::now() + timeout, lock);
}

uint64_t ReplicationBandwidthLimiter::ReplicationLimiter::request(
		const IoLimitGroupId& groupId, uint64_t size) {
	return database_.request(SteadyClock::now(), groupId, size);
}

void ReplicationBandwidthLimiter::ReplicationLimiter::setLimit(uint64_t limit_kBps) {
	database_.setLimits(SteadyClock::now(), {{kReplicationGroupId, limit_kBps}}, 200);
}

void ReplicationBandwidthLimiter::ReplicationLimiter::unsetLimit() {
	database_.setLimits(SteadyClock::now(), IoLimitsConfigLoader::LimitsMap{}, 200);
}
