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
#include "common/io_limiting.h"

#include <algorithm>
#include <thread>

#include "common/io_limits_config_loader.h"
#include "common/massert.h"
#include "common/MFSCommunication.h"

using namespace ioLimiting;

void Limiter::registerReconfigure(ReconfigurationFunction reconfigure) {
	reconfigure_ = reconfigure;
}

SteadyTimePoint RTClock::now() {
	return SteadyClock::now();
}

void RTClock::sleepUntil(SteadyTimePoint time) {
	return std::this_thread::sleep_until(time);
}

bool Group::attempt(uint64_t size) {
	if (lastRequestEndTime_ + shared_.delta < clock_.now()) {
		reserve_ = 0;
	}
	if (size <= reserve_) {
		reserve_ -= size;
		return true;
	} else {
		return false;
	}
}

Group::PendingRequests::iterator Group::enqueue(uint64_t size) {
	PendingRequests::iterator it = pendingRequests_.emplace(pendingRequests_.end(), size);
	return it;
}

void Group::dequeue(PendingRequests::iterator it) {
	pastRequests_.emplace_back(clock_.now(), it->size);
	pendingRequests_.erase(it);
}

void Group::notifyQueue() {
	if (!pendingRequests_.empty()) {
		pendingRequests_.front().cond.notify_one();
	}
}

bool Group::isFirst(PendingRequests::iterator it) const {
	return it == pendingRequests_.begin();
}

void Group::askMaster(std::unique_lock<std::mutex>& lock) {
	while (!pastRequests_.empty()
			&& ((pastRequests_.front().creationTime + shared_.delta) < clock_.now())) {
		pastRequests_.pop_front();
	}
	uint64_t size = 0;
	for (const auto& request : pendingRequests_) {
		size += request.size;
	}
	for (const auto& request : pastRequests_) {
		size += request.size;
	}
	sassert(size > reserve_);
	size -= reserve_;
	lastRequestStartTime_ = clock_.now();
	lock.unlock();
	uint64_t receivedSize = shared_.limiter.request(groupId_, size);
	lock.lock();
	lastRequestEndTime_ = clock_.now();
	lastRequestSuccessful_ = receivedSize >= size;
	reserve_ += receivedSize;
}

uint8_t Group::wait(uint64_t size, SteadyTimePoint deadline, std::unique_lock<std::mutex>& lock) {
	PendingRequests::iterator it = enqueue(size);
	it->cond.wait(lock, [this, it]() {return isFirst(it);});
	uint8_t status = ERROR_TIMEOUT;
	while (clock_.now() < deadline) {
		if (dead_) {
			status = ERROR_ENOENT;
			break;
		}
		if (attempt(size)) {
			status = STATUS_OK;
			break;
		}
		if (!lastRequestSuccessful_) {
			SteadyTimePoint nextRequestTime = lastRequestStartTime_ + shared_.delta;
			if (nextRequestTime > deadline) {
				break;
			}
			lock.unlock();
			clock_.sleepUntil(nextRequestTime);
			lock.lock();
			if (dead_) {
				continue;
			}
		}
		askMaster(lock);
	}
	dequeue(it);
	notifyQueue();
	return status;
}

void Group::die() {
	dead_ = true;
}
