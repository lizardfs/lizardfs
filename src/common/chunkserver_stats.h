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

#include <chrono>
#include <mutex>
#include <unordered_map>

#include "common/network_address.h"
#include "common/time_utils.h"

// For each chunkserver, track how many operations on this chunksrever are being performed by us
// at the moment and whether the chunkserver appears to be overloaded or offline ("defective").
//
// Code which uses chunkservers to perform read/write operations should register and unregister
// these operations with the global ChunkserverStats instance.
//
// If there is a choice between multiple chunkservers capable of performing some operation, the
// chunkserver with lowest pending operation count should be chosen.
//
// Code that determines a chunkserver to be defective should call markDefective(). Others should
// prefer to use chunkservers not marked as defective, if possible. The defective flag is cleared
// after defectiveTimeout_ms to check if the chunkserver recovered from problems.
//
// Successful operations on chunkservers considered "defective" should call markWorking().
//
// All methods are thread safe.
//
class ChunkserverStats {
public:
	class ChunkserverEntry {
	public:
		ChunkserverEntry();

		uint32_t getOperationCount() const {
			return pendingReads() + pendingWrites();
		}

		uint32_t pendingReads() const {
			return pendingReads_;
		}

		uint32_t pendingWrites() const {
			return pendingWrites_;
		}

		float score() const;

	private:
		static constexpr int defectiveTimeout_ms = 2000;

		uint32_t pendingReads_;
		uint32_t pendingWrites_;
		uint32_t defects_;
		Timeout defectiveTimeout_;

		friend class ChunkserverStats;
	};

	// each of the following methods accesses existing entry or creates a new one

	const ChunkserverEntry getStatisticsFor(const NetworkAddress& address);

	void registerReadOperation(const NetworkAddress& address);
	void unregisterReadOperation(const NetworkAddress& address);

	void registerWriteOperation(const NetworkAddress& address);
	void unregisterWriteOperation(const NetworkAddress& address);

	void markDefective(const NetworkAddress& address);
	void markWorking(const NetworkAddress& address);

private:
	std::mutex mutex_;
	std::unordered_map<NetworkAddress, ChunkserverEntry> chunkserverEntries_;
};

// global chunkserver statistics for this mount instance
extern ChunkserverStats globalChunkserverStats;


// Limited proxy for ChunkserverStats instance. Destructor unregisters all operations
// registered through the proxy.
//
// Not thread safe.
//
class ChunkserverStatsProxy {
public:
	ChunkserverStatsProxy(ChunkserverStats &stats): stats_(stats) {
	}

	// Don't copy it because bad things would happen.
	ChunkserverStatsProxy(const ChunkserverStatsProxy&) = delete;
	ChunkserverStatsProxy& operator=(const ChunkserverStatsProxy &) = delete;

	~ChunkserverStatsProxy();

	void registerReadOperation(const NetworkAddress& address);
	void unregisterReadOperation(const NetworkAddress& address);

	void registerWriteOperation(const NetworkAddress& address);
	void unregisterWriteOperation(const NetworkAddress& address);

	void markDefective(const NetworkAddress& address);
	void markWorking(const NetworkAddress& address);

	void allPendingDefective();

private:
	ChunkserverStats &stats_;
	std::unordered_map<NetworkAddress, uint32_t> readOperations_;
	std::unordered_map<NetworkAddress, uint32_t> writeOperations_;
};
