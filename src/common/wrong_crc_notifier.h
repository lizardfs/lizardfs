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

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>
#include <vector>

#include "common/chunk_connector.h"
#include "common/chunk_part_type.h"
#include "common/chunk_with_version_and_type.h"
#include "common/connection_pool.h"
#include "common/network_address.h"
#include "common/slogger.h"

class WrongCrcNotifier {
public:
	WrongCrcNotifier() : terminate_(false) {
	}

	~WrongCrcNotifier() {
		if (chunkConnector_) {
			terminate();
			try {
				myThread_.join();
			} catch (std::system_error& e) {
				lzfs_pretty_syslog(LOG_NOTICE, "Failed to join wrong CRC notifier thread: %s", e.what());
			}
		}
	}

	void init(uint32_t sourceIp) {
		sassert(!connectionPool_);
		connectionPool_.reset(new ConnectionPool);
		init(std::unique_ptr<ChunkConnector>(
				new ChunkConnectorUsingPool(*connectionPool_, sourceIp)));
	}

	void init(std::unique_ptr<ChunkConnector> chunkConnector) {
		sassert(!chunkConnector_);
		chunkConnector_ = std::move(chunkConnector);
		myThread_ = std::thread(std::ref(*this));
	}

	// main loop:
	void operator()();

	void reportBadCrc(NetworkAddress server, uint64_t chunkId, uint32_t chunkVersion,
			ChunkPartType chunkType);
private:
	typedef std::pair<NetworkAddress, ChunkWithVersionAndType> InconsistentChunk;

	void terminate();

	std::atomic<bool> terminate_;
	std::unique_ptr<ConnectionPool> connectionPool_;
	std::unique_ptr<ChunkConnector> chunkConnector_;
	std::thread myThread_;

	std::mutex mutex_;
	std::condition_variable cond_;
	std::set<InconsistentChunk> inconsistentChunks_;
};

extern WrongCrcNotifier gWrongCrcNotifier;
