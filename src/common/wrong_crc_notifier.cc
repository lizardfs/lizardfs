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
#include "common/wrong_crc_notifier.h"

#include "protocol/cltocs.h"
#include "common/sockets.h"

void WrongCrcNotifier::operator()() {
	sassert(chunkConnector_);
	while (!terminate_) {
		std::unique_lock<std::mutex> lock(mutex_);
		if (inconsistentChunks_.empty()) {
			cond_.wait_for(lock, std::chrono::seconds(1));
		}
		decltype(inconsistentChunks_) oldInconsistentChunks;
		std::swap(inconsistentChunks_, oldInconsistentChunks);
		lock.unlock();
		if (connectionPool_) {
			connectionPool_->cleanup();
		}

		for (const auto& addressAndChunk : oldInconsistentChunks) {
			const auto& address = addressAndChunk.first;
			const auto& chunkWithVersionAndType = addressAndChunk.second;
			int fd;
			try {
				fd = chunkConnector_->startUsingConnection(address,
						Timeout(std::chrono::seconds(1)));
			} catch (std::exception& e) {
				lzfs_pretty_syslog(LOG_NOTICE, "Failed to notify CS: %s about chunk: %s"
						" with wrong CRC - connection timed out",
						address.toString().c_str(),
						chunkWithVersionAndType.toString().c_str());
				continue;
			}
			sassert(fd >= 0);
			std::vector<uint8_t> packet;
			cltocs::testChunk::serialize(packet, chunkWithVersionAndType.id,
					chunkWithVersionAndType.version, chunkWithVersionAndType.type);
			if (tcptowrite(fd, packet.data(), packet.size(), 1000) == int32_t(packet.size())) {
				chunkConnector_->endUsingConnection(fd, address);
			} else {
				tcpclose(fd);
				lzfs_pretty_syslog(LOG_NOTICE, "Failed to notify CS: %s about chunk: %s"
						" with wrong CRC - write error",
						address.toString().c_str(),
						chunkWithVersionAndType.toString().c_str());
			}
		}
	}
}

void WrongCrcNotifier::reportBadCrc(NetworkAddress server, uint64_t chunkId, uint32_t chunkVersion,
		ChunkPartType chunkType) {
	sassert(chunkConnector_);
	std::unique_lock<std::mutex> lock(mutex_);
	InconsistentChunk inconsistentChunk = std::make_pair(
			server, ChunkWithVersionAndType(chunkId, chunkVersion, chunkType));
	inconsistentChunks_.insert(inconsistentChunk);
	lock.unlock();
	cond_.notify_all();
}

void WrongCrcNotifier::terminate() {
	sassert(chunkConnector_);
	terminate_ = true;
	cond_.notify_all();
}

WrongCrcNotifier gWrongCrcNotifier;
