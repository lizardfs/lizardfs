#include "common/wrong_crc_notifier.h"

#include "common/sockets.h"
#include "common/cltocs_communication.h"

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
		connectionPool_.cleanup();

		for (const auto& addressAndChunk : oldInconsistentChunks) {
			const auto& address = addressAndChunk.first;
			const auto& chunkWithVersionAndType = addressAndChunk.second;
			int fd;
			try {
				fd = chunkConnector_->startUsingConnection(address,
						Timeout(std::chrono::seconds(1)));
			} catch (std::exception& e) {
				syslog(LOG_NOTICE, "Failed to notify CS: %s about chunk: %s"
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
				syslog(LOG_NOTICE, "Failed to notify CS: %s about chunk: %s"
						" with wrong CRC - write error",
						address.toString().c_str(),
						chunkWithVersionAndType.toString().c_str());
			}
		}
	}
}

void WrongCrcNotifier::reportBadCrc(NetworkAddress server, uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType) {
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
