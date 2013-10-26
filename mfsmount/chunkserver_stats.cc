#include "mfsmount/chunkserver_stats.h"

#include <mutex>
#include <unordered_map>


// global chunkserver statistics for this mount instance
ChunkserverStats globalChunkserverStats;


// ChunkserverEntry implementation

constexpr int ChunkserverStats::ChunkserverEntry::defectiveTimeout_ms;

ChunkserverStats::ChunkserverEntry::ChunkserverEntry(): pendingReads_(0), pendingWrites_(0),
		defective_(false), defectiveTimeout_(std::chrono::milliseconds(defectiveTimeout_ms)) {
}

// ChunkserverStats implementation

const ChunkserverStats::ChunkserverEntry ChunkserverStats::getStatisticsFor(
		const NetworkAddress& address) {
	std::unique_lock<std::mutex> lock(mutex_);
	return chunkserverEntries_[address];
}

void ChunkserverStats::registerReadOperation(const NetworkAddress& address) {
	std::unique_lock<std::mutex> lock(mutex_);
	chunkserverEntries_[address].pendingReads_++;
}

void ChunkserverStats::unregisterReadOperation(const NetworkAddress& address) {
	std::unique_lock<std::mutex> lock(mutex_);
	chunkserverEntries_[address].pendingReads_--;
}

void ChunkserverStats::registerWriteOperation(const NetworkAddress& address) {
	std::unique_lock<std::mutex> lock(mutex_);
	chunkserverEntries_[address].pendingWrites_++;
}

void ChunkserverStats::unregisterWriteOperation(const NetworkAddress& address) {
	std::unique_lock<std::mutex> lock(mutex_);
	chunkserverEntries_[address].pendingWrites_--;
}

void ChunkserverStats::markWorking(const NetworkAddress& address) {
	std::unique_lock<std::mutex> lock(mutex_);
	chunkserverEntries_[address].defective_ = false;
}

void ChunkserverStats::markDefective(const NetworkAddress& address) {
	std::unique_lock<std::mutex> lock(mutex_);
	ChunkserverEntry &chunkserver = chunkserverEntries_[address];
	chunkserver.defective_ = true;
	chunkserver.defectiveTimeout_.reset();
}

bool ChunkserverStats::ChunkserverEntry::isDefective() const {
	return (defective_ && !defectiveTimeout_.expired());
}

// ChunkserverStatsProxy implementation

ChunkserverStatsProxy::~ChunkserverStatsProxy() {
	for (auto entry : readOperations_) {
		for (uint32_t i = 0; i < entry.second; i++) {
			stats_.unregisterReadOperation(entry.first);
		}
	}
	for (auto entry : writeOperations_) {
		for (uint32_t i = 0; i < entry.second; i++) {
			stats_.unregisterWriteOperation(entry.first);
		}
	}
}

void ChunkserverStatsProxy::registerReadOperation(const NetworkAddress& address) {
	stats_.registerReadOperation(address);
	readOperations_[address]++;
}

void ChunkserverStatsProxy::unregisterReadOperation(const NetworkAddress& address) {
	stats_.unregisterReadOperation(address);
	readOperations_[address]--;
}

void ChunkserverStatsProxy::registerWriteOperation(const NetworkAddress& address) {
	stats_.registerWriteOperation(address);
	writeOperations_[address]++;
}

void ChunkserverStatsProxy::unregisterWriteOperation(const NetworkAddress& address) {
	stats_.unregisterWriteOperation(address);
	writeOperations_[address]--;
}

void ChunkserverStatsProxy::markDefective(const NetworkAddress& address) {
	stats_.markDefective(address);
}

void ChunkserverStatsProxy::markWorking(const NetworkAddress& address) {
	stats_.markWorking(address);
}

void ChunkserverStatsProxy::allPendingDefective() {
	for (auto entry : readOperations_) {
		if (entry.second > 0) {
			stats_.markDefective(entry.first);
		}
	}
	for (auto entry : writeOperations_) {
		if (entry.second > 0) {
			stats_.markDefective(entry.first);
		}
	}
}
