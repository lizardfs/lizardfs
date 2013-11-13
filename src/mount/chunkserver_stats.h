#ifndef LIZARDFS_MFSMOUNT_CHUNKSERVER_STATS_H_
#define LIZARDFS_MFSMOUNT_CHUNKSERVER_STATS_H_

#include "common/network_address.h"
#include "common/time_utils.h"

#include <chrono>
#include <mutex>
#include <unordered_map>

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


#endif /* LIZARDFS_MFSMOUNT_CHUNKSERVER_STATS_H_ */
