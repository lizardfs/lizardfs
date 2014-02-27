#pragma once

#include "common/network_address.h"
#include "utils/lizardfs_probe/lizardfs_probe_command.h"

struct ChunkserverEntry {
	NetworkAddress address;
	uint32_t version, chunks, tdChunks, errorCount;
	uint64_t usedSpace, totalSpace, tdUsedSpace, tdTotalSpace;
};

inline uint32_t serializedSize(const ChunkserverEntry& entry) {
	return serializedSize(entry.version, entry.address,
			entry.usedSpace, entry.totalSpace, entry.chunks,
			entry.tdUsedSpace, entry.tdTotalSpace, entry.tdChunks, entry.errorCount);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkserverEntry& value) {
	deserialize(source, bytesLeftInBuffer, value.version, value.address,
			value.usedSpace, value.totalSpace, value.chunks,
			value.tdUsedSpace, value.tdTotalSpace, value.tdChunks, value.errorCount);
}

class ListChunkserversCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual void usage() const;
	virtual void run(const std::vector<std::string>& argv) const;

	static std::vector<ChunkserverEntry> getChunkserversList (
			const std::string& masterHost, const std::string& masterPort);
};
