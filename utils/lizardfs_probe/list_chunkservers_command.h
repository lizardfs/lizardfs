#pragma once

#include "common/network_address.h"
#include "common/serializable_class.h"
#include "utils/lizardfs_probe/lizardfs_probe_command.h"

SERIALIZABLE_CLASS_BEGIN(ChunkserverEntry)
SERIALIZABLE_CLASS_BODY(ChunkserverEntry,
		uint32_t, version,
		NetworkAddress, address,
		uint64_t, usedSpace,
		uint64_t, totalSpace,
		uint32_t, chunks,
		uint64_t, tdUsedSpace,
		uint64_t, tdTotalSpace,
		uint32_t, tdChunks,
		uint32_t, errorCount)
SERIALIZABLE_CLASS_END;

class ListChunkserversCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual void usage() const;
	virtual void run(const std::vector<std::string>& argv) const;

	static std::vector<ChunkserverEntry> getChunkserversList (
			const std::string& masterHost, const std::string& masterPort);
};
