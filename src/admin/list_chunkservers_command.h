#pragma once

#include "common/platform.h"

#include "common/network_address.h"
#include "common/serialization_macros.h"
#include "common/chunkserver_list_entry.h"
#include "admin/lizardfs_admin_command.h"

class ListChunkserversCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual SupportedOptions supportedOptions() const;
	virtual void usage() const;
	virtual void run(const Options& options) const;

	static std::vector<ChunkserverListEntry> getChunkserversList (
			const std::string& masterHost, const std::string& masterPort);
};
