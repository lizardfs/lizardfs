#pragma once

#include "common/platform.h"

#include "common/server_connection.h"
#include "probe/lizardfs_probe_command.h"

struct MetadataserverStatus {
	std::string personality;
	std::string serverStatus;
	uint64_t metadataVersion;
};

class MetadataserverStatusCommand : public LizardFsProbeCommand {
public:
	std::string name() const override;
	void usage() const override;
	SupportedOptions supportedOptions() const override;
	void run(const Options& options) const override;
	static MetadataserverStatus getStatus(ServerConnection& connection);
};
