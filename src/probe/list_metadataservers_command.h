#pragma once

#include "common/platform.h"

#include "probe/lizardfs_probe_command.h"

class ListMetadataserversCommand : public LizardFsProbeCommand {
public:
	std::string name() const override;
	void usage() const override;
	SupportedOptions supportedOptions() const override;
	void run(const Options& options) const override;
};
