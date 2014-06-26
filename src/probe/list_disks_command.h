#pragma once

#include "common/platform.h"

#include "probe/lizardfs_probe_command.h"

class ListDisksCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual SupportedOptions supportedOptions() const;
	virtual void usage() const;
	virtual void run(const Options& options) const;
};
