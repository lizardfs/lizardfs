#pragma once

#include "common/platform.h"

#include "admin/lizardfs_admin_command.h"

class ListTapeserversCommand : public LizardFsProbeCommand {
public:
	std::string name() const override;
	void usage() const override;
	SupportedOptions supportedOptions() const override;
	void run(const Options& options) const override;
};
