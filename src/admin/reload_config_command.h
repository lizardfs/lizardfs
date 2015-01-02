#pragma once

#include "common/platform.h"

#include "admin/lizardfs_admin_command.h"

class ReloadConfigCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const override;
	virtual void usage() const override;
	virtual void run(const Options& options) const override;
};
