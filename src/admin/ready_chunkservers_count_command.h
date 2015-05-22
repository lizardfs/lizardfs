#pragma once

#include "common/platform.h"

#include "admin/lizardfs_admin_command.h"

class ReadyChunkserversCountCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual void usage() const;
	virtual void run(const Options& options) const;
};
