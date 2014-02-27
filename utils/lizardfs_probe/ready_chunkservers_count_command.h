#pragma once

#include "utils/lizardfs_probe/lizardfs_probe_command.h"

class ReadyChunkserversCountCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual void usage() const;
	virtual void run(const std::vector<std::string>& argv) const;
};
