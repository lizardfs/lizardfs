#pragma once

#include "config.h"

#include "probe/lizardfs_probe_command.h"

class ReadyChunkserversCountCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual void usage() const;
	virtual void run(const Options& options) const;
};
