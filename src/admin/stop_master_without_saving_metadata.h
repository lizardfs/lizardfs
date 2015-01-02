#pragma once

#include "common/platform.h"

#include "common/server_connection.h"
#include "admin/lizardfs_admin_command.h"

class MetadataserverStopWithoutSavingMetadataCommand : public LizardFsProbeCommand {
public:
	std::string name() const override;
	void usage() const override;
	void run(const Options& options) const override;
};
