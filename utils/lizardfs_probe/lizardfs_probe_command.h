#pragma once

#include <string>
#include <vector>

#include "common/exception.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(WrongUsageException, Exception);

class LizardFsProbeCommand {
public:
	static const std::string kPorcelainMode;
	static const std::string kVerboseMode;

	virtual ~LizardFsProbeCommand() {}
	virtual std::string name() const = 0;
	virtual void usage() const = 0;
	virtual void run(const std::vector<std::string>& argv) const = 0;
};
