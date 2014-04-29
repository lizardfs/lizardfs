#pragma once

#include "config.h"

#include <string>
#include <vector>

#include "common/exception.h"
#include "probe/options.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(WrongUsageException, Exception);

class LizardFsProbeCommand {
public:
	typedef std::vector<std::pair<std::string, std::string>> SupportedOptions;

	static const std::string kPorcelainMode;
	static const std::string kPorcelainModeDescription;
	static const std::string kVerboseMode;

	virtual ~LizardFsProbeCommand() {}
	virtual std::string name() const = 0;
	virtual void usage() const = 0;
	virtual SupportedOptions supportedOptions() const { return {}; }
	virtual void run(const Options& options) const = 0;
};
