#include "config.h"
#include "probe/io_limits_status_command.h"

#include <iomanip>
#include <iostream>

#include "common/cltoma_communication.h"
#include "common/server_connection.h"
#include "common/to_string.h"

std::string IoLimitsStatusCommand::name() const {
	return "iolimits-status";
}

void IoLimitsStatusCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Prints current configuration of global I/O limiting" << std::endl;
}

LizardFsProbeCommand::SupportedOptions IoLimitsStatusCommand::supportedOptions() const {
	return { {kPorcelainMode, kPorcelainModeDescription} };
}

void IoLimitsStatusCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	std::vector<uint8_t> request, response;
	cltoma::iolimitsStatus::serialize(request, 1);
	response = connection.sendAndReceive(request, LIZ_MATOCL_IOLIMITS_STATUS);

	uint32_t messageId, configId, period_us, accumulate_ms;
	std::string subsystem;
	std::vector<IoGroupAndLimit> groupsAndLimits;
	matocl::iolimitsStatus::deserialize(response, messageId, configId,
			period_us, accumulate_ms, subsystem, groupsAndLimits);

	if (options.isSet(kPorcelainMode)) {
		printPorcelain(configId, period_us, accumulate_ms, subsystem, groupsAndLimits);
	} else {
		printStandard(configId, period_us, accumulate_ms, subsystem, groupsAndLimits);
	}
}

void IoLimitsStatusCommand::printStandard(uint32_t configId,
		uint32_t period_us, uint32_t accumulate_ms, const std::string& subsystem,
		const std::vector<IoGroupAndLimit>& groupsAndLimits) const {
	if (isLimitingDisabled(subsystem, groupsAndLimits)) {
		std::cout << "Global I/O limiting disabled" << std::endl;
		return;
	}
	std::cout << "configuration ID:\t" << configId << std::endl;
	std::cout << "renegotiation period:\t" << printPeriod(period_us) << " ms" << std::endl;
	std::cout << "bandwidth accumulation:\t" << accumulate_ms << " ms" << std::endl;
	if (!subsystem.empty()) {
		std::cout << "subsystem:\t" << subsystem << std::endl;
	}
	for (const auto& entry : groupsAndLimits) {
		std::cout << "group:\t" << entry.group << " = " << entry.limit / 1024 << " KiB/s"
				<< std::endl;
	}
}

void IoLimitsStatusCommand::printPorcelain(uint32_t configId,
		uint32_t period_us, uint32_t accumulate_ms, const std::string& subsystem,
		const std::vector<IoGroupAndLimit>& groupsAndLimits) const {
	if (isLimitingDisabled(subsystem, groupsAndLimits)) {
		return;
	}
	std::cout << configId <<
			" " << printPeriod(period_us) <<
			" " << accumulate_ms;
	if (!subsystem.empty()) {
		std::cout << " " << subsystem;
	}
	for (const auto& entry : groupsAndLimits) {
		std::cout << std::endl << entry.group << " " << entry.limit / 1024;
	}
	std::cout << std::endl;
}

std::string IoLimitsStatusCommand::printPeriod(uint32_t period_us) const {
	std::stringstream result;
	result << toString(period_us / 1000) << '.' <<
			std::setw(3) << std::setfill('0') << toString(period_us % 1000);
	return result.str();
}

bool IoLimitsStatusCommand::isLimitingDisabled(const std::string& subsystem,
		const std::vector<IoGroupAndLimit>& groups) const {
	for (auto group : groups) {
		if (group.group == "unclassified") {
			return false;
		}
	}
	return subsystem.empty();
}
