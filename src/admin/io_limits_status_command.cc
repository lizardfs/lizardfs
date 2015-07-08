/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "admin/io_limits_status_command.h"

#include <iomanip>
#include <iostream>

#include "common/cltoma_communication.h"
#include "common/server_connection.h"

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
	auto request = cltoma::iolimitsStatus::build(1);
	auto response = connection.sendAndReceive(request, LIZ_MATOCL_IOLIMITS_STATUS);

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
	result << std::to_string(period_us / 1000) << '.' <<
			std::setw(3) << std::setfill('0') << std::to_string(period_us % 1000);
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
