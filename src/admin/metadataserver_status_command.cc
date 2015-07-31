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
#include "admin/metadataserver_status_command.h"

#include <iomanip>
#include <iostream>

#include "protocol/cltoma.h"
#include "protocol/matocl.h"

std::string MetadataserverStatusCommand::name() const {
	return "metadataserver-status";
}

void MetadataserverStatusCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Prints status of a master or shadow master server" << std::endl;
}

LizardFsProbeCommand::SupportedOptions MetadataserverStatusCommand::supportedOptions() const {
	return { {kPorcelainMode, kPorcelainModeDescription} };
}

void MetadataserverStatusCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	MetadataserverStatus s = MetadataserverStatusCommand::getStatus(connection);

	if (options.isSet(kPorcelainMode)) {
		std::cout << s.personality << "\t" << s.serverStatus << "\t"
				<< s.metadataVersion << std::endl;
	} else {
		std::cout << "     personality: " << s.personality << std::endl;
		std::cout << "   server status: " << s.serverStatus << std::endl;
		std::cout << "metadata version: " << s.metadataVersion << std::endl;
	}
}

MetadataserverStatus MetadataserverStatusCommand::getStatus(ServerConnection& connection) {
	std::vector<uint8_t> request;
	request = cltoma::metadataserverStatus::build(1);
	auto response = connection.sendAndReceive(request, LIZ_MATOCL_METADATASERVER_STATUS);

	uint32_t messageId;
	uint8_t status;
	uint64_t metadataVersion;
	matocl::metadataserverStatus::deserialize(response, messageId, status, metadataVersion);

	std::string personality, serverStatus;
	switch (status) {
	case LIZ_METADATASERVER_STATUS_MASTER:
		personality = "master";
		serverStatus = "running";
		break;
	case LIZ_METADATASERVER_STATUS_SHADOW_CONNECTED:
		personality = "shadow";
		serverStatus = "connected";
		break;
	case LIZ_METADATASERVER_STATUS_SHADOW_DISCONNECTED:
		personality = "shadow";
		serverStatus = "disconnected";
		break;
	default:
		personality = "<unknown>";
		serverStatus = "<unknown>";
	}
	return MetadataserverStatus{personality, serverStatus, metadataVersion};
}
