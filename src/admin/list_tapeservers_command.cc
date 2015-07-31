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
#include "admin/list_tapeservers_command.h"

#include <iomanip>
#include <iostream>

#include "protocol/cltoma.h"
#include "common/lizardfs_version.h"
#include "protocol/matocl.h"
#include "common/server_connection.h"

std::string ListTapeserversCommand::name() const {
	return "list-tapeservers";
}

void ListTapeserversCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Prints status of active tapeservers" << std::endl;
}

LizardFsProbeCommand::SupportedOptions ListTapeserversCommand::supportedOptions() const {
	return { {kPorcelainMode, kPorcelainModeDescription} };
}

void ListTapeserversCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	auto request = cltoma::listTapeservers::build();
	auto response = connection.sendAndReceive(request, LIZ_MATOCL_LIST_TAPESERVERS);

	std::vector<TapeserverListEntry> tapeservers;
	matocl::listTapeservers::deserialize(response, tapeservers);

	for (const auto& t : tapeservers) {
		if (options.isSet(kPorcelainMode)) {
			std::cout << t.address.toString() << ' ' << t.server << ' '
					<< lizardfsVersionToString(t.version) << ' ' << t.label << std::endl;
		} else {
			std::cout << "Server " << t.server << ":"
					<< "\n\tversion: " << lizardfsVersionToString(t.version)
					<< "\n\taddress: " << t.address.toString()
					<< "\n\tlabel: " << t.label << std::endl;
		}
	}
}
