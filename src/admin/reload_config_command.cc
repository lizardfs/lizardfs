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
#include "admin/reload_config_command.h"

#include <unistd.h>
#include <iomanip>
#include <iostream>

#include "admin/registered_admin_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

std::string ReloadConfigCommand::name() const {
	return "reload-config";
}

void ReloadConfigCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Requests reloading configuration from the config file." << std::endl;
	std::cerr << "    This is synchronous (waits for reload to finish)." << std::endl;
	std::cerr << "    Authentication with the admin password is required." << std::endl;
}

void ReloadConfigCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException(
				"Expected <metadataserver ip> and <metadataserver port> for " + name());
	}

	auto connection = RegisteredAdminConnection::create(options.argument(0), options.argument(1));
	auto adminReloadResponse =
			connection->sendAndReceive(cltoma::adminReload::build(), LIZ_MATOCL_ADMIN_RELOAD);
	uint8_t status;
	matocl::adminStopWithoutMetadataDump::deserialize(adminReloadResponse, status);
	std::cerr << lizardfs_error_string(status) << std::endl;
	if (status != LIZARDFS_STATUS_OK) {
		exit(1);
	}
}
