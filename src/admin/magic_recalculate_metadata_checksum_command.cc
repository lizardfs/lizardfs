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
#include "admin/magic_recalculate_metadata_checksum_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

std::string MagicRecalculateMetadataChecksumCommand::name() const {
	return "magic-recalculate-metadata-checksum";
}

void MagicRecalculateMetadataChecksumCommand::usage() const {
	std::cerr << name() << " <metadataserver ip> <metadataserver port>" << std::endl;
	std::cerr << "    Requests recalculation of metadata checksum." << std::endl;
	std::cerr << "    Authentication with the admin password is required." << std::endl;
}

LizardFsProbeCommand::SupportedOptions MagicRecalculateMetadataChecksumCommand::supportedOptions() const {
	return { {"--async", "Don't wait for the task to finish."},
	         {"--timeout=", "Operation timeout" }};
}

void MagicRecalculateMetadataChecksumCommand::run(const Options& options) const {
	bool async = options.isSet("--async");

	int timeout = 1000 * options.getValue<int>("--timeout", ServerConnection::kDefaultTimeout / 1000);
	auto connection = RegisteredAdminConnection::create(options.argument(0), options.argument(1), timeout);
	auto request = cltoma::adminRecalculateMetadataChecksum::build(async);
	auto response = connection->sendAndReceive(request, LIZ_MATOCL_ADMIN_RECALCULATE_METADATA_CHECKSUM);
	uint8_t status;
	matocl::adminRecalculateMetadataChecksum::deserialize(response, status);
	std::cerr << lizardfs_error_string(status) << std::endl;
	if (status != LIZARDFS_STATUS_OK) {
		exit(1);
	}
}
