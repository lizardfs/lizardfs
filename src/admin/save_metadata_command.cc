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
#include "admin/save_metadata_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

std::string SaveMetadataCommand::name() const {
	return "save-metadata";
}

void SaveMetadataCommand::usage() const {
	std::cerr << name() << " <metadataserver ip> <metadataserver port>\n"
			"    Requests saving the current state of metadata into the metadata.mfs file.\n"
			"    With --async fail if the process cannot be started, e.g. because the process\n"
			"    is already in progress. Without --async, fails if either the process cannot be\n"
			"    started or if it finishes with an error (i.e., no metadata file is created).\n"
			"    Authentication with the admin password is required." << std::endl;
}

LizardFsProbeCommand::SupportedOptions SaveMetadataCommand::supportedOptions() const {
	return { {"--async", "Don't wait for the task to finish."} };
}

void SaveMetadataCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException(
				"Expected <metadataserver ip> and <metadataserver port> for " + name());
	}

	bool async = options.isSet("--async");
	auto connection = RegisteredAdminConnection::create(options.argument(0), options.argument(1));
	auto request = cltoma::adminSaveMetadata::build(async);
	auto response = connection->sendAndReceive(request, LIZ_MATOCL_ADMIN_SAVE_METADATA);
	uint8_t status;
	matocl::adminSaveMetadata::deserialize(response, status);
	std::cerr << lizardfs_error_string(status) << std::endl;
	if (status != LIZARDFS_STATUS_OK) {
		exit(1);
	}
}
