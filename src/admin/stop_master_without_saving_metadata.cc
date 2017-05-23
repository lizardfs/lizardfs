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
#include "admin/stop_master_without_saving_metadata.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

std::string MetadataserverStopWithoutSavingMetadataCommand::name() const {
	return "stop-master-without-saving-metadata";
}

void MetadataserverStopWithoutSavingMetadataCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Stop the master server without saving metadata in the metadata.mfs file."
			<< std::endl;
	std::cerr << "    Used to quickly migrate a metadata server (works for all personalities)."
			<< std::endl;
	std::cerr << "    Authentication with the admin password is required." << std::endl;
}

void MetadataserverStopWithoutSavingMetadataCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <metadataserver ip> and <metadataserver port>"
				" for " + name());
	}
	auto connection = RegisteredAdminConnection::create(options.argument(0), options.argument(1));
	auto adminStopWithoutMetadataDumpResponse = connection->sendAndReceive(
			cltoma::adminStopWithoutMetadataDump::build(),
			LIZ_MATOCL_ADMIN_STOP_WITHOUT_METADATA_DUMP);
	uint8_t status;
	matocl::adminStopWithoutMetadataDump::deserialize(adminStopWithoutMetadataDumpResponse, status);
	std::cerr << lizardfs_error_string(status) << std::endl;
	if (status != LIZARDFS_STATUS_OK) {
		exit(1);
	}
}
