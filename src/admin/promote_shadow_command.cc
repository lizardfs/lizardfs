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
#include "admin/promote_shadow_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

std::string PromoteShadowCommand::name() const {
	return "promote-shadow";
}

void PromoteShadowCommand::usage() const {
	std::cerr << name() << " <shadow ip> <shadow port>" << std::endl;
	std::cerr << "    Promotes metadata server. Works only if personality 'ha-cluster-managed'"
			"is used." << std::endl;
	std::cerr << "    Authentication with the admin password is required." << std::endl;
}

void PromoteShadowCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <shadow ip> and <shadow port>"
				" for " + name());
	}
	auto connection = RegisteredAdminConnection::create(options.argument(0), options.argument(1));
	auto becomeMasterResponse = connection->sendAndReceive(
			cltoma::adminBecomeMaster::build(), LIZ_MATOCL_ADMIN_BECOME_MASTER);
	uint8_t status;
	matocl::adminBecomeMaster::deserialize(becomeMasterResponse, status);
	std::cerr << lizardfs_error_string(status) << std::endl;
	if (status != LIZARDFS_STATUS_OK) {
		exit(1);
	}

	// The server claims that is successfully changed personality to master, let's double check it
	auto response = connection->sendAndReceive(
			cltoma::metadataserverStatus::build(1), LIZ_MATOCL_METADATASERVER_STATUS);
	uint32_t messageId;
	uint64_t metadataVersion;
	matocl::metadataserverStatus::deserialize(response, messageId, status, metadataVersion);
	if (status != LIZ_METADATASERVER_STATUS_MASTER) {
		std::cerr << "Metadata server promotion failed for unknown reason" << std::endl;
		exit(1);
	}
}
