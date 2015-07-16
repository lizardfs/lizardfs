#include "common/platform.h"
#include "admin/promote_shadow_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"

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
	std::cerr << mfsstrerr(status) << std::endl;
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
