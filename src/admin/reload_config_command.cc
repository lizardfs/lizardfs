#include "common/platform.h"
#include "admin/reload_config_command.h"

#include <unistd.h>
#include <iomanip>
#include <iostream>

#include "admin/registered_admin_connection.h"
#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"

std::string ReloadConfigCommand::name() const {
	return "reload-config";
}

void ReloadConfigCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Requests reloading configuration from the config file." << std::endl;
	std::cerr << "    This is synchronous (waits for reload to finish)." << std::endl;
	std::cerr << "    Authentication with admin password required." << std::endl;
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
	std::cerr << mfsstrerr(status) << std::endl;
	if (status != STATUS_OK) {
		exit(1);
	}
}
