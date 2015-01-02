#include "common/platform.h"
#include "admin/stop_master_without_saving_metadata.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"

std::string MetadataserverStopWithoutSavingMetadataCommand::name() const {
	return "stop-master-without-saving-metadata";
}

void MetadataserverStopWithoutSavingMetadataCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Stop master server without dumping metadata." << std::endl;
	std::cerr << "    Used to quickly migrate master server. Works" << std::endl;
	std::cerr << "    only if personality 'auto' is used." << std::endl;
	std::cerr << "    Authentication needed." << std::endl;
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
	std::cerr << mfsstrerr(status) << std::endl;
	if (status != STATUS_OK) {
		exit(1);
	}
}
