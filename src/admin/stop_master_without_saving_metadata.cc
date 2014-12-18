#include "common/platform.h"
#include "admin/stop_master_without_saving_metadata.h"

#include <unistd.h>
#include <iomanip>
#include <iostream>

#include "admin/register_connection.h"
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

LizardFsProbeCommand::SupportedOptions MetadataserverStopWithoutSavingMetadataCommand::supportedOptions() const {
	return {};
}

void MetadataserverStopWithoutSavingMetadataCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <metadataserver ip> and <metadataserver port>"
				" for " + name());
	}
	std::string password = get_password();

	ServerConnection connection(options.argument(0), options.argument(1));
	uint8_t status = register_master_connection(connection, password);

	if (status != STATUS_OK) {
		std::cerr << "Error registering to master" << std::endl;
		exit(1);
	}

	auto adminStopWithoutMetadataDumpResponse = connection.sendAndReceive(
			cltoma::adminStopWithoutMetadataDump::build(),
			LIZ_MATOCL_ADMIN_STOP_WITHOUT_METADATA_DUMP);
	matocl::adminStopWithoutMetadataDump::deserialize(adminStopWithoutMetadataDumpResponse, status);
	std::cerr << mfsstrerr(status) << std::endl;
	if (status != STATUS_OK) {
		exit(1);
	}
}
