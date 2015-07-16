#include "common/platform.h"
#include "admin/magic_recalculate_metadata_checksum_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"

std::string MagicRecalculateMetadataChecksumCommand::name() const {
	return "magic-recalculate-metadata-checksum";
}

void MagicRecalculateMetadataChecksumCommand::usage() const {
	std::cerr << name() << " <metadataserver ip> <metadataserver port>" << std::endl;
	std::cerr << "    Requests recalculation of metadata checksum." << std::endl;
	std::cerr << "    Authentication with the admin password is required." << std::endl;
}

LizardFsProbeCommand::SupportedOptions MagicRecalculateMetadataChecksumCommand::supportedOptions() const {
	return { {"--async", "Don't wait for the task to finish."} };
}

void MagicRecalculateMetadataChecksumCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException(
				"Expected <metadataserver ip> and <metadataserver port> for " + name());
	}

	bool async = options.isSet("--async");
	auto connection = RegisteredAdminConnection::create(options.argument(0), options.argument(1));
	auto request = cltoma::adminRecalculateMetadataChecksum::build(async);
	auto response = connection->sendAndReceive(request, LIZ_MATOCL_ADMIN_RECALCULATE_METADATA_CHECKSUM);
	uint8_t status;
	matocl::adminRecalculateMetadataChecksum::deserialize(response, status);
	std::cerr << mfsstrerr(status) << std::endl;
	if (status != LIZARDFS_STATUS_OK) {
		exit(1);
	}
}
