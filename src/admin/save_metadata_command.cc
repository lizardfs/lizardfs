#include "common/platform.h"
#include "admin/save_metadata_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"

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
	std::cerr << mfsstrerr(status) << std::endl;
	if (status != LIZARDFS_STATUS_OK) {
		exit(1);
	}
}
