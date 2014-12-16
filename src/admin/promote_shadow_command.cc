#include "common/platform.h"
#include "admin/promote_shadow_command.h"

#include <unistd.h>
#include <array>
#include <iomanip>
#include <iostream>

#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"
#include "common/md5.h"

std::string PromoteShadowCommand::name() const {
	return "promote-shadow";
}

void PromoteShadowCommand::usage() const {
	std::cerr << name() << " <shadow ip> <shadow port>" << std::endl;
	std::cerr << "    Promotes metadata server. Works only if personality 'ha-cluster-managed'"
			"is used." << std::endl;
	std::cerr << "    Authentication needed." << std::endl;
}

LizardFsProbeCommand::SupportedOptions PromoteShadowCommand::supportedOptions() const {
	return {};
}

void PromoteShadowCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <shadow ip> and <shadow port>"
				" for " + name());
	}
	std::string password;
	if (isatty(fileno(stdin))) {
		password = getpass("Admin password: ");
	} else {
		std::cin >> password;
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	auto challangeResponse = connection.sendAndReceive(
			cltoma::adminRegister::build(), LIZ_MATOCL_ADMIN_REGISTER_CHALLENGE);
	LizMatoclAdminRegisterChallangeData challangeData;
	matocl::adminRegisterChallange::deserialize(challangeResponse, challangeData);
	std::array<uint8_t, 16> digest = md5_challenge_response(challangeData, password);
	auto registerResponse = connection.sendAndReceive(
			cltoma::adminRegisterResponse::build(digest), LIZ_MATOCL_ADMIN_REGISTER_RESPONSE);
	uint8_t status;
	matocl::adminRegisterResponse::deserialize(registerResponse, status);
	if (status != STATUS_OK) {
		std::cerr << "Wrong password" << std::endl;
		exit(1);
	}

	auto becomeMasterResponse = connection.sendAndReceive(
			cltoma::adminBecomeMaster::build(), LIZ_MATOCL_ADMIN_BECOME_MASTER);
	std::cerr << mfsstrerr(status) << std::endl;
	if (status != STATUS_OK) {
		exit(1);
	}

	// The server claims that is successfully changed personality to master, let's double check it
	auto response = connection.sendAndReceive(
			cltoma::metadataserverStatus::build(1), LIZ_MATOCL_METADATASERVER_STATUS);
	uint32_t messageId;
	uint64_t metadataVersion;
	matocl::metadataserverStatus::deserialize(response, messageId, status, metadataVersion);
	if (status != LIZ_METADATASERVER_STATUS_MASTER) {
		std::cerr << "Metadata server promotion failed for unknown reason" << std::endl;
		exit(1);
	}
}
