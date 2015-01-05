#include "common/platform.h"
#include "admin/registered_admin_connection.h"

#include <unistd.h>
#include <cstdio>
#include <iostream>

#include "common/exceptions.h"
#include "common/matocl_communication.h"
#include "common/md5.h"
#include "common/cltoma_communication.h"

namespace {

std::string getPassword() {
	std::string password;
	if (isatty(fileno(stdin))) {
		password = getpass("Admin password: ");
	} else {
		std::cin >> password;
	}
	return password;
}

} // anonymous namespace

std::unique_ptr<RegisteredAdminConnection> RegisteredAdminConnection::create(
		const std::string& host,
		const std::string& port) {
	// Connect the master server
	auto connection = std::unique_ptr<RegisteredAdminConnection>(
			new RegisteredAdminConnection(host, port));

	// Get a challenge
	auto challengeMessage = connection->sendAndReceive(
			cltoma::adminRegister::build(), LIZ_MATOCL_ADMIN_REGISTER_CHALLENGE);
	LizMatoclAdminRegisterChallengeData challenge;
	matocl::adminRegisterChallenge::deserialize(challengeMessage, challenge);

	// Read a password from stdin and send the response
	auto password = getPassword();
	auto response = md5_challenge_response(challenge, password);
	std::fill(password.begin(), password.end(), char(0)); // shred the password
	auto registerResponse = connection->sendAndReceive(
			cltoma::adminRegisterResponse::build(response), LIZ_MATOCL_ADMIN_REGISTER_RESPONSE);

	// Receive information about authentication
	uint8_t status;
	matocl::adminRegisterResponse::deserialize(registerResponse, status);
	if (status != STATUS_OK) {
		throw ConnectionException(
				std::string("Authentication with the admin password failed: ") + mfsstrerr(status));
	}
	return connection;
}
