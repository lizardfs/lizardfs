#include "common/platform.h"
#include "admin/register_connection.h"

#include <unistd.h>
#include <array>
#include <iomanip>
#include <iostream>

#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"
#include "common/md5.h"

std::string get_password() {
	std::string password;
	if (isatty(fileno(stdin))) {
		password = getpass("Admin password: ");
	} else {
		std::cin >> password;
	}
	return password;
}

uint8_t register_master_connection(ServerConnection& connection, std::string password) {
	auto challengeResponse = connection.sendAndReceive(
			cltoma::adminRegister::build(), LIZ_MATOCL_ADMIN_REGISTER_CHALLENGE);
	LizMatoclAdminRegisterChallengeData challengeData;
	matocl::adminRegisterChallenge::deserialize(challengeResponse, challengeData);
	std::array<uint8_t, 16> digest = md5_challenge_response(challengeData, password);
	auto registerResponse = connection.sendAndReceive(
			cltoma::adminRegisterResponse::build(digest), LIZ_MATOCL_ADMIN_REGISTER_RESPONSE);
	uint8_t status;
	matocl::adminRegisterResponse::deserialize(registerResponse, status);
	return status;
}
