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
#include "admin/registered_admin_connection.h"

#include <unistd.h>
#include <cstdio>
#include <iostream>

#ifdef _WIN32
#include <windows.h>
#endif

#include "common/exceptions.h"
#include "protocol/matocl.h"
#include "common/md5.h"
#include "protocol/cltoma.h"

namespace {

std::string getPassword() {
	std::string password;
#ifdef _WIN32
	// disable stdin echo
	HANDLE hStdin = GetStdHandle(STD_INPUT_HANDLE);
	DWORD mode = 0;
	GetConsoleMode(hStdin, &mode);
	SetConsoleMode(hStdin, mode & (~ENABLE_ECHO_INPUT));

	std::cin >> password;

	// enable stdin echo
	SetConsoleMode(hStdin, mode);
#else
	if (isatty(fileno(stdin))) {
		password = getpass("Admin password: ");
	} else {
		std::cin >> password;
	}
#endif
	return password;
}

} // anonymous namespace

std::unique_ptr<RegisteredAdminConnection> RegisteredAdminConnection::create(
		const std::string& host,
		const std::string& port,
		int timeout) {
	// Connect the master server
	auto connection = std::unique_ptr<RegisteredAdminConnection>(
			new RegisteredAdminConnection(host, port));

	connection->setTimeout(timeout);

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
	if (status != LIZARDFS_STATUS_OK) {
		throw ConnectionException(
				std::string("Authentication with the admin password failed: ") + lizardfs_error_string(status));
	}
	return connection;
}
