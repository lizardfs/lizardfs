#pragma once

#include "common/platform.h"

#include <memory>

#include "common/server_connection.h"

/// An authenticated (using an admin password) version of cl<->ma connection.
class RegisteredAdminConnection : public ServerConnection {
public:
	/// Creates a new registered admin connection.
	/// Asks for a password and authenticates using an challenge-response mechanism.
	/// Throws if something goes wrong.
	static std::unique_ptr<RegisteredAdminConnection> create(
			const std::string& host,
			const std::string& port);

private:
	/// Private constructor for ::create.
	RegisteredAdminConnection(const std::string host, const std::string& port)
		: ServerConnection(host, port) {
	}
};
