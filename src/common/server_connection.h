#pragma once

#include "common/platform.h"

#include <string>
#include <vector>

#include "common/network_address.h"
#include "common/packet.h"

class ServerConnection {
public:
	ServerConnection(const std::string& host, const std::string& port);
	ServerConnection(const NetworkAddress& server);
	ServerConnection(int fd);
	~ServerConnection();

	std::vector<uint8_t> sendAndReceive(
			const std::vector<uint8_t>& request,
			PacketHeader::Type expectedResponseType);
private:
	int fd_;

	void connect(const NetworkAddress& server);
};

