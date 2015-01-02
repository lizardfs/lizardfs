#pragma once

#include "common/platform.h"

#include <string>
#include <vector>

#include "common/network_address.h"
#include "common/packet.h"

class ServerConnection {
public:
	enum class ReceiveMode {
		kReceiveFirstNonNopMessage, ///< Ignores ANTOAN_NOP responses
		kReceiveFirstMessage,       ///< Returns the first received response, even ANTOAN_NOP
	};

	ServerConnection(const std::string& host, const std::string& port);
	ServerConnection(const NetworkAddress& server);
	ServerConnection(int fd);
	ServerConnection(ServerConnection&& connection);
	~ServerConnection();

	std::vector<uint8_t> sendAndReceive(
			const std::vector<uint8_t>& request,
			PacketHeader::Type expectedResponseType,
			ReceiveMode receiveMode = ReceiveMode::kReceiveFirstNonNopMessage);

	static std::vector<uint8_t> sendAndReceive(
			int fd,
			const std::vector<uint8_t>& request,
			PacketHeader::Type expectedResponseType,
			ReceiveMode receiveMode = ReceiveMode::kReceiveFirstNonNopMessage);
private:
	int fd_;

	void connect(const NetworkAddress& server);
};

