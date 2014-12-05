#include "common/platform.h"
#include "common/server_connection.h"

#include "common/message_receive_buffer.h"
#include "common/mfserr.h"
#include "common/multi_buffer_writer.h"
#include "common/sockets.h"
#include "common/time_utils.h"

static const uint32_t kTimeout_ms = 5000;

ServerConnection::ServerConnection(const std::string& host, const std::string& port) : fd_(-1) {
	NetworkAddress server;
	tcpresolve(host.c_str(), port.c_str(), &server.ip, &server.port, false);
	connect(server);
}

ServerConnection::ServerConnection(const NetworkAddress& server) : fd_(-1) {
	connect(server);
}

ServerConnection::ServerConnection(int fd) : fd_(fd) { }

ServerConnection::~ServerConnection() {
	if (fd_ != -1) {
		tcpclose(fd_);
	}
}

std::vector<uint8_t> ServerConnection::sendAndReceive(
		const std::vector<uint8_t>& request,
		PacketHeader::Type expectedType,
		ReceiveMode receiveMode) {
	return ServerConnection::sendAndReceive(fd_, request, expectedType, receiveMode);
}

std::vector<uint8_t> ServerConnection::sendAndReceive(
		int fd,
		const std::vector<uint8_t>& request,
		PacketHeader::Type expectedType,
		ReceiveMode receiveMode) {
	Timeout timeout{std::chrono::milliseconds(kTimeout_ms)};
	// Send
	MultiBufferWriter writer;
	writer.addBufferToSend(request.data(), request.size());
	while (writer.hasDataToSend()) {
		int status = tcptopoll(fd, POLLOUT, timeout.remaining_ms());
		if (status == 0 || timeout.expired()) {
			throw Exception("Can't write data to socket: timeout");
		} else if (status < 0) {
			throw Exception("Can't write data to socket: " + std::string(strerr(errno)));
		}
		ssize_t bytesWritten = writer.writeTo(fd);
		if (bytesWritten < 0) {
			throw Exception("Can't write data to socket: " + std::string(strerr(errno)));
		}
	}

	// Receive
	MessageReceiveBuffer reader(4 * 1024 * 1024);
	while (!reader.hasMessageData()) {
		int status = tcptopoll(fd, POLLIN, timeout.remaining_ms());
		if (status == 0 || timeout.expired()) {
			throw Exception("Can't read data from socket: timeout");
		} else if (status < 0) {
			throw Exception("Can't read data from socket: " + std::string(strerr(errno)));
		}
		ssize_t bytesRead = reader.readFrom(fd);
		if (bytesRead == 0) {
			throw Exception("Can't read data from socket: connection reset by peer");
		}
		if (bytesRead < 0) {
			throw Exception("Can't read data from socket: " + std::string(strerr(errno)));
		}
		if (reader.isMessageTooBig()) {
			throw Exception("Receive buffer overflow");
		}
		if (reader.hasMessageData()
				&& receiveMode == ReceiveMode::kReceiveFirstNonNopMessage
				&& reader.getMessageHeader().type == ANTOAN_NOP) {
			// We have received a NOP message and were instructed to ignore it
			reader.removeMessage();
		}
	}

	if (reader.getMessageHeader().type != expectedType) {
		throw Exception("Received unexpected message #" +
				std::to_string(reader.getMessageHeader().type));
	}

	uint32_t length = reader.getMessageHeader().length;
	return std::vector<uint8_t>(reader.getMessageData(), reader.getMessageData() + length);
}

void ServerConnection::connect(const NetworkAddress& server) {
	fd_ = tcpsocket();
	if (fd_ < 0) {
		throw Exception("Can't create socket: " + std::string(strerr(errno)));
	}
	tcpnonblock(fd_);
	if (tcpnumtoconnect(fd_, server.ip, server.port, kTimeout_ms) != 0) {
		tcpclose(fd_);
		fd_ = -1;
		throw Exception("Can't connect to " + server.toString() + ": " + strerr(errno));
	}
}
