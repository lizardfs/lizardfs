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
#include "common/server_connection.h"

#include "common/cwrap.h"
#include "common/exceptions.h"
#include "common/message_receive_buffer.h"
#include "common/MFSCommunication.h"
#include "common/mfserr.h"
#include "common/multi_buffer_writer.h"
#include "common/packet.h"
#include "common/sockets.h"
#include "common/time_utils.h"

static const uint32_t kTimeout_ms = 5000;

namespace {

/// Writes the \p request to the \p fd_ socket
void sendRequestGeneric(int fd, const MessageBuffer& request, const Timeout& timeout) {
	MultiBufferWriter writer;
	writer.addBufferToSend(request.data(), request.size());
	while (writer.hasDataToSend()) {
		int status = tcptopoll(fd, POLLOUT, timeout.remaining_ms());
		if (status == 0 || timeout.expired()) {
			throw ConnectionException("Can't write data to socket: timeout");
		} else if (status < 0) {
			throw ConnectionException("Can't write data to socket: " + errorString(errno));
		}
		ssize_t bytesWritten = writer.writeTo(fd);
		if (bytesWritten < 0) {
			throw ConnectionException("Can't write data to socket: " + errorString(errno));
		}
	}
}

/// Reads a message from the \p socket
/// Throws if its type is different than \p expectedType.
/// Ignores NOP messages if \p receiveMode is ReceiveMode::kReceiveFirstNonNopMessage.
MessageBuffer receiveRequestGeneric(
		int fd,
		PacketHeader::Type expectedType,
		ServerConnection::ReceiveMode receiveMode,
		const Timeout& timeout) {
	MessageReceiveBuffer reader(4 * 1024 * 1024);
	while (!reader.hasMessageData()) {
		int status = tcptopoll(fd, POLLIN, timeout.remaining_ms());
		if (status == 0 || timeout.expired()) {
			throw ConnectionException("Can't read data from socket: timeout");
		} else if (status < 0) {
			throw ConnectionException("Can't read data from socket: " + errorString(errno));
		}
		ssize_t bytesRead = reader.readFrom(fd);
		if (bytesRead == 0) {
			throw ConnectionException("Can't read data from socket: connection reset by peer");
		}
		if (bytesRead < 0) {
			throw ConnectionException("Can't read data from socket: " + errorString(errno));
		}
		if (reader.isMessageTooBig()) {
			throw Exception("Receive buffer overflow");
		}
		while (reader.hasMessageData()
				&& receiveMode == ServerConnection::ReceiveMode::kReceiveFirstNonNopMessage
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
	return MessageBuffer(reader.getMessageData(), reader.getMessageData() + length);
}

} // anonymous namespace

ServerConnection::ServerConnection(const std::string& host, const std::string& port) : fd_(-1) {
	NetworkAddress server;
	tcpresolve(host.c_str(), port.c_str(), &server.ip, &server.port, false);
	connect(server);
}

ServerConnection::ServerConnection(const NetworkAddress& server) : fd_(-1) {
	connect(server);
}

ServerConnection::~ServerConnection() {
	if (fd_ != -1) {
		tcpclose(fd_);
	}
}

MessageBuffer ServerConnection::sendAndReceive(
		const MessageBuffer& request,
		PacketHeader::Type expectedType,
		ReceiveMode receiveMode) {
	return ServerConnection::sendAndReceive(fd_, request, expectedType, receiveMode);
}

MessageBuffer ServerConnection::sendAndReceive(
		int fd,
		const MessageBuffer& request,
		PacketHeader::Type expectedType,
		ReceiveMode receiveMode) {
	Timeout timeout{std::chrono::milliseconds(kTimeout_ms)};
	sendRequestGeneric(fd, request, timeout);
	return receiveRequestGeneric(fd, expectedType, receiveMode, timeout);
}

void ServerConnection::connect(const NetworkAddress& server) {
	fd_ = tcpsocket();
	if (fd_ < 0) {
		throw ConnectionException("Can't create socket: " + std::string(strerr(errno)));
	}
	tcpnonblock(fd_);
	if (tcpnumtoconnect(fd_, server.ip, server.port, kTimeout_ms) != 0) {
		tcpclose(fd_);
		fd_ = -1;
		throw ConnectionException("Can't connect to " + server.toString() + ": " + strerr(errno));
	}
}

KeptAliveServerConnection::~KeptAliveServerConnection() {
	threadCanRun_ = false;
	cond_.notify_all();
	nopThread_.join();
}

MessageBuffer KeptAliveServerConnection::sendAndReceive(
		const MessageBuffer& request,
		PacketHeader::Type expectedResponseType,
		ReceiveMode receiveMode) {
	Timeout timeout{std::chrono::milliseconds(kTimeout_ms)};
	/* synchronized with nopThread_ */ {
		std::unique_lock<std::mutex> lock(mutex_);
		sendRequestGeneric(fd_, request, timeout);
	}
	return receiveRequestGeneric(fd_, expectedResponseType, receiveMode, timeout);
}

void KeptAliveServerConnection::startNopThread() {
	auto threadCode = [this]() {
		std::unique_lock<std::mutex> lock(mutex_);
		while (threadCanRun_) {
			cond_.wait_for(lock, std::chrono::seconds(1));
			if (threadCanRun_) {
				// We have the lock, we can send something
				Timeout timeout{std::chrono::milliseconds(kTimeout_ms)};
				sendRequestGeneric(fd_, buildMooseFsPacket(ANTOAN_NOP), timeout);
			}
		}
	};
	nopThread_ = std::thread(threadCode);
}
