#include "common/platform.h"
#include "unittests/mocks/module_mock.h"

#include <poll.h>
#include <thread>
#include <gtest/gtest.h>

#include "common/sockets.h"

ModuleMock::ModuleMock()
		: sock_(-1),
		  terminate_(false),
		  currentClientFd_(-1),
		  receivedPackets_(0),
		  signals_(0) {
}

ModuleMock::~ModuleMock() {
	terminate_ = true;
	thread_.join();
}

void ModuleMock::respondToCurrentClient(std::vector<uint8_t> message) {
	clients_.at(currentClient()).sendQueue.push(std::move(message));
}

void ModuleMock::disconnectCurrentClient() {
	onConnectionEnd();
	tcpclose(currentClient());
	clients_.erase(currentClient());
}

void ModuleMock::signal() {
	std::unique_lock<std::mutex> lock(mutex_);
	++signals_;
	cond_.notify_all();
}

void ModuleMock::init() {
	sock_ = tcpsocket();
	if (sock_ < 0) {
		FAIL() << "ModuleMock::init tcpsocket";
	}
	tcpnonblock(sock_);
	tcpnodelay(sock_);
	if (tcpnumlisten(sock_, 0x7f000001, 0, 100) < 0) {
		FAIL() << "ModuleMock::init tcpnumlisten";
	}
	thread_ = std::thread(std::ref(*this));
}

uint16_t ModuleMock::port() const {
	sassert(sock_ >= 0);
	uint16_t p;
	sassert(tcpgetmyaddr(sock_, nullptr, &p) == 0);
	return p;
}

void ModuleMock::operator()() {
	while (!terminate_) {
		std::vector<pollfd> pfd;
		pfd.push_back(pollfd{sock_, POLLIN, 0});
		for (const auto& client : clients_) {
			short int mask = POLLIN;
			if (client.second.writer.hasDataToSend()) {
				mask |= POLLOUT;
			}
			pfd.push_back(pollfd{client.first, mask, 0});
		}
		poll(pfd.data(), pfd.size(), 10);

		for (const auto& p : pfd) {
			if (p.fd == sock_) {
				if (p.revents & POLLIN) {
					int newClient = tcpaccept(sock_);
					if (newClient >= 0) {
						tcpnonblock(newClient);
						clients_[newClient];
						onNewConnection();
					}
				}
			} else {
				serveFd(p.fd, p.revents);
			}
		}
	}
}

void ModuleMock::serveFd(int fd, int revents) {
	currentClientFd_ = fd;
	ClientRecord& client = clients_.at(currentClientFd_);
	if (revents & POLLIN) {
		if (client.receiver.readFrom(fd) <= 0) {
			clients_.erase(fd);
			onConnectionEnd();
			return;
		}
		if (client.receiver.isMessageTooBig()) {
			disconnectCurrentClient();
			return;
		}
		while (client.receiver.hasMessageData()) {
			const auto& header = client.receiver.getMessageHeader();
			std::vector<uint8_t> message(
					client.receiver.getMessageData(),
					client.receiver.getMessageData() + header.length);
			onIncomingMessage(header.type, message);
			std::unique_lock<std::mutex> lock(mutex_);
			++receivedPackets_;
			cond_.notify_all();
			// disconnectCurrentClient could be called in onIncomingMessage
			if (clients_.count(currentClient()) == 0) {
				return;
			}
			client.receiver.removeMessage();
		}
	}
	if (revents & POLLOUT) {
		if (client.writer.writeTo(fd) < 0) {
			disconnectCurrentClient();
			return;
		}
		if (!client.writer.hasDataToSend()) {
			client.writer.reset();
			client.sendQueue.pop();
		}
	}
	if (!client.writer.hasDataToSend() && !client.sendQueue.empty()) {
		client.writer.addBufferToSend(
				client.sendQueue.front().data(),
				client.sendQueue.front().size());
	}
}
