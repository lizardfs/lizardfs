/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include "unittests/mocks/module_mock.h"

#include <gtest/gtest.h>

#include "common/MFSCommunication.h"
#include "common/sockets.h"
#include "unittests/mocks/chunk_connector_mock.h"

class ModuleMockTests : public ::testing::Test {
protected:
	std::unique_ptr<ChunkConnector> connector_;

	Connection makeConnection(ModuleMock& mock) {
		NetworkAddress server(123, 456);
		connector_.reset(new ChunkConnectorMock{{server, &mock}});
		return Connection(*connector_, server, Timeout{std::chrono::milliseconds(200)});
	}

	void TearDown() {
		connector_.reset();
	}
};

TEST_F(ModuleMockTests, WaitForPacketsReceived) {
	ModuleMock mock;
	Connection connection = makeConnection(mock);
	int fd = connection.fd();

	std::vector<uint8_t> nop(8, 0);
	ASSERT_FALSE(mock.waitForPacketReceived(std::chrono::milliseconds(50)));
	ASSERT_EQ(int(nop.size()), tcptowrite(fd, nop.data(), nop.size(), 200));
	ASSERT_TRUE(mock.waitForPacketReceived(std::chrono::milliseconds(200)));
	ASSERT_EQ(int(nop.size()), tcptowrite(fd, nop.data(), nop.size(), 200));
	ASSERT_EQ(int(nop.size()), tcptowrite(fd, nop.data(), nop.size(), 200));
	ASSERT_EQ(int(nop.size()), tcptowrite(fd, nop.data(), nop.size(), 200));
	ASSERT_TRUE(mock.waitForPacketsReceived(3, std::chrono::milliseconds(200)));
	ASSERT_FALSE(mock.waitForPacketReceived(std::chrono::milliseconds(50)));
	connection.endUsing();
}

TEST_F(ModuleMockTests, DisconnectByServer) {
	class DisconnectMock : public ModuleMock {
	protected:
		void onIncomingMessage(PacketHeader::Type, const std::vector<uint8_t>&) {
			disconnectCurrentClient();
		}
	} mock;

	Connection connection = makeConnection(mock);
	std::vector<uint8_t> nop(8, 0);
	ASSERT_EQ(int(nop.size()), tcptowrite(connection.fd(), nop.data(), nop.size(), 200));
	ASSERT_EQ(0, tcptoread(connection.fd(), nop.data(), nop.size(), 200));
	connection.destroy();
	EXPECT_TRUE(mock.waitForPacketReceived(std::chrono::milliseconds(200)));
}

TEST_F(ModuleMockTests, DisconnectByPeer) {
	class DisconnectMock : public ModuleMock {
	protected:
		void onConnectionEnd() {
			signal();
		}
	} mock;

	Connection connection = makeConnection(mock);
	connection.destroy();
	EXPECT_TRUE(mock.waitForSignal(std::chrono::milliseconds(200)));
}


#define ANTOAN_SUM_REQUEST 1234U
#define ANTOAN_SUM_REPLY 1456U

TEST_F(ModuleMockTests, Respond) {
	class SumModuleMock : public ModuleMock {
		void onIncomingMessage(PacketHeader::Type type, const std::vector<uint8_t>& message) {
			// Read ANTOAN_CALCULATE_SUM request and calculate the sum
			ASSERT_EQ(ANTOAN_SUM_REQUEST, type);
			std::vector<uint8_t> buffer;
			uint32_t a, b;
			deserializePacketDataNoHeader(message, a, b);
			a += b;

			// Respond with ANTOAN_SUM
			buffer.clear();
			serializePacket(buffer, ANTOAN_SUM_REPLY, 0, a);
			respondToCurrentClient(std::move(buffer));
		}
	} mock;

	Connection connection = makeConnection(mock);
	for (uint32_t i = 0; i < 20; ++i) {
		// Send request to add 9 to i
		std::vector<uint8_t> message;
		serializePacket(message, ANTOAN_SUM_REQUEST, 0, uint32_t(i), uint32_t(9));
		ASSERT_EQ(int(message.size()),
				tcptowrite(connection.fd(), message.data(), message.size(), 200));

		// Receive response
		MessageReceiveBuffer recv(100);
		while (!recv.hasMessageData()) {
			if (tcptopoll(connection.fd(), POLLIN, 200) < 1) {
				FAIL() << "Timeout when reading from mock";
			}
			if (recv.readFrom(connection.fd()) <= 0) {
				FAIL() << "Connection with mock lock";
			}
		}
		PacketHeader header = recv.getMessageHeader();

		// Validate it
		ASSERT_EQ(ANTOAN_SUM_REPLY, header.type);
		uint32_t sum;
		deserializePacketDataNoHeader(recv.getMessageData(), header.length, sum);
		EXPECT_EQ(i + 9, sum);
	}
	connection.endUsing();
}
