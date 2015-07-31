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

#include <gtest/gtest.h>

#include "protocol/packet.h"
#include "unittests/mocks/module_mock.h"

namespace {

// A mock which responds with a series of ANTOAN_NOP followed by
// a message with type increased by one and some random body
class NopResponder : public ModuleMock {
public:
	void onIncomingMessage(PacketHeader::Type type, const std::vector<uint8_t>&) override {
		for (int i = 0; i < 20; ++i) {
			respondToCurrentClient(buildMooseFsPacket(ANTOAN_NOP));
		}
		respondToCurrentClient(buildMooseFsPacket(type + 1, uint32_t(123)));
	}
};

} // anonymous namespace

TEST(ServerConnectionTests, SendAndReceive_ReceiveMode) {
	NopResponder responder;
	responder.init();
	ServerConnection connection(responder.address());

	// Should be OK if NOPs are ignored
	ASSERT_NO_THROW(connection.sendAndReceive(buildMooseFsPacket(10, uint32_t(123)),
			10 + 1, ServerConnection::ReceiveMode::kReceiveFirstNonNopMessage));
	ASSERT_NO_THROW(connection.sendAndReceive(buildMooseFsPacket(21, uint32_t(123)),
			21 + 1, ServerConnection::ReceiveMode::kReceiveFirstNonNopMessage));

	// Should throw if we don't ignore NOPs
	ASSERT_ANY_THROW(connection.sendAndReceive(buildMooseFsPacket(21, uint32_t(123)),
			21 + 1, ServerConnection::ReceiveMode::kReceiveFirstMessage));
}

TEST(ServerConnectionTests, SendAndReceive_ExpectedType) {
	NopResponder responder;
	responder.init();
	ServerConnection connection(responder.address());

	// Should be OK -- actual response is 21 + 1
	ASSERT_NO_THROW(connection.sendAndReceive(buildMooseFsPacket(21, uint32_t(123)), 21 + 1));

	// Should throw -- actual response is 21 + 1, not 321
	ASSERT_ANY_THROW(connection.sendAndReceive(buildMooseFsPacket(21, uint32_t(123)), 321));
}
