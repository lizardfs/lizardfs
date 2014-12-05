#include "common/platform.h"
#include "common/server_connection.h"

#include <gtest/gtest.h>

#include "common/packet.h"
#include "unittests/mocks/module_mock.h"

namespace {

// A mock which responds with a ANTOAN_NOP followed by
// a message with type increased by one and some random body
class NopResponder : public ModuleMock {
public:
	void onIncomingMessage(PacketHeader::Type type, const std::vector<uint8_t>&) override {
		respondToCurrentClient(buildMooseFsPacket(ANTOAN_NOP));
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
