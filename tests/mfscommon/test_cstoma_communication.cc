#include "mfscommon/cstoma_communication.h"

#include <gtest/gtest.h>

#include "mfscommon/crc.h"
#include "mfscommon/strerr.h"

TEST(CstomaCommunicationTests, RegisterHost) {
	uint32_t outIp, inIp = 127001;
	uint16_t outPort, inPort = 8080;
	uint16_t outTimeout, inTimeout = 1;

	std::vector<uint8_t> wholePacketBuffer;
	ASSERT_NO_THROW(cstoma::registerHost::serialize(wholePacketBuffer,
			inIp, inPort, inTimeout));

	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(wholePacketBuffer, header));
	EXPECT_EQ(LIZ_CSTOMA_REGISTER_HOST, header.type);
	EXPECT_EQ(wholePacketBuffer.size() - PacketHeader::kSize, header.length);

	PacketVersion version = 1;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(wholePacketBuffer, version));
	EXPECT_EQ(0u, version);

	std::vector<uint8_t> packetWithoutHeader(
			wholePacketBuffer.begin() + PacketHeader::kSize,
			wholePacketBuffer.end());

	ASSERT_NO_THROW(cstoma::registerHost::deserialize(packetWithoutHeader,
			outIp, outPort, outTimeout));
	EXPECT_EQ(inIp, outIp);
	EXPECT_EQ(inPort, outPort);
	EXPECT_EQ(inTimeout, outTimeout);
}
