#ifndef LIZARDFS_TESTS_COMMON_PACKET_H_
#define LIZARDFS_TESTS_COMMON_PACKET_H_

#include <cstdint>
#include <vector>

#include "common/packet.h"

inline void removeHeaderInPlace(std::vector<uint8_t>& packet) {
	sassert(packet.size() >= PacketHeader::kSize);
	packet.erase(packet.begin(), packet.begin() + PacketHeader::kSize);
}

inline void verifyHeader(const std::vector<uint8_t>& messageWithHeader,
		PacketHeader::Type expectedType) {
	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(messageWithHeader, header));
	EXPECT_EQ(expectedType, header.type);
	EXPECT_EQ(messageWithHeader.size() - PacketHeader::kSize, header.length);
}

inline void verifyHeaderInPrefix(const std::vector<uint8_t>& messagePrefixWithHeader,
		PacketHeader::Type type, uint32_t extraDataSize) {
	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(messagePrefixWithHeader, header));
	EXPECT_EQ(type, header.type);
	EXPECT_EQ(messagePrefixWithHeader.size() + extraDataSize - PacketHeader::kSize, header.length);
}

inline void verifyVersion(const std::vector<uint8_t>& messageWithoutHeader,
		PacketVersion expectedVersion) {
	PacketVersion version = !expectedVersion;
	ASSERT_NO_THROW(deserializePacketVersionNoHeader(messageWithoutHeader, version));
	EXPECT_EQ(expectedVersion, version);
}

#endif /* LIZARDFS_TESTS_COMMON_PACKET_H_ */
