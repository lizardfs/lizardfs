#ifndef LIZARDFS_TESTS_COMMON_PACKET_H_
#define LIZARDFS_TESTS_COMMON_PACKET_H_

#include <cstdint>
#include <vector>

#include "common/packet.h"

inline std::vector<uint8_t> removeHeader(const std::vector<uint8_t>& packet) {
	std::vector<uint8_t> ret(packet.begin() + PacketHeader::kSize, packet.end());
	return ret;
}

inline std::vector<uint8_t> removeHeaderAndVersion(const std::vector<uint8_t>& packet) {
	std::vector<uint8_t> ret(packet.begin() + PacketHeader::kSize + serializedSize(PacketVersion()),
			packet.end());
	return ret;
}

inline void verifyHeader(const std::vector<uint8_t>& buffer, PacketHeader::Type expectedType) {
	PacketHeader header;
	ASSERT_NO_THROW(deserializePacketHeader(buffer, header));
	EXPECT_EQ(expectedType, header.type);
	EXPECT_EQ(buffer.size() - PacketHeader::kSize, header.length);
}

inline void verifyVersion(const std::vector<uint8_t>& buffer, PacketVersion expectedVersion) {
	PacketVersion version = !expectedVersion;
	ASSERT_NO_THROW(deserializePacketVersionSkipHeader(buffer, version));
	EXPECT_EQ(expectedVersion, version);
}

inline void verifyMessageId(const std::vector<uint8_t>& buffer, uint32_t expectedMessageId) {
	uint32_t messageId;
	deserialize(removeHeaderAndVersion(buffer), messageId);
	EXPECT_EQ(expectedMessageId, messageId);
}

#endif /* LIZARDFS_TESTS_COMMON_PACKET_H_ */
