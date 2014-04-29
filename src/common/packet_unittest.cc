#include "config.h"
#include "common/packet.h"

#include <gtest/gtest.h>

TEST(PacketTests, PacketHeaderSize) {
	// workaround: ASSERT_EQ(x, y) requires &x and &y to be valid expressions,
	// which is not true for constants defined in header files
	uint32_t packetHeaderSize = PacketHeader::kSize;
	PacketHeader header(1000, 1);
	ASSERT_EQ(serializedSize(header), packetHeaderSize) <<
			"The constant PacketHeader::kSize defined in hacket.h has wrong value";
}
