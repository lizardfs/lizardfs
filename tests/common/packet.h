#ifndef LIZARDFS_TESTS_COMMON_PACKET_H_
#define LIZARDFS_TESTS_COMMON_PACKET_H_

#include <cstdint>
#include <vector>

#include "mfscommon/packet.h"

inline std::vector<uint8_t> removeHeader(const std::vector<uint8_t>& packet) {
	std::vector<uint8_t> ret(packet.begin() + PacketHeader::kSize, packet.end());
	return ret;
}

#endif /* LIZARDFS_TESTS_COMMON_PACKET_H_ */
