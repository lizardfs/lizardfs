#ifndef LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_

#include "mfscommon/packet.h"

namespace cstoma {

namespace registerHost {

void serialize(std::vector<uint8_t>& destination,
		uint32_t ip, uint16_t port, uint16_t timeout) {
	serializePacket(destination, LIZ_CSTOMA_REGISTER_HOST, 0, ip, port, timeout);
}

void deserialize(const std::vector<uint8_t>& source,
		uint32_t& ip, uint16_t& port, uint16_t& timeout) {
	deserializePacketDataNoHeader(source, ip, port, timeout);
}

} // namespace registerHost

} // namespace cstoma

#endif /* LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_ */
