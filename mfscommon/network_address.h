#ifndef LIZARDFS_MFSCOMMON_NETWORK_ADDRESS_H_
#define LIZARDFS_MFSCOMMON_NETWORK_ADDRESS_H_

#include "mfscommon/serialization.h"

struct NetworkAddress {
	NetworkAddress (uint32_t ip, uint16_t port) : ip(ip), port(port) {
	}

	NetworkAddress() : ip(0), port(0) {
	}

	uint32_t ip;
	uint16_t port;
	bool operator<(const NetworkAddress& rhs) const {
		return std::make_pair(ip, port) < std::make_pair(rhs.ip, rhs.port);
	}
};

inline uint32_t serializedSize(const NetworkAddress& server) {
	return serializedSize(server.ip, server.port);
}

inline void serialize(uint8_t** destination, const NetworkAddress& server) {
	return serialize(destination, server.ip, server.port);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		NetworkAddress& server) {
	deserialize(source, bytesLeftInBuffer, server.ip, server.port);
}

#endif // LIZARDFS_MFSCOMMON_NETWORK_ADDRESS_H_
