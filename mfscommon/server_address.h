#ifndef LIZARDFS_MFSCOMMON_SERVER_ADDRESS_H_
#define LIZARDFS_MFSCOMMON_SERVER_ADDRESS_H_

#include "mfscommon/serialization.h"

struct ServerAddress {
	ServerAddress (uint32_t ip, uint16_t port) : ip(ip), port(port) {
	}

	ServerAddress() : ip(0), port(0) {
	}

	uint32_t ip;
	uint16_t port;
};

inline uint32_t serializedSize(const ServerAddress& server) {
	return serializedSize(server.ip, server.port);
}

inline void serialize(uint8_t** destination, const ServerAddress& server) {
	return serialize(destination, server.ip, server.port);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ServerAddress& server) {
	deserialize(source, bytesLeftInBuffer, server.ip, server.port);
}

#endif // LIZARDFS_MFSCOMMON_SERVER_ADDRESS_H_
