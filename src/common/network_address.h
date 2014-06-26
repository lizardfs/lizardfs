#pragma once

#include "common/platform.h"

#include <arpa/inet.h>
#include <functional>
#include <sstream>

#include "common/serialization.h"

struct NetworkAddress {
	uint32_t ip;
	uint16_t port;

	NetworkAddress(uint32_t ip, uint16_t port) : ip(ip), port(port) {
	}

	NetworkAddress() : ip(0), port(0) {
	}

	bool operator<(const NetworkAddress& rhs) const {
		return std::make_pair(ip, port) < std::make_pair(rhs.ip, rhs.port);
	}

	bool operator==(const NetworkAddress& rhs) const {
		return std::make_pair(ip, port) == std::make_pair(rhs.ip, rhs.port);
	}

	std::string toString() const {
		std::stringstream ss;
		for (int i = 24; i >= 0; i -= 8) {
			ss << ((ip >> i) & 0xff) << (i > 0 ? '.' : ':');
		}
		ss << port;
		return ss.str();
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

namespace std {
template <>
struct hash<NetworkAddress> {
	size_t operator()(const NetworkAddress& address) const {
		// MooseFS CSDB hash function
		return address.ip * 0x7b348943 + address.port;
	}
};
}
