/*
   Copyright 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <functional>
#include <sstream>

#include "common/human_readable_format.h"
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
		ss << ipToString(ip);
		if (port) {
			ss << ':' << port;
		}
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

class ChunkserverConnectionException : public Exception {
public:
	ChunkserverConnectionException(const std::string& message, const NetworkAddress& server)
			: Exception(message + " (server " + server.toString() + ")"),
			  server_(server) {
	}

	~ChunkserverConnectionException() noexcept {
	}

	const NetworkAddress& server() const { return server_; }

private:
	NetworkAddress server_;
};
