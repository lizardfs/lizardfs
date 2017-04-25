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

#include <inttypes.h>
#include <memory>
#include <string>

#include "protocol/MFSCommunication.h"
#include "common/serialization.h"

// Legacy MooseFS packet format:
//
// type
// length of data
// data (type-dependent)
//
// New packet header contains an additional version field.
// For backwards compatibility, length indicates total size
// of remaining part of the packet, including version.
//
// type
// length of version field and data
// version
// data

/*
 * Header of all LizarsdFS network packets
 */
struct PacketHeader {
public:
	typedef uint32_t Type;
	typedef uint32_t Length;

	static const uint32_t kSize = 8;
	static const Type kMinOldPacketType = 0;
	static const Type kMaxOldPacketType = 1000;
	static const Type kMinLizPacketType = 1001;
	static const Type kMaxLizPacketType = 2000;

	Type type;
	Length length;

	PacketHeader(Type type_, Length length_) : type(type_), length(length_) {
	}

	PacketHeader() : type(0), length(0) {
	}

	bool isLizPacketType() const {
		return kMinLizPacketType <= type && type <= kMaxLizPacketType;
	}

	bool isOldPacketType() const {
		/*
		 *  We do not check if type >= kMinOldPacketType to avoid gcc's warning:
		 *     comparison of unsigned expression >= 0 is always true
		 */
		return type <= kMaxOldPacketType;
	}
};

/*
 * Type of a variable which stores version of a packet
 */
typedef uint32_t PacketVersion;

/*
 * Type of a variable which stores a packet's data
 */
typedef std::vector<uint8_t> MessageBuffer;

/*
 * Extension of serialization framework to handle PacketHeader
 */
inline uint32_t serializedSize(const PacketHeader& ph) {
	return serializedSize(ph.type, ph.length);
}

inline void serialize(uint8_t** destination, const PacketHeader& ph) {
	return serialize(destination, ph.type, ph.length);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, PacketHeader& ph) {
	return deserialize(source, bytesLeftInBuffer, ph.type, ph.length);
}

/*
 * Assembles a whole packet
 */
template <class... Data>
inline void serializePacket(std::vector<uint8_t>& destination,
		PacketHeader::Type type, PacketVersion version, const Data&... data) {
	sassert(type >= PacketHeader::kMinLizPacketType && type <= PacketHeader::kMaxLizPacketType);
	uint32_t length = serializedSize(version, data...);
	serialize(destination, PacketHeader(type, length), version, data...);
}

template <class... Data>
inline std::vector<uint8_t> buildPacket(PacketHeader::Type type, PacketVersion version, const Data&... data) {
	sassert(type >= PacketHeader::kMinLizPacketType && type <= PacketHeader::kMaxLizPacketType);
	uint32_t length = serializedSize(version, data...);
	std::vector<uint8_t> buffer;
	serialize(buffer, PacketHeader(type, length), version, data...);
	return buffer;
}

/*
 * Assembles initial segment of a packet, sets bigger length in the header to accommodate
 * data appended later.
 */
template <class... Data>
inline void serializePacketPrefix(std::vector<uint8_t>& destination, uint32_t extraLength,
		PacketHeader::Type type, PacketVersion version, const Data&... data) {
	sassert(type >= PacketHeader::kMinLizPacketType && type <= PacketHeader::kMaxLizPacketType);
	uint32_t length = serializedSize(version, data...) + extraLength;
	serialize(destination, PacketHeader(type, length), version, data...);
}

/*
 * Assembles a whole MooseFS packet (packet without version)
 */
template<class T, class... Data>
inline void serializeMooseFsPacket(std::vector<uint8_t>& buffer,
		const PacketHeader::Type& type,
		const T& t,
		const Data &...args) {
	sassert(type <= PacketHeader::kMaxOldPacketType);
	uint32_t length = serializedSize(t, args...);
	serialize(buffer, type, length, t, args...);
}

inline void serializeMooseFsPacket(std::vector<uint8_t>& buffer,
		const PacketHeader::Type& type) {
	sassert(type <= PacketHeader::kMaxOldPacketType);
	uint32_t length = 0;
	serialize(buffer, type, length);
}

template<class... Data>
inline std::vector<uint8_t> buildMooseFsPacket(const Data&... args) {
	std::vector<uint8_t> ret;
	serializeMooseFsPacket(ret, args...);
	return ret;
}

/*
 * Assembles initial segment of a MooseFS packet (without version),
 * sets bigger length in the header to accommodate data appended later.
 */
template<class... Args>
inline void serializeMooseFsPacketPrefix(std::vector<uint8_t>& buffer, uint32_t extraLength,
		const PacketHeader::Type& type, const Args &...args) {
	sassert(type <= PacketHeader::kMaxOldPacketType);
	uint32_t length = serializedSize(args...) + extraLength;
	serialize(buffer, type, length, args...);
}

/*
 * Partial deserialization for new, versioned packets
 *
 * Doesn't modify source and bytesInBuffer
 *
 * Some procedures have two versions:
 *   NoHeader   - version for headerless packet fragments
 *   SkipHeader - version for packets with full MooseFS header
 *
 * If the function name contains All infix, it means that the function will throw
 * IncorrectDeserializationException when the buffer is too long
 */

inline void deserializePacketHeader(const uint8_t* source, uint32_t bytesInBuffer,
		PacketHeader& header) {
	deserialize(source, bytesInBuffer, header);
}

inline void deserializePacketHeader(const std::vector<uint8_t>& source, PacketHeader& header) {
	deserializePacketHeader(source.data(), source.size(), header);
}

inline void deserializePacketVersionNoHeader(const uint8_t* source, uint32_t bytesInBuffer,
		PacketVersion& version) {
	deserialize(source, bytesInBuffer, version);
}

inline void deserializePacketVersionNoHeader(const std::vector<uint8_t>& source,
		PacketVersion& version) {
	deserializePacketVersionNoHeader(source.data(), source.size(), version);
}

inline void deserializePacketVersionSkipHeader(const uint8_t* source, uint32_t bytesInBuffer,
		PacketVersion& version) {
	deserializeAndIgnore<PacketHeader>(&source, bytesInBuffer);
	deserialize(source, bytesInBuffer, version);
}

inline void deserializePacketVersionSkipHeader(const std::vector<uint8_t>& source,
		PacketVersion& version) {
	deserializePacketVersionSkipHeader(source.data(), source.size(), version);
}

template <class... Data>
inline void deserializePacketDataNoHeader(const uint8_t* source, uint32_t bytesInBuffer,
		Data&... data) {
	deserializeAndIgnore<PacketVersion>(&source, bytesInBuffer);
	deserialize(source, bytesInBuffer, data...);
}

template <class... Data>
inline void deserializePacketDataNoHeader(const std::vector<uint8_t>& source, Data&... data) {
	deserializePacketDataNoHeader(source.data(), source.size(), data...);
}

template <class... Data>
inline void deserializeAllPacketDataNoHeader(const uint8_t* source, uint32_t bytesInBuffer,
		Data&... data) {
	deserializeAndIgnore<PacketVersion>(&source, bytesInBuffer);
	uint32_t bytesNotUsed = deserialize(source, bytesInBuffer, data...);
	if (bytesNotUsed > 0) {
		throw IncorrectDeserializationException("buffer longer than expected");
	}
}

template <class... Data>
inline void deserializeAllPacketDataNoHeader(const std::vector<uint8_t>& source, Data&... data) {
	deserializeAllPacketDataNoHeader(source.data(), source.size(), data...);
}

template <class... Data>
inline void deserializePacketDataSkipHeader(const uint8_t* source, uint32_t bytesInBuffer,
		Data&... data) {
	deserializeAndIgnore<PacketHeader>(&source, bytesInBuffer);
	deserializeAndIgnore<PacketVersion>(&source, bytesInBuffer);
	deserialize(source, bytesInBuffer, data...);
}

template <class... Data>
inline void deserializePacketDataSkipHeader(const std::vector<uint8_t>& source, Data&... data) {
	deserializePacketDataSkipHeader(source.data(), source.size(), data...);
}

template<class... Data>
inline void deserializeAllMooseFsPacketDataNoHeader(const uint8_t* source, uint32_t bytesInBuffer,
		Data &...args) {
	uint32_t bytesNotUsed = deserialize(source, bytesInBuffer, args...);
	if (bytesNotUsed > 0) {
		throw IncorrectDeserializationException("buffer longer than expected");
	}
}

template<class... Data>
inline void deserializeAllMooseFsPacketDataNoHeader(const std::vector<uint8_t>& source,
		Data &...args) {
	deserializeAllMooseFsPacketDataNoHeader(source.data(), source.size(), args...);
}

template<class... Data>
inline void deserializeMooseFsPacketPrefixNoHeader(const uint8_t* source, uint32_t bytesInBuffer,
		Data &...args) {
	deserialize(source, bytesInBuffer, args...);
}

template<class... Data>
inline void deserializeMooseFsPacketPrefixNoHeader(const std::vector<uint8_t>& source,
		Data &...args) {
	deserializeMooseFsPacketPrefixNoHeader(source.data(), source.size(), args...);
}

// check whether a LizardFS packet has expected version
inline void verifyPacketVersionNoHeader(const uint8_t* source, uint32_t bytesInBuffer,
		PacketVersion expectedVersion) {
	PacketVersion actualVersion;
	deserializePacketVersionNoHeader(source, bytesInBuffer, actualVersion);
	if (actualVersion != expectedVersion) {
		throw IncorrectDeserializationException(
				"expected packet version " + std::to_string(expectedVersion) +
				", got " + std::to_string(actualVersion));
	}
}

inline void verifyPacketVersionNoHeader(const std::vector<uint8_t>& source,
		PacketVersion expectedVersion) {
	verifyPacketVersionNoHeader(source.data(), source.size(), expectedVersion);
}

void receivePacket(PacketHeader& header, std::vector<uint8_t>& data, int sock,
		uint32_t timeout_ms);
