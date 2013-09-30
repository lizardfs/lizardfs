#ifndef LIZARDFS_MFSCOMMON_PACKET_H_
#define LIZARDFS_MFSCOMMON_PACKET_H_

#include <inttypes.h>
#include <memory>

#include "mfscommon/serialization.h"
#include "mfscommon/MFSCommunication.h"

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
};

/*
 * Type of a variable which stores version of a packet
 */
typedef uint32_t PacketVersion;

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
	uint32_t length = serializedSize(version, data...);
	serialize(destination, PacketHeader(type, length), version, data...);
};

/*
 * Assembles initial segment of a packet, sets bigger length in the header to accommodate
 * data appended later.
 */
template <class... Data>
inline void serializePacketPrefix(std::vector<uint8_t>& destination, uint32_t extraLength,
		PacketHeader::Type type, PacketVersion version, const Data&... data) {
	uint32_t length = serializedSize(version, data...) + extraLength;
	serialize(destination, PacketHeader(type, length), version, data...);
};

/*
 * Assembles a whole MooseFS packet (packet without version)
 */
template<class T, class... Data>
inline void serializeMooseFsPacket(std::vector<uint8_t>& buffer,
		const PacketHeader::Type& type,
		const T& t,
		const Data &...args) {
	uint32_t length = serializedSize(t, args...);
	serialize(buffer, type, length, t, args...);
}

inline void serializeMooseFsPacket(std::vector<uint8_t>& buffer,
		const PacketHeader::Type& type) {
	uint32_t length = 0;
	serialize(buffer, type, length);
}

/*
 * Assembles initial segment of a MooseFS packet (without version),
 * sets bigger length in the header to accommodate data appended later.
 */
template<class... Args>
inline void serializeMooseFsPacketPrefix(std::vector<uint8_t>& buffer, uint32_t extraLength,
		const PacketHeader::Type& type, const Args &...args) {
	uint32_t length = serializedSize(args...) + extraLength;
	serialize(buffer, type, length, args...);
}

// Partial deserialization for new, versioned packets
//
// Doesn't modify source and bytesInBuffer
//
// Some procedures have two versions:
//   NoHeader   - version for headerless packet fragments
//   SkipHeader - version for packets with full MooseFS header

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
	deserialize(&source, bytesInBuffer, version);
}

inline void deserializePacketVersionSkipHeader(const std::vector<uint8_t>& source,
		PacketVersion& version) {
	deserializePacketVersionSkipHeader(source.data(), source.size(), version);
}

template <class... Data>
void deserializePacketDataNoHeader(const uint8_t* source, uint32_t bytesInBuffer, Data&... data) {
	deserializeAndIgnore<PacketVersion>(&source, bytesInBuffer);
	deserialize(&source, bytesInBuffer, data...);
}

template <class... Data>
void deserializePacketDataNoHeader(const std::vector<uint8_t>& source, Data&... data) {
	deserializePacketDataNoHeader(source.data(), source.size(), data...);
}

template <class... Data>
void deserializePacketDataSkipHeader(const uint8_t* source, uint32_t bytesInBuffer, Data&... data) {
	deserializeAndIgnore<PacketHeader>(&source, bytesInBuffer);
	deserializeAndIgnore<PacketVersion>(&source, bytesInBuffer);
	deserialize(&source, bytesInBuffer, data...);
}

template <class... Data>
void deserializePacketDataSkipHeader(const std::vector<uint8_t>& source, Data&... data) {
	deserializePacketDataSkipHeader(source.data(), source.size(), data...);
}

template<class... Data>
inline void deserializeMooseFsPacketDataNoHeader(const uint8_t* source, uint32_t bytesInBuffer,
		Data &...args) {
	deserialize(source, bytesInBuffer, args...);
}

template<class... Data>
inline void deserializeMooseFsPacketDataNoHeader(const std::vector<uint8_t>& source,
		Data &...args) {
	deserializeMooseFsPacketDataNoHeader(source.data(), source.size(), args...);
}

// check whether a LizardFS packet has expected version
inline void verifyPacketVersionNoHeader(const std::vector<uint8_t>& packetWithoutHeader,
		PacketVersion expectedVersion) throw (IncorrectDeserializationException) {
	PacketVersion actualVersion;
	deserializePacketVersionNoHeader(packetWithoutHeader, actualVersion);
	if (actualVersion != expectedVersion) {
		throw IncorrectDeserializationException();
	}
}

#endif // LIZARDFS_MFSCOMMON_PACKET_H_
