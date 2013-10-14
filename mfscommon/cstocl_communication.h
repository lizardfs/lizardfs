#ifndef LIZARDFS_MFSCOMMON_CSTOCL_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_CSTOCL_COMMUNICATION_H_

#include "mfscommon/chunk_type.h"
#include "mfscommon/packet.h"

namespace cstocl {

namespace readData {

inline void serialize(std::vector<uint8_t>& buffer,
		uint64_t chunkId, uint32_t readOffset, uint32_t readSize,
		uint32_t crc, const std::vector<uint8_t>& data) {
	serializePacket(buffer, LIZ_CSTOCL_READ_DATA, 0, chunkId, readOffset, readSize, crc, data);
}

inline void serializePrefix(std::vector<uint8_t>& buffer,
		uint64_t chunkId, uint32_t readOffset, uint32_t readSize) {
	// This prefix requires CRC (uint32_t) and data (readSize * uint8_t) to be appended
	uint32_t extraSpace = serializedSize(uint32_t()) + readSize;
	serializePacketPrefix(buffer, extraSpace,
			LIZ_CSTOCL_READ_DATA, 0, chunkId, readOffset, readSize);
}

inline void deserialize(const std::vector<uint8_t>& buffer,
		uint64_t& chunkId, uint32_t& readOffset, uint32_t& readSize,
		uint32_t& crc, std::vector<uint8_t>& data) {
	deserializePacketDataNoHeader(buffer, chunkId, readOffset, readSize, crc, data);
}

inline void deserializePrefix(const std::vector<uint8_t>& buffer,
		uint64_t& chunkId, uint32_t& readOffset, uint32_t& readSize, uint32_t& crc) {
	deserializePacketDataNoHeader(buffer, chunkId, readOffset, readSize, crc);
}

// kPrefixSize - version, chcunkId, readOffset, readSize, crc
static const uint32_t kPrefixSize = 4 + 8 + 4 + 4 + 4;

} // namespace readData

namespace readStatus {

inline void serialize(std::vector<uint8_t>& buffer, uint64_t chunkId, uint8_t status) {
	serializePacket(buffer, LIZ_CSTOCL_READ_STATUS, 0, chunkId, status);
}

inline void deserialize(const std::vector<uint8_t>& buffer, uint64_t& chunkId, uint8_t& status) {
	deserializePacketDataNoHeader(buffer, chunkId, status);
}

} // namespace readStatus

} // namespace cstocl

#endif // LIZARDFS_MFSCOMMON_CSTOCL_COMMUNICATION_H_
