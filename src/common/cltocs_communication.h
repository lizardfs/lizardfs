#pragma once

#include "common/chunk_type.h"
#include "common/network_address.h"
#include "common/packet.h"

namespace cltocs {

namespace read {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
		uint32_t readOffset, uint32_t readSize) {
	serializePacket(destination, LIZ_CLTOCS_READ, 0,
			chunkId, chunkVersion, chunkType, readOffset, readSize);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType,
		uint32_t& readOffset, uint32_t& readSize) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize,
			chunkId, chunkVersion, chunkType, readOffset, readSize);
}

} // namespace read

namespace writeInit {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
		const std::vector<NetworkAddress>& chain) {
	serializePacket(destination, LIZ_CLTOCS_WRITE_INIT, 0,
			chunkId, chunkVersion, chunkType, chain);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& chunkVersion, ChunkType& chunkType,
		std::vector<NetworkAddress>& chain) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize,
			chunkId, chunkVersion, chunkType, chain);
}

} // namespace writeInit

namespace writeData {

inline void serializePrefix(std::vector<uint8_t>& destination,
		uint64_t chunkId, uint32_t writeId,
		uint16_t blockNumber, uint32_t offset, uint32_t size, uint32_t crc) {
	serializePacketPrefix(destination, size, LIZ_CLTOCS_WRITE_DATA, 0,
			chunkId, writeId, blockNumber, offset, size, crc);
}

inline void deserializePrefix(const uint8_t* source, uint32_t sourceSize,
		uint64_t& chunkId, uint32_t& writeId,
		uint16_t& blockNumber, uint32_t& offset, uint32_t& size, uint32_t& crc) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializePacketDataNoHeader(source, sourceSize,
			chunkId, writeId, blockNumber, offset, size, crc);
}

// kPrefixSize is equal to:
// version:u32 chunkId:u64 writeId:32 block:16 offset:32 size:32 crc:32
static const uint32_t kPrefixSize = 4 + 8 + 4 + 2 + 4 + 4 + 4;

} // namespace writeData

namespace writeEnd {

inline void serialize(std::vector<uint8_t>& destination, uint64_t chunkId) {
	serializePacket(destination, LIZ_CLTOCS_WRITE_END, 0, chunkId);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint64_t& chunkId) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, chunkId);
}

} // namespace writeEnd

} // namespace cltocs
