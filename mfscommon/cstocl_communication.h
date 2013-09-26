#ifndef _LIZARDFS_MFSCOMMON_CSTOCL_COMMUNICATION_H_
#define _LIZARDFS_MFSCOMMON_CSTOCL_COMMUNICATION_H_

#include "mfscommon/chunk_type.h"
#include "mfscommon/packet.h"

namespace cstocl {

namespace readData {

void serialize(std::vector<uint8_t>& buffer, uint64_t chunkId, uint32_t readOffset,
		uint32_t readSize, uint32_t crc, const std::vector<uint8_t>& data) {
	serializePacket(buffer, LIZ_CSTOCL_READ_DATA, 0, chunkId, readOffset, readSize, crc, data);
}

void deserialize(const std::vector<uint8_t>& buffer, uint64_t& chunkId, uint32_t& readOffset,
		uint32_t& readSize, uint32_t& crc, std::vector<uint8_t>& data) {
	deserializePacketDataNoHeader(buffer, chunkId, readOffset, readSize, crc, data);
}

}

namespace readStatus {

void serialize(std::vector<uint8_t>& buffer, uint64_t chunkId, uint8_t status) {
	serializePacket(buffer, LIZ_CSTOCL_READ_STATUS, 0, chunkId, status);
}

void deserialize(const std::vector<uint8_t>& buffer, uint64_t& chunkId, uint8_t& status) {
	deserializePacketDataNoHeader(buffer, chunkId, status);
}

}
}

#endif /*_LIZARDFS_MFSCOMMON_CSTOCL_COMMUNICATION_H_*/
