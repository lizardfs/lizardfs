#ifndef LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_

#include "mfscommon/chunk_type.h"
#include "mfscommon/chunk_with_version_and_type.h"
#include "mfscommon/packet.h"

namespace cstoma {

namespace chunkNew {

inline void serialize(std::vector<uint8_t>& destination,
		const std::vector<ChunkWithVersionAndType>& chunks) {
	serializePacket(destination, LIZ_CSTOMA_CHUNK_NEW, 0, chunks);
}

inline void deserialize(const std::vector<uint8_t>& source,
		std::vector<ChunkWithVersionAndType>& chunks) {
	verifyPacketVersionNoHeader(source, 0);
	deserializePacketDataNoHeader(source, chunks);
}

} // namespace chunkNew

namespace registerHost {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t ip, uint16_t port, uint16_t timeout, uint32_t csVersion) {
	serializePacket(destination, LIZ_CSTOMA_REGISTER_HOST, 0, ip, port, timeout, csVersion);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint32_t& ip, uint16_t& port, uint16_t& timeout, uint32_t& csVersion) {
	deserializePacketDataNoHeader(source, ip, port, timeout, csVersion);
}

} // namespace registerHost

namespace registerChunks {

inline void serialize(std::vector<uint8_t>& destination,
		const std::vector<ChunkWithVersionAndType>& chunks) {
	serializePacket(destination, LIZ_CSTOMA_REGISTER_CHUNKS, 0, chunks);
}

inline void deserialize(const std::vector<uint8_t>& source,
		std::vector<ChunkWithVersionAndType>& chunks) {
	deserializePacketDataNoHeader(source, chunks);
}

} // namespace registerChunks

namespace registerSpace {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t usedSpace, uint64_t totalSpace, uint32_t chunkCount, uint64_t tdUsedSpace,
		uint64_t toDeleteTotalSpace, uint32_t toDeleteChunksNumber) {
	serializePacket(destination, LIZ_CSTOMA_REGISTER_SPACE, 0, usedSpace, totalSpace,
			chunkCount, tdUsedSpace, toDeleteTotalSpace, toDeleteChunksNumber);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& usedSpace, uint64_t& totalSpace, uint32_t& chunkCount, uint64_t& tdUsedSpace,
		uint64_t& toDeleteTotalSpace, uint32_t& toDeleteChunksNumber) {
	deserializePacketDataNoHeader(source, usedSpace, totalSpace,
			chunkCount, tdUsedSpace, toDeleteTotalSpace, toDeleteChunksNumber);
}

} // namespace registerSpace

} // namespace cstoma

#endif /* LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_ */
