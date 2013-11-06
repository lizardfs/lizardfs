#ifndef LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_

#include "common/chunk_type.h"
#include "common/chunk_with_version_and_type.h"
#include "common/packet.h"

namespace cstoma {

namespace chunkNew {

inline void serialize(std::vector<uint8_t>& destination,
		const std::vector<ChunkWithVersionAndType>& chunks) {
	serializePacket(destination, LIZ_CSTOMA_CHUNK_NEW, 0, chunks);
}

inline void deserialize(const std::vector<uint8_t>& source,
		std::vector<ChunkWithVersionAndType>& chunks) {
	verifyPacketVersionNoHeader(source, 0);
	deserializeAllPacketDataNoHeader(source, chunks);
}

} // namespace chunkNew

namespace registerHost {

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t ip, uint16_t port, uint16_t timeout, uint32_t csVersion) {
	serializePacket(destination, LIZ_CSTOMA_REGISTER_HOST, 0, ip, port, timeout, csVersion);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint32_t& ip, uint16_t& port, uint16_t& timeout, uint32_t& csVersion) {
	deserializeAllPacketDataNoHeader(source, ip, port, timeout, csVersion);
}

} // namespace registerHost

namespace registerChunks {

inline void serialize(std::vector<uint8_t>& destination,
		const std::vector<ChunkWithVersionAndType>& chunks) {
	serializePacket(destination, LIZ_CSTOMA_REGISTER_CHUNKS, 0, chunks);
}

inline void deserialize(const std::vector<uint8_t>& source,
		std::vector<ChunkWithVersionAndType>& chunks) {
	deserializeAllPacketDataNoHeader(source, chunks);
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
	deserializeAllPacketDataNoHeader(source, usedSpace, totalSpace,
			chunkCount, tdUsedSpace, toDeleteTotalSpace, toDeleteChunksNumber);
}

} // namespace registerSpace

namespace setVersion {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, ChunkType chunkType, uint8_t status) {
	serializePacket(destination, LIZ_CSTOMA_SET_VERSION, 0, chunkId, chunkType, status);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, ChunkType& chunkType, uint8_t& status) {
	deserializeAllPacketDataNoHeader(source, chunkId, chunkType, status);
}

} // namespace setVersion

namespace deleteChunk {

inline void serialize(std::vector<uint8_t>& destination,
		uint64_t chunkId, ChunkType chunkType, uint8_t status) {
	serializePacket(destination, LIZ_CSTOMA_DELETE_CHUNK, 0, chunkId, chunkType, status);
}

inline void deserialize(const std::vector<uint8_t>& source,
		uint64_t& chunkId, ChunkType& chunkType, uint8_t& status) {
	deserializeAllPacketDataNoHeader(source, chunkId, chunkType, status);
}

} // namespace deleteChunk

} // namespace cstoma

#endif /* LIZARDFS_MFSCOMMON_CSTOMA_COMMUNICATION_H_ */
