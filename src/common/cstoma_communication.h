#pragma once

#include "common/platform.h"

#include "common/chunk_with_version.h"
#include "common/packet.h"
#include "common/serialization_macros.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerHost, LIZ_CSTOMA_REGISTER_HOST, 0,
		uint32_t, ip,
		uint16_t, port,
		uint32_t, timeout,
		uint32_t, csVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerChunks, LIZ_CSTOMA_REGISTER_CHUNKS, 1,
		std::vector<ChunkWithVersion>, chunks)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerSpace, LIZ_CSTOMA_REGISTER_SPACE, 0,
		uint64_t, usedSpace,
		uint64_t, totalSpace,
		uint32_t, chunkCount,
		uint64_t, tdUsedSpace,
		uint64_t, toDeleteTotalSpace,
		uint32_t, toDeleteChunksNumber)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		cstoma, registerLabel, LIZ_CSTOMA_REGISTER_LABEL, 0,
		std::string, label)
