#pragma once

#include "common/packet.h"

namespace cltoma {

namespace iolimit {

inline void serialize(std::vector<uint8_t>& destination, const std::string& group, bool wantMore,
		uint64_t currentLimit_Bps, uint64_t recentUsage_Bps) {
	serializePacket(destination, LIZ_CLTOMA_IOLIMIT, 0, group, wantMore,
			currentLimit_Bps, recentUsage_Bps);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, std::string& group,
		bool& wantMore, uint64_t& currentLimit_Bps, uint64_t& recentUsage_Bps)
{
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, group, wantMore,
			currentLimit_Bps, recentUsage_Bps);
}

} // namespace iolimit

} // namespace cltoma
