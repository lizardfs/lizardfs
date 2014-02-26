#pragma once

#include "common/packet.h"

namespace matocl {

namespace iolimits_config {

inline void serialize(std::vector<uint8_t>& destination, const std::string& subsystem,
		const std::vector<std::string>& groups, uint32_t renewFrequency_us) {
	serializePacket(destination, LIZ_MATOCL_IOLIMITS_CONFIG, 0, subsystem, groups,
			renewFrequency_us);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, std::string& subsystem,
		std::vector<std::string>& groups, uint32_t& renewFrequency_us) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, subsystem, groups, renewFrequency_us);
}

} // namespace iolimits_config

namespace iolimit {

inline void serialize(std::vector<uint8_t>& destination, const std::string& group,
		uint64_t limit_Bps) {
	serializePacket(destination, LIZ_MATOCL_IOLIMIT, 0, group, limit_Bps);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, std::string& group,
		uint64_t& limit_Bps) {
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, group, limit_Bps);
}

} // namespace iolimit

} // namespace matocl
