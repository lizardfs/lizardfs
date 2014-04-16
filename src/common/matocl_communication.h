#pragma once

#include "common/access_control_list.h"
#include "common/packet.h"

namespace matocl {

namespace fuseDeleteAcl {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_DELETE_ACL, 0, messageId, status);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, status);
}

} // fuseDeleteAcl

namespace fuseGetAcl {

const PacketVersion kStatusPacketVersion = 0;
const PacketVersion kResponsePacketVersion = 1;

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_GET_ACL, kStatusPacketVersion, messageId, status);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, kStatusPacketVersion);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, status);
}

inline void serialize(std::vector<uint8_t>& destination,
		uint32_t messageId, const AccessControlList& acl) {
	serializePacket(destination, LIZ_MATOCL_FUSE_GET_ACL, kResponsePacketVersion, messageId, acl);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, AccessControlList& acl) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, kResponsePacketVersion);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, acl);
}


} // fuseGetAcl

namespace fuseSetAcl {

inline void serialize(std::vector<uint8_t>& destination, uint32_t messageId, uint8_t status) {
	serializePacket(destination, LIZ_MATOCL_FUSE_SET_ACL, 0, messageId, status);
}

inline void deserialize(const uint8_t* source, uint32_t sourceSize, uint8_t& status) {
	uint32_t dummyMessageId;
	verifyPacketVersionNoHeader(source, sourceSize, 0);
	deserializeAllPacketDataNoHeader(source, sourceSize, dummyMessageId, status);
}

} // fuseSetAcl

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
