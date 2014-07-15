#pragma once

#include "common/platform.h"

#include "common/packet.h"
#include "common/serialization_macros.h"

// LIZ_MATOML_REGISTER_SHADOW

LIZARDFS_DEFINE_PACKET_VERSION(matoml, registerShadow, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matoml, registerShadow, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matoml, registerShadow, LIZ_MATOML_REGISTER_SHADOW, kStatusPacketVersion,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matoml, registerShadow, LIZ_MATOML_REGISTER_SHADOW, kResponsePacketVersion,
		uint32_t, version,
		uint64_t, metadataVersion)
