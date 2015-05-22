#pragma once

#include "common/platform.h"

#include "common/packet.h"
#include "common/serialization_macros.h"

// LIZ_MATOTS_REGISTER_TAPESERVER
LIZARDFS_DEFINE_PACKET_VERSION(matots, registerTapeserver, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matots, registerTapeserver, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matots, registerTapeserver, LIZ_MATOTS_REGISTER_TAPESERVER, kStatusPacketVersion,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matots, registerTapeserver, LIZ_MATOTS_REGISTER_TAPESERVER, kResponsePacketVersion,
		uint32_t, version)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matots, putFiles, LIZ_MATOTS_PUT_FILES, 0,
		std::vector<TapeKey>, tapeContents)
