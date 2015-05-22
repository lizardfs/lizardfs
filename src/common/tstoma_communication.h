#pragma once

#include "common/platform.h"

#include "common/packet.h"
#include "common/serialization_macros.h"
#include "common/tape_key.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		tstoma, registerTapeserver, LIZ_TSTOMA_REGISTER_TAPESERVER, 0,
		uint32_t, version)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		tstoma, hasFiles, LIZ_TSTOMA_HAS_FILES, 0,
		std::vector<TapeKey>, tapeContents)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		tstoma, endOfFiles, LIZ_TSTOMA_END_OF_FILES, 0)
