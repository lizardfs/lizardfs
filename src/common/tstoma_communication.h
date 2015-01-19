#pragma once

#include "common/platform.h"

#include "common/packet.h"
#include "common/serialization_macros.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		tstoma, registerTapeserver, LIZ_TSTOMA_REGISTER_TAPESERVER, 0,
		uint32_t, version)
