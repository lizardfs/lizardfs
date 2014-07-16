#pragma once

#include "common/platform.h"

#include "common/serialization_macros.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(mltoma, registerShadow, LIZ_MLTOMA_REGISTER_SHADOW, 0,
		uint32_t, version,
		uint32_t, timeout_ms,
		uint64_t, metadataVersion)
