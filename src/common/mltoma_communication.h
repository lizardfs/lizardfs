#pragma once

#include "common/platform.h"

#include <map>
#include <string>

#include "common/serialization_macros.h"

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		mltoma, registerShadow, LIZ_MLTOMA_REGISTER_SHADOW, 0,
		uint32_t, version,
		uint32_t, timeout_ms,
		uint64_t, metadataVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		mltoma, changelogApplyError, LIZ_MLTOMA_CHANGELOG_APPLY_ERROR, 0,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		mltoma, matoclport, LIZ_MLTOMA_CLTOMA_PORT, 0,
		uint16_t, port)
