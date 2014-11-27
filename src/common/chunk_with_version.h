#pragma once

#include "common/platform.h"

#include <cstdint>

#include "common/serialization_macros.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(ChunkWithVersion,
	uint64_t, id,
	uint32_t, version);
