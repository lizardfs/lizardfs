#pragma once

#include "common/platform.h"

#include "common/serialization_macros.h"
#include "common/media_label.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(ChunkserverListEntry,
		uint32_t, version,
		uint32_t, servip,
		uint16_t, servport,
		uint64_t, usedspace,
		uint64_t, totalspace,
		uint32_t, chunkscount,
		uint64_t, todelusedspace,
		uint64_t, todeltotalspace,
		uint32_t, todelchunkscount,
		uint32_t, errorcounter,
		MediaLabel, label);
