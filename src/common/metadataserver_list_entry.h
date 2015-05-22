#pragma once

#include "common/platform.h"

#include <string>

#include "common/serialization_macros.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(MetadataserverListEntry,
		uint32_t, ip,
		uint16_t, port,
		uint32_t, version);
