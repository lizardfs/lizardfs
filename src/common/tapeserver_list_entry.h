#pragma once

#include "common/platform.h"

#include <string>

#include "common/media_label.h"
#include "common/network_address.h"
#include "common/serialization_macros.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(TapeserverListEntry,
		uint32_t, version,
		std::string, server,
		MediaLabel, label,
		NetworkAddress, address);
