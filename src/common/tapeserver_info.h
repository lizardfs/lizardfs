#pragma once

#include "common/platform.h"

#include <string>

#include "common/media_label.h"
#include "common/network_address.h"
#include "common/serialization_macros.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(TapeserverInfo,
		std::string, server,
		MediaLabel, label,
		NetworkAddress, address);
