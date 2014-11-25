#pragma once

#include "common/platform.h"

#include "common/serialization_macros.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(SerializedGoal,
		uint16_t, id,
		std::string, name,
		std::string, definition);

