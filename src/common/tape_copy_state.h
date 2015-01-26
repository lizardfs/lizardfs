#pragma once

#include "common/platform.h"

#include "common/serialization_macros.h"

/// State of the tape copy
LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(TapeCopyState, kInvalid, kCreating, kOk)
