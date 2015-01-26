#pragma once

#include "common/platform.h"

#include "common/serialization_macros.h"
#include "common/tape_copy_state.h"
#include "common/tapeserver_info.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(TapeCopyLocationInfo,
		TapeserverInfo, tapeserver,
		TapeCopyState, state);
