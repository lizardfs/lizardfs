#pragma once

#include "common/platform.h"

#include <algorithm>

#include "common/compact_vector.h"
#include "common/serialization_macros.h"

/// Information about a copy of a file on a tapeserver
struct TapeCopy {

	TapeCopy() : state(TapeCopyState::kInvalid), server(0) {}

	TapeCopy(TapeCopyState state, TapeserverId server) : state(state), server(server) {}

	TapeCopyState state;

	TapeserverId server;
};

/// Information about all copies of a file on tapeservers.
typedef compact_vector<TapeCopy> TapeCopies;
