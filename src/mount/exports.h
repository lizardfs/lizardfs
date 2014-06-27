#pragma once

#include "common/platform.h"

/**
 * Function returning reference to a variable determining if non-root users are allowed to
 * use filesystem mounted in the meta mode
 */
inline bool& nonRootAllowedToUseMeta() {
	static bool ret;
	return ret;
}
